from __future__ import annotations
import typer, time
from pathlib import Path
from rich import print as rprint
from music_catalog.config import load_config
from music_catalog.db import connect, migrate as migrate_db
from music_catalog.inventory import (
    iter_audio_files,
    quick_probe,
    upsert_track_fast,
    load_tags_and_artwork,
    digest_tags,
)

app = typer.Typer(help="Music catalog (read-only)")
inventory_app = typer.Typer(help="Inventory operations")
app.add_typer(inventory_app, name="inventory")
db_app = typer.Typer(help="Database operations")
app.add_typer(db_app, name="db")

def _open_db(cfg):
    return connect(cfg.db_path)


@app.command("migrate")
def migrate_cmd(
    config: Path = typer.Option(None, "--config", exists=True, readable=True, dir_okay=False),
):
    """Create/upgrade the SQLite schema."""
    cfg = load_config(str(config) if config else None)
    con = _open_db(cfg)
    migrate_db(con)
    rprint(f"[green]Schema ensured at[/green] {cfg.db_path}")

@inventory_app.command("fast")
def inventory_fast(
    config: Path = typer.Option(None, "--config", exists=True, readable=True, dir_okay=False),
    workers: int = typer.Option(6, "--workers", min=1, max=32, help="Number of header-probe workers"),
    batch_size: int = typer.Option(1000, "--batch-size", min=100, max=10000, help="DB commit batch size"),
):
    """Cheap pass: threaded header probe + single-writer batched upserts."""
    cfg = load_config(str(config) if config else None)
    con = _open_db(cfg)
    migrate_db(con)

    roots = [r["path"] for r in cfg.roots]
    ignore = cfg.scan.get("ignore_patterns", [])
    start = time.time()

    # import here to avoid circulars at import time
    from music_catalog.inventory import fast_index_threaded

    total = fast_index_threaded(
        con=con,
        roots=roots,
        ignore_dirs=ignore,
        workers=workers,
        batch_size=batch_size,
        progress_every=2000,
    )

    dur = time.time() - start
    rprint(f"[cyan]FAST inventory (threaded)[/cyan] processed {total} files in {dur:.1f}s "
           f"({total/max(dur,1):.1f} files/sec)")

@inventory_app.command("deep")
def inventory_deep(
    config: Path = typer.Option(None, "--config", exists=True, readable=True, dir_okay=False),
    limit: int = typer.Option(0, "--limit", help="Max dirty tracks to process (0 = no limit)"),
):
    """Deep pass: read full tags/artwork for DIRTY_META/NEW items and clear flag."""
    cfg = load_config(str(config) if config else None)
    con = _open_db(cfg)
    migrate_db(con)
    cur = con.cursor()
    q = """
        SELECT path FROM track
        WHERE status IN ('DIRTY_META','NEW','DEEP_PENDING')
        ORDER BY mtime_ns DESC
    """
    if limit and limit > 0:
        q += f" LIMIT {int(limit)}"
    rows = cur.execute(q).fetchall()
    processed = 0
    start = time.time()
    for (path,) in rows:
        try:
            tags, art = load_tags_and_artwork(path)
            td = digest_tags(tags)
            cur.execute(
                "UPDATE track SET tag_digest=?, status='TAGGED' WHERE path=?",
                (td, path),
            )
            processed += 1
            if processed % 200 == 0:
                con.commit()
        except Exception as e:
            cur.execute(
                "UPDATE track SET status='ERROR', last_error=? WHERE path=?",
                (str(e), path),
            )
    con.commit()
    dur = time.time() - start
    rprint(f"[cyan]DEEP inventory[/cyan] updated {processed} tracks in {dur:.1f}s")

@inventory_app.command("changed")
def inventory_changed(
    config: Path = typer.Option(None, "--config", exists=True, readable=True, dir_okay=False),
    debounce_sec: int = typer.Option(5, "--debounce-sec", min=0, help="Ignore files modified in the last N seconds"),
):
    """
    Change-only pass:
      - Skips untouched albums via in-DB fingerprint
      - Marks changed files DIRTY_META
      - Flags deleted/missing files
      - No sidecar files; SQLite only
    """
    cfg = load_config(str(config) if config else None)
    con = _open_db(cfg)
    migrate_db(con)

    roots = [r["path"] for r in cfg.roots]
    ignore = cfg.scan.get("ignore_patterns", [])
    start = time.time()

    from music_catalog.inventory import changed_index

    albums, touched = changed_index(
        con=con,
        roots=roots,
        ignore_dirs=ignore,
        debounce_sec=debounce_sec,
    )

    dur = time.time() - start
    rprint(f"[magenta]CHANGED inventory[/magenta] albums={albums}, touched_rows={touched}, "
           f"time={dur:.1f}s")

@db_app.command("clear")
def db_clear(
    config: Path = typer.Option(None, "--config", exists=True, readable=True, dir_okay=False),
    hard: bool = typer.Option(False, "--hard", help="Delete the DB file and recreate it (fresh schema)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt for confirmation"),
):
    """
    Clear the SQLite database.

    Soft (default): DELETEs all rows (track/album/disc/run_event), VACUUM, keeping the schema.
    Hard (--hard):  Removes the DB file and re-migrates a fresh schema.
    """
    cfg = load_config(str(config) if config else None)
    db_path = cfg.db_path

    if not yes:
        mode = "HARD (delete file & recreate)" if hard else "SOFT (delete rows & vacuum)"
        proceed = typer.confirm(f"About to clear database at {db_path} [{mode}]. Continue?")
        if not proceed:
            raise typer.Abort()

    if hard:
        # Close any open connection & remove file
        try:
            con = _open_db(cfg)
            con.close()
        except Exception:
            pass
        # Remove DB and sidecar files if present
        for suffix in ("", "-wal", "-shm"):
            try:
                p = Path(db_path + suffix)
                if p.exists():
                    p.unlink()
            except Exception as e:
                rprint(f"[red]Failed to remove {p}: {e}[/red]")
        # Recreate
        con = _open_db(cfg)
        migrate_db(con)
        rprint(f"[green]DB hard-reset complete[/green] → {db_path}")
        return

    # Soft clear
    con = _open_db(cfg)
    migrate_db(con)  # ensure schema is present
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=OFF;")
    # Order matters due to FKs
    for tbl in ("track", "disc", "album", "run_event"):
        cur.execute(f"DELETE FROM {tbl};")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("VACUUM;")
    con.commit()
    rprint(f"[green]DB soft clear complete[/green] (rows deleted, schema preserved) → {db_path}")