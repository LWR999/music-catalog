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

@inventory_app.command("status")
def inventory_status(
    config: Path = typer.Option(None, "--config", exists=True, readable=True, dir_okay=False),
    since_minutes: int = typer.Option(60, "--since-minutes", min=1, help="Show recent activity within this window"),
):
    """
    Show catalog health at a glance:
      - totals by state (dirty/tagged/error/missing)
      - recent activity (last_seen within --since-minutes)
      - album/disc counts and fingerprint coverage
      - last run event
    """
    cfg = load_config(str(config) if config else None)
    con = _open_db(cfg)
    migrate_db(con)
    cur = con.cursor()

    # track-level stats
    total = cur.execute("SELECT COUNT(*) FROM track").fetchone()[0]
    dirty = cur.execute(
        "SELECT COUNT(*) FROM track WHERE status IN ('DIRTY_META','NEW','DEEP_PENDING')"
    ).fetchone()[0]
    tagged = cur.execute("SELECT COUNT(*) FROM track WHERE status='TAGGED'").fetchone()[0]
    errors = cur.execute("SELECT COUNT(*) FROM track WHERE status='ERROR'").fetchone()[0]
    missing = cur.execute("SELECT COUNT(*) FROM track WHERE is_missing=1").fetchone()[0]

    recent = cur.execute(
        "SELECT COUNT(*) FROM track WHERE last_seen > datetime('now', ?)",
        (f"-{int(since_minutes)} minutes",),
    ).fetchone()[0]

    # album/disc stats
    albums = cur.execute("SELECT COUNT(*) FROM album").fetchone()[0]
    albums_fp = cur.execute(
        "SELECT COUNT(*) FROM album WHERE album_fingerprint IS NOT NULL"
    ).fetchone()[0]
    discs = cur.execute("SELECT COUNT(*) FROM disc").fetchone()[0]

    # last run event
    last = cur.execute(
        "SELECT id, started_at, command FROM run_event ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # pretty print
    rprint("[bold cyan]Inventory Status[/bold cyan]")
    rprint(f"[white]DB:[/white] {cfg.db_path}")
    if last:
        rprint(f"[white]Last run:[/white] #{last[0]}  {last[1]}  ({last[2]})")

    rprint("\n[bold]Tracks[/bold]")
    rprint(f"  total   : {total:,}")
    rprint(f"  dirty   : {dirty:,}")
    rprint(f"  tagged  : {tagged:,}")
    rprint(f"  errors  : {errors:,}")
    rprint(f"  missing : {missing:,}")
    rprint(f"  recent  : {recent:,} (last {since_minutes} min)")

    rprint("\n[bold]Albums/Discs[/bold]")
    rprint(f"  albums           : {albums:,}")
    rprint(f"  with fingerprint : {albums_fp:,}")
    rprint(f"  discs            : {discs:,}")

    # optional quick breakdowns
    by_status = cur.execute(
        "SELECT status, COUNT(*) FROM track GROUP BY status ORDER BY COUNT(*) DESC"
    ).fetchall()
    rprint("\n[bold]By status[/bold]")
    for s, n in by_status:
        rprint(f"  {s or 'NULL':<12} {n:,}")

    # codec/rate snapshot (top combos)
    by_fmt = cur.execute(
        """
        SELECT COALESCE(codec,'?') AS codec,
               COALESCE(bit_depth,0) AS bitd,
               COALESCE(sample_rate_hz,0) AS rate,
               COUNT(*) AS n
        FROM track
        GROUP BY 1,2,3
        ORDER BY n DESC
        LIMIT 8
        """
    ).fetchall()
    rprint("\n[bold]Top codec/bit-depth/sample-rate[/bold]")
    for codec, bitd, rate, n in by_fmt:
        rprint(f"  {codec:<5} {bitd:>2}-bit @ {rate:>6} Hz  → {n:,}")