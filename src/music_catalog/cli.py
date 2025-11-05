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
