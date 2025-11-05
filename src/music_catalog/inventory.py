from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple, Dict, Any, List
from threading import Thread
from queue import Queue, Empty
import hashlib
import os
import re
import sqlite3
import time

import xxhash
from mutagen import File as MutagenFile
from mutagen.flac import FLAC

# Audio extensions we consider
AUDIO_EXTS = {".flac", ".dsf"}

# Accept common disc-folder spellings: "Disc 1", "CD1", "Disk 02", "D3 - Bonus"
DISC_FOLDER_RE = re.compile(
    r"^(?:disc|disk|cd|d)\s*[-_ ]*\s*(\d{1,2})(?:\s*[-_:]\s*(.+))?$",
    re.IGNORECASE,
)


@dataclass
class FastInfo:
    path: str
    size: int
    mtime_ns: int
    codec: str
    bit_depth: Optional[int]
    sample_rate: Optional[int]
    channels: Optional[int]
    duration: Optional[float]


def iter_audio_files(root: str, ignore_dirs: Optional[List[str]] = None) -> Iterable[Path]:
    """
    Yield audio file Paths under root.
    Skips dot-directories and known junk (@eaDir), plus any names in ignore_dirs (exact match).
    """
    ignore_dirs = set(ignore_dirs or [])
    for dirpath, dirnames, filenames in os.walk(root):
        base = os.path.basename(dirpath)
        if base.startswith(".") or base == "@eaDir" or base in ignore_dirs:
            dirnames[:] = []
            continue
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and d not in ignore_dirs]
        for fn in filenames:
            if fn.startswith("."):
                continue
            p = Path(dirpath) / fn
            if p.suffix.lower() in AUDIO_EXTS:
                yield p


def quick_probe(p: Path) -> FastInfo:
    """
    Cheap probe using mutagen header info (kept as-is for your desired detail).
    """
    st = p.stat()
    audio = MutagenFile(p.as_posix(), easy=False)
    codec = "FLAC" if p.suffix.lower() == ".flac" else "DSF"
    info = getattr(audio, "info", None)

    bit_depth = None
    if info is not None:
        bit_depth = getattr(info, "bits_per_sample", None) or getattr(info, "bits", None)
    sample_rate = getattr(info, "sample_rate", None) if info is not None else None
    channels = getattr(info, "channels", None) if info is not None else None
    duration = getattr(info, "length", None) if info is not None else None

    return FastInfo(
        path=str(p),
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
        codec=codec,
        bit_depth=int(bit_depth) if bit_depth else None,
        sample_rate=int(sample_rate) if sample_rate else None,
        channels=int(channels) if channels else None,
        duration=float(duration) if duration else None,
    )


def parse_album_folder(folder_path: str) -> Tuple[Optional[str], Optional[str]]:
    base = os.path.basename(folder_path.rstrip("/"))
    m = re.match(r"^(?P<artist>.+?)\s*-\s*(?P<title>.+)$", base)
    if m:
        return m.group("artist").strip(), m.group("title").strip()
    return None, None


def album_id_from_path(folder_path: str) -> int:
    # 63-bit positive space avoids signed overflow issues in SQLite
    return int.from_bytes(xxhash.xxh64(folder_path.encode("utf-8")).digest(), "big") & ((1 << 63) - 1)


def upsert_track_fast(con: sqlite3.Connection, fi: FastInfo):
    parent = Path(fi.path).parent
    parent_name = parent.name

    disc_number: Optional[int] = None
    disc_title: Optional[str] = None
    m = DISC_FOLDER_RE.match(parent_name)
    if m:
        disc_number = int(m.group(1))
        disc_title = (m.group(2) or "").strip() or None
        album_folder = parent.parent
    else:
        album_folder = parent

    folder_artist, folder_title = parse_album_folder(str(album_folder))
    album_id = album_id_from_path(str(album_folder))

    con.execute(
        """
        INSERT INTO album (id, folder_path, folder_artist, folder_title, status, updated_at)
        VALUES (?, ?, ?, ?, 'PARTIAL', datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
          folder_path = excluded.folder_path,
          folder_artist = COALESCE(excluded.folder_artist, album.folder_artist),
          folder_title  = COALESCE(excluded.folder_title,  album.folder_title),
          updated_at    = datetime('now')
        """,
        (album_id, str(album_folder), folder_artist, folder_title),
    )

    con.execute(
        """
        INSERT INTO track (path, album_id, size_bytes, mtime_ns, codec, bit_depth, sample_rate_hz, channels, duration_sec, status, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'DIRTY_META', datetime('now'))
        ON CONFLICT(path) DO UPDATE SET
          album_id       = excluded.album_id,
          size_bytes     = excluded.size_bytes,
          mtime_ns       = excluded.mtime_ns,
          codec          = excluded.codec,
          bit_depth      = excluded.bit_depth,
          sample_rate_hz = excluded.sample_rate_hz,
          channels       = excluded.channels,
          duration_sec   = excluded.duration_sec,
          status         = 'DIRTY_META',
          last_seen      = datetime('now')
        """,
        (fi.path, album_id, fi.size, fi.mtime_ns, fi.codec, fi.bit_depth, fi.sample_rate, fi.channels, fi.duration),
    )

    if disc_number is not None:
        con.execute(
            """
            INSERT INTO disc (album_id, disc_number, disc_title, path)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(album_id, disc_number) DO UPDATE SET
              disc_title = COALESCE(excluded.disc_title, disc.disc_title),
              path       = excluded.path
            """,
            (album_id, disc_number, disc_title, str(parent)),
        )


def load_tags_and_artwork(track_path: str) -> Tuple[Dict[str, Any], bool]:
    ext = Path(track_path).suffix.lower()
    tags: Dict[str, Any] = {}
    artwork = False

    if ext == ".flac":
        f = FLAC(track_path)
        artwork = bool(getattr(f, "pictures", []))
        if f.tags:
            for k, vals in f.tags.items():
                tags[k.upper()] = [str(v) for v in vals]
    elif ext == ".dsf":
        mf = MutagenFile(track_path)
        if mf is not None and hasattr(mf, "tags") and mf.tags:
            try:
                artwork = any(getattr(fr, "FrameID", getattr(fr, "HashKey", b"APIC")).startswith(b"APIC")
                               or fr.__class__.__name__ == "APIC"
                               for fr in mf.tags.values())
            except Exception:
                artwork = any(fr.__class__.__name__ == "APIC" for fr in mf.tags.values())

            for k, v in mf.tags.items():
                try:
                    if hasattr(v, "text"):
                        val = v.text
                        if isinstance(val, (list, tuple)):
                            tags[k] = [str(x) for x in val]
                        else:
                            tags[k] = [str(val)]
                    else:
                        tags[k] = [str(v)]
                except Exception:
                    tags[k] = [str(v)]
    return tags, artwork


def digest_tags(tags: Dict[str, Any]) -> str:
    items: List[str] = []
    for k in sorted(tags.keys()):
        v = tags[k]
        if isinstance(v, list):
            items.append(f"{k}=" + "|".join(v))
        else:
            items.append(f"{k}={v}")
    s = "\n".join(items)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# -------------------------
# Threaded FAST inventory
# -------------------------

def _producer(paths_q: Queue, roots: List[str], ignore_dirs: Optional[List[str]]):
    try:
        for root in roots:
            for p in iter_audio_files(root, ignore_dirs=ignore_dirs):
                paths_q.put(p)
    finally:
        pass  # sentinels inserted by caller


def _worker(paths_q: Queue, infos_q: Queue):
    while True:
        p = paths_q.get()
        if p is None:
            paths_q.task_done()
            break
        try:
            fi = quick_probe(Path(p))
            infos_q.put(fi)
        except Exception:
            pass
        finally:
            paths_q.task_done()


def fast_index_threaded(
    con: sqlite3.Connection,
    roots: List[str],
    ignore_dirs: Optional[List[str]] = None,
    workers: int = 6,
    batch_size: int = 1000,
    progress_every: int = 2000,
) -> int:
    paths_q: Queue = Queue(maxsize=2000)
    infos_q: Queue = Queue(maxsize=2000)

    prod = Thread(target=_producer, args=(paths_q, roots, ignore_dirs), daemon=True)
    prod.start()

    threads: List[Thread] = []
    for _ in range(max(1, int(workers))):
        t = Thread(target=_worker, args=(paths_q, infos_q), daemon=True)
        t.start()
        threads.append(t)

    processed = 0
    last_print = time.time()
    start_wall = last_print

    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("BEGIN;")

    while True:
        try:
            fi = infos_q.get(timeout=0.5)
        except Empty:
            if not prod.is_alive() and paths_q.unfinished_tasks == 0:
                break
            continue

        upsert_track_fast(con, fi)
        processed += 1

        if processed % batch_size == 0:
            con.commit()
            con.execute("BEGIN;")

        if processed % progress_every == 0 or (time.time() - last_print) > 10:
            last_print = time.time()
            elapsed = max(last_print - start_wall, 1e-6)
            rate = processed / elapsed
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] [FAST] processed ~{processed} files… ({rate:.1f} files/sec)", flush=True)

        infos_q.task_done()

    for _ in threads:
        paths_q.put(None)
    paths_q.join()
    for t in threads:
        t.join()

    con.commit()
    return processed


# -------------------------
# CHANGE-ONLY inventory
# -------------------------

@dataclass
class StatEntry:
    album_id: int
    album_folder: str
    relpath: str          # relative to album folder
    abspath: str
    size: int
    mtime_ns: int


def _album_root_and_rel(p: Path) -> Tuple[int, str, str]:
    """Return (album_id, album_folder, relative_path) for a given audio file path."""
    parent = p.parent
    parent_name = parent.name
    m = DISC_FOLDER_RE.match(parent_name)
    if m:
        album_folder = parent.parent
        rel = f"{parent.name}/{p.name}"
    else:
        album_folder = parent
        rel = p.name
    aid = album_id_from_path(str(album_folder))
    return aid, str(album_folder), rel


def changed_collect_stats(
    roots: List[str],
    ignore_dirs: Optional[List[str]],
    debounce_sec: int,
) -> Dict[int, List[StatEntry]]:
    """Walk roots, stat audio files, group them by album_id."""
    cutoff_ns = (time.time() - max(0, debounce_sec)) * 1e9
    albums: Dict[int, List[StatEntry]] = {}
    for root in roots:
        for p in iter_audio_files(root, ignore_dirs=ignore_dirs):
            st = p.stat()
            if st.st_mtime_ns > cutoff_ns:
                # debounce: skip very-recent writes
                continue
            aid, afolder, rel = _album_root_and_rel(p)
            albums.setdefault(aid, []).append(
                StatEntry(
                    album_id=aid,
                    album_folder=afolder,
                    relpath=rel,
                    abspath=str(p),
                    size=st.st_size,
                    mtime_ns=st.st_mtime_ns,
                )
            )
    return albums


def fingerprint_album(entries: List[StatEntry]) -> Tuple[str, int]:
    """Compute stable fingerprint from relpath|size|mtime_ns lines."""
    lines = [f"{e.relpath}|{e.size}|{e.mtime_ns}" for e in entries]
    lines.sort()
    s = "\n".join(lines)
    # xxh3 via xxhash for speed; hex string
    fp = xxhash.xxh3_128_hexdigest(s.encode("utf-8"))
    return fp, len(entries)


def ensure_album_row(con: sqlite3.Connection, album_id: int, album_folder: str):
    folder_artist, folder_title = parse_album_folder(album_folder)
    con.execute(
        """
        INSERT INTO album (id, folder_path, folder_artist, folder_title, status, updated_at)
        VALUES (?, ?, ?, ?, 'PARTIAL', datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
          folder_path = excluded.folder_path,
          folder_artist = COALESCE(excluded.folder_artist, album.folder_artist),
          folder_title  = COALESCE(excluded.folder_title,  album.folder_title),
          updated_at    = datetime('now')
        """,
        (album_id, album_folder, folder_artist, folder_title),
    )


def changed_apply_album(
    con: sqlite3.Connection,
    run_id: int,
    album_id: int,
    album_folder: str,
    entries: List[StatEntry],
):
    """Apply change-only logic for a single album."""
    ensure_album_row(con, album_id, album_folder)

    # Get previous fingerprint (if any)
    row = con.execute("SELECT album_fingerprint FROM album WHERE id=?", (album_id,)).fetchone()
    prev_fp = row[0] if row else None

    fp, count = fingerprint_album(entries)

    if prev_fp == fp:
        # Skip per-file; mark album tracks seen
        con.execute(
            "UPDATE track SET seen_run_id=?, is_missing=0 WHERE album_id=?",
            (run_id, album_id),
        )
        # Touch album metadata
        con.execute(
            "UPDATE album SET item_count=?, updated_at=datetime('now') WHERE id=?",
            (count, album_id),
        )
        return

    # Changed/new album: upsert per-file rows using (path,size,mtime) deltas
    for e in entries:
        # Find existing row
        row = con.execute(
            "SELECT size_bytes, mtime_ns FROM track WHERE path=?",
            (e.abspath,),
        ).fetchone()

        if row is None:
            # New file
            con.execute(
                """
                INSERT INTO track (path, album_id, size_bytes, mtime_ns, status, last_seen, seen_run_id, is_missing)
                VALUES (?, ?, ?, ?, 'DIRTY_META', datetime('now'), ?, 0)
                """,
                (e.abspath, album_id, e.size, e.mtime_ns, run_id),
            )
        else:
            old_size, old_mtime = row
            if int(old_size) != int(e.size) or int(old_mtime) != int(e.mtime_ns):
                # Changed
                con.execute(
                    """
                    UPDATE track SET
                      album_id=?,
                      size_bytes=?,
                      mtime_ns=?,
                      status='DIRTY_META',
                      last_seen=datetime('now'),
                      seen_run_id=?,
                      is_missing=0
                    WHERE path=?
                    """,
                    (album_id, e.size, e.mtime_ns, run_id, e.abspath),
                )
            else:
                # Unchanged file in changed album (e.g., sibling added/removed)
                con.execute(
                    "UPDATE track SET seen_run_id=?, is_missing=0 WHERE path=?",
                    (run_id, e.abspath),
                )

    # Mark deletions within this album (files we didn't see this run)
    con.execute(
        """
        UPDATE track
           SET is_missing=1
         WHERE album_id=?
           AND (seen_run_id IS NULL OR seen_run_id<>?)
        """,
        (album_id, run_id),
    )

    # Update album fingerprint/count
    con.execute(
        "UPDATE album SET album_fingerprint=?, item_count=?, updated_at=datetime('now') WHERE id=?",
        (fp, count, album_id),
    )


def changed_index(
    con: sqlite3.Connection,
    roots: List[str],
    ignore_dirs: Optional[List[str]],
    debounce_sec: int,
    progress_every_albums: int = 200,
) -> Tuple[int, int]:
    """
    Change-only run:
      - group files by album
      - compute fingerprint and compare
      - update tracks seen/changed/missing accordingly
    Returns: (albums_processed, tracks_touched)
    """
    # Run header (event)
    con.execute(
        "INSERT INTO run_event(started_at, command) VALUES(datetime('now'), 'inventory changed')"
    )
    run_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]

    albums = changed_collect_stats(roots, ignore_dirs=ignore_dirs, debounce_sec=debounce_sec)

    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("BEGIN;")

    processed_albums = 0
    touched_tracks = 0

    # Process each album we saw this run
    for aid, entries in albums.items():
        # Count tracks we will touch roughly equals len(entries) for changed albums
        before = con.total_changes
        # Album folder string from first entry
        album_folder = entries[0].album_folder if entries else ""
        changed_apply_album(con, run_id, aid, album_folder, entries)
        after = con.total_changes
        touched_tracks += (after - before)
        processed_albums += 1

        if processed_albums % progress_every_albums == 0:
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] [CHANGED] processed ~{processed_albums} albums…", flush=True)
            con.commit()
            con.execute("BEGIN;")

    # Albums not seen at all this run → mark their tracks missing
    # Build a temp table with seen album ids for this run
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _seen_albums(aid INTEGER PRIMARY KEY)")
    con.execute("DELETE FROM _seen_albums")
    con.executemany("INSERT INTO _seen_albums(aid) VALUES(?)", [(aid,) for aid in albums.keys()])

    con.execute(
        """
        UPDATE track
           SET is_missing=1
         WHERE album_id IN (SELECT id FROM album WHERE id NOT IN (SELECT aid FROM _seen_albums))
        """
    )

    con.commit()
    return processed_albums, touched_tracks