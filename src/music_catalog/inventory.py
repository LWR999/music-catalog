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
        # signal end-of-stream by placing one sentinel per worker to be consumed later by caller
        pass  # caller will enqueue sentinels


def _worker(paths_q: Queue, infos_q: Queue):
    while True:
        p = paths_q.get()
        if p is None:  # sentinel
            paths_q.task_done()
            break
        try:
            fi = quick_probe(Path(p))
            infos_q.put(fi)
        except Exception as e:
            # Represent errors as a FastInfo with minimal data and mark codec as 'ERROR'
            # (writer can choose to skip or log; for now we skip silently)
            # You could also push a side-channel log here.
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
    """
    Threaded fast pass:
      - 1 producer walks the tree
      - N workers probe headers
      - 1 writer (this thread) upserts in batches within a single transaction
    Returns number of files processed.
    """
    start_wall = time.time()
    paths_q: Queue = Queue(maxsize=2000)
    infos_q: Queue = Queue(maxsize=2000)

    # Start producer
    prod = Thread(target=_producer, args=(paths_q, roots, ignore_dirs), daemon=True)
    prod.start()

    # Start workers
    workers = max(1, int(workers))
    threads: List[Thread] = []
    for _ in range(workers):
        t = Thread(target=_worker, args=(paths_q, infos_q), daemon=True)
        t.start()
        threads.append(t)

    processed = 0
    last_print = time.time()

    # DB optimizations for bulk ingest
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("BEGIN;")

    # Keep consuming until producer is done and all workers have drained
    producer_done = False
    while True:
        try:
            fi = infos_q.get(timeout=0.5)
        except Empty:
            # If producer thread finished and queue is empty and all workers are idle / finished, we break
            if not prod.is_alive() and paths_q.unfinished_tasks == 0:
                break
            continue

        upsert_track_fast(con, fi)
        processed += 1

        if processed % batch_size == 0:
            con.commit()
            con.execute("BEGIN;")

        # lightweight progress ticker
        if processed % progress_every == 0 or (time.time() - last_print) > 10:
            last_print = time.time()
            elapsed = max(last_print - start_wall, 1e-6)
            rate = processed / elapsed
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] [FAST] processed ~{processed} files… ({rate:.1f} files/sec)", flush=True)

        infos_q.task_done()

    # Send sentinels to workers now that producer is done and all paths consumed
    for _ in threads:
        paths_q.put(None)
    paths_q.join()  # ensure workers exit

    # Join worker threads
    for t in threads:
        t.join()

    # Final commit
    con.commit()
    return processed