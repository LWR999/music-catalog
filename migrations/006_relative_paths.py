"""Migration 006: convert absolute NAS paths to paths relative to NAS_MUSIC_PATH.

Reads NAS_MUSIC_PATH from the environment (via .env) and strips that prefix
from albums.nas_path and tracks.file_path.
"""

import os

from dotenv import load_dotenv

load_dotenv()


def run(conn):
    nas_root = os.environ['NAS_MUSIC_PATH'].rstrip('/')
    prefix = nas_root + '/'
    # PostgreSQL substr is 1-indexed; skip past the prefix to get the relative part.
    skip = len(prefix) + 1

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE albums SET nas_path = substr(nas_path, %s) WHERE nas_path LIKE %s",
            (skip, prefix + '%'),
        )
        albums_updated = cur.rowcount

        cur.execute(
            "UPDATE tracks SET file_path = substr(file_path, %s) WHERE file_path LIKE %s",
            (skip, prefix + '%'),
        )
        tracks_updated = cur.rowcount

    conn.commit()
    print(f"  Converted {albums_updated} album paths, {tracks_updated} track paths.")
