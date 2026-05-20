#!/usr/bin/env python3
"""One-off migration: set added_at from NAS folder mtime for all existing albums.

Run once after adding the added_at column. Safe to re-run — only updates rows
where the folder exists on the NAS.

Usage:
    venv/bin/python migrate_added_at.py
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from catalog.db import get_connection

load_dotenv()

nas_root = os.environ.get("NAS_MUSIC_PATH")
if not nas_root:
    print("ERROR: NAS_MUSIC_PATH not set", file=sys.stderr)
    sys.exit(1)

nas_root = Path(nas_root)
conn = get_connection()

try:
    with conn.cursor() as cur:
        cur.execute("SELECT id, nas_path FROM albums ORDER BY nas_path")
        rows = cur.fetchall()

    updated = 0
    missing = 0
    for album_id, nas_path in rows:
        folder = nas_root / nas_path
        try:
            mtime = folder.stat().st_mtime
        except OSError:
            missing += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE albums SET added_at = to_timestamp(%s) WHERE id = %s",
                (mtime, album_id),
            )
        updated += 1

    conn.commit()
    print(f"Updated {updated} albums from NAS mtime.")
    if missing:
        print(f"Skipped {missing} albums (folder not found on NAS).")
finally:
    conn.close()
