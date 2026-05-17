#!/usr/bin/env python3
"""Sync the music catalog with Last.fm metadata (play counts, tags).

Usage:
    venv/bin/python lastfm_sync.py              # sync albums not synced in last 24 hours
    venv/bin/python lastfm_sync.py --full       # re-sync all albums
    venv/bin/python lastfm_sync.py --limit 10   # sync at most 10 albums (for testing)
    venv/bin/python lastfm_sync.py -v           # verbose / debug logging
"""

import argparse
import logging
import sys

from dotenv import load_dotenv

from catalog.db import get_connection
from catalog.lastfm import LastFmSyncer


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Sync music catalog albums with Last.fm metadata."
    )
    parser.add_argument(
        '--full', action='store_true',
        help="Re-sync all albums, including those synced within the last 24 hours.",
    )
    parser.add_argument(
        '--limit', type=int, metavar='N',
        help="Sync at most N albums (useful for testing).",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    conn = get_connection()
    try:
        syncer = LastFmSyncer(conn)
        syncer.sync_all(force=args.full, limit=args.limit)
    except Exception:
        logging.exception("Last.fm sync failed.")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
