#!/usr/bin/env python3
"""Normalise album genre tags using Claude Haiku.

Usage:
    venv/bin/python genre_normalise.py           # normalise albums not yet tagged
    venv/bin/python genre_normalise.py --full    # reprocess entire library
    venv/bin/python genre_normalise.py -v        # verbose logging
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from catalog.db import get_connection
from catalog.genre import GenreNormaliser


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Normalise genre tags across the music catalog.")
    parser.add_argument(
        '--full', action='store_true',
        help="Reprocess all albums, clearing existing normalised tags.",
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

    nas_path = os.environ.get('NAS_MUSIC_PATH')
    if not nas_path:
        print("ERROR: NAS_MUSIC_PATH is not set in .env", file=sys.stderr)
        sys.exit(1)

    conn = get_connection()
    try:
        normaliser = GenreNormaliser(conn, nas_path)
        if args.full:
            normaliser.normalise_all()
        else:
            normaliser.normalise_pending()
    except Exception:
        logging.exception("Genre normalisation failed.")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
