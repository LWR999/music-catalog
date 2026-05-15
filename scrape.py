#!/usr/bin/env python3
"""CLI entry point for the music catalog scraper.

Usage:
    venv/bin/python scrape.py           # incremental (default, safe for cron)
    venv/bin/python scrape.py --full    # full rebuild from scratch
    venv/bin/python scrape.py -v        # verbose logging
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from catalog.db import get_connection
from catalog.scraper import Scraper


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Scrape FLAC library into music catalog database.")
    parser.add_argument(
        '--full', action='store_true',
        help="Full scrape: process every album regardless of last_scraped timestamp.",
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
        scraper = Scraper(conn)
        if args.full:
            scraper.full_scrape(nas_path)
        else:
            scraper.incremental_scrape(nas_path)
    except Exception:
        logging.exception("Scrape failed.")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
