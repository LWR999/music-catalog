#!/usr/bin/env python3
"""Enrich the music catalog with MusicBrainz metadata.

Usage:
    venv/bin/python enrich.py           # enrich albums without a MB match
    venv/bin/python enrich.py --force   # re-enrich all albums, including matched ones
    venv/bin/python enrich.py -v        # verbose / debug logging
"""

import argparse
import logging
import sys

from dotenv import load_dotenv

from catalog.db import get_connection
from catalog.enricher import Enricher


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Enrich music catalog albums with MusicBrainz metadata."
    )
    parser.add_argument(
        '--force', action='store_true',
        help="Re-enrich albums that already have a MusicBrainz release ID.",
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
        enricher = Enricher(conn)
        enricher.enrich_all(force=args.force)
    except Exception:
        logging.exception("Enrichment failed.")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
