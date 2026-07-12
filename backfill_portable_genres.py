#!/usr/bin/env python3
"""One-off: overwrite GENRE tags in FLACs under ~/drobos/hibiki/Portable/
with canonical genres from the music catalog DB, matched by artist + album title."""

import logging
import os
import sys
from pathlib import Path

import mutagen.flac
from dotenv import load_dotenv
from mutagen.flac import FLAC

from catalog.db import get_connection


def _normalize(s):
    import re
    s = s.lower()
    s = re.sub(r'[^\w\s]', '', s)
    return re.sub(r'\s+', ' ', s).strip()

log = logging.getLogger(__name__)

PORTABLE_ROOT = Path.home() / 'drobos/hibiki/Portable'


def load_genre_map(conn):
    """Return {(norm_artist, norm_title): [genre, ...]} for all normalised albums."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ar.name, a.title, ARRAY_AGG(g.name ORDER BY g.name)
            FROM albums a
            JOIN artists ar ON ar.id = a.artist_id
            JOIN album_genres ag ON ag.album_id = a.id
            JOIN genres g ON g.id = ag.genre_id
            WHERE a.genre_normalised_at IS NOT NULL
            GROUP BY ar.name, a.title
        """)
        return {
            (_normalize(artist), _normalize(title)): genres
            for artist, title, genres in cur.fetchall()
        }


def tag(f, *keys):
    for key in keys:
        vals = f.tags.get(key.lower()) or f.tags.get(key.upper()) or []
        if vals:
            return vals[0].strip()
    return None


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    conn = get_connection()
    genre_map = load_genre_map(conn)
    conn.close()
    log.info("Loaded %d albums from DB.", len(genre_map))

    matched = skipped = missing = 0
    for flac_path in sorted(PORTABLE_ROOT.rglob('*.flac')):
        if flac_path.name.startswith('._'):
            continue
        try:
            f = FLAC(str(flac_path))
        except mutagen.flac.error as e:
            log.warning("Cannot read %s: %s", flac_path, e)
            skipped += 1
            continue

        artist = tag(f, 'albumartist', 'artist')
        title  = tag(f, 'album')
        if not artist or not title:
            log.warning("Missing artist/album tags: %s", flac_path)
            skipped += 1
            continue

        key = (_normalize(artist), _normalize(title))
        genres = genre_map.get(key)
        if not genres:
            log.info("NO MATCH  %s – %s", artist, title)
            missing += 1
            continue

        f['GENRE'] = genres
        f.save()
        log.info("UPDATED   %-50s  → %s", f"{artist} – {title}"[:50], genres)
        matched += 1

    log.info("\nDone — %d updated, %d no match, %d skipped.", matched, missing, skipped)


if __name__ == '__main__':
    main()
