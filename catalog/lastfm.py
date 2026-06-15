import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import psycopg2.extras
import pylast

log = logging.getLogger(__name__)

_CALL_INTERVAL = 0.25  # seconds between API calls (~4/sec, well within Last.fm limits)
_RECENT_TRACKS_LIMIT = 200  # scrobbles to prefetch for last-played cache (API max per page)


class LastFmSyncer:
    def __init__(self, conn):
        self.conn = conn
        api_key = os.environ["LASTFM_API_KEY"]
        username = os.environ["LASTFM_USERNAME"]
        self.network = pylast.LastFMNetwork(api_key=api_key)
        self.user = self.network.get_user(username)

    def sync_all(self, force=False, limit=None):
        scrobble_map = self._fetch_scrobble_map()
        last_played_map = self._fetch_last_played_map()
        album_map = self._load_album_map()

        to_sync = self._plan(scrobble_map, album_map, force, limit)
        log.info("%d scrobbled album(s) to sync.", len(to_sync))

        synced = errors = 0
        for album_id, artist, title, playcount in to_sync:
            try:
                tags = self._fetch_tags(artist, title)
                self._store(album_id, {
                    'playcount':   playcount,
                    'last_played': last_played_map.get((artist.lower(), title.lower())),
                    'tags':        tags,
                })
                log.info(
                    "SYNCED  %s – %s  (plays=%s, tags=[%s])",
                    artist, title, playcount,
                    ', '.join(t['tag'] for t in tags),
                )
                synced += 1
            except Exception:
                log.exception("Error syncing album %d (%s – %s)", album_id, artist, title)
                self.conn.rollback()
                errors += 1

        log.info("Last.fm sync complete — %d synced, %d errors.", synced, errors)

    def sync_play_counts(self):
        """Bulk-fetch all top albums from Last.fm and write play_count for every DB album."""
        api_key = os.environ["LASTFM_API_KEY"]
        username = os.environ["LASTFM_USERNAME"]

        log.info("Fetching top albums from Last.fm for play count sync…")
        top = self._fetch_top_albums_paginated(api_key, username)
        log.info("Fetched %d scrobbled albums from Last.fm.", len(top))

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, ar.name, a.title
                FROM albums a JOIN artists ar ON ar.id = a.artist_id
            """)
            rows = cur.fetchall()

        updates = [
            (top.get((_normalize(artist), _normalize(title)), 0), album_id)
            for album_id, artist, title in rows
        ]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                "UPDATE albums SET play_count = %s WHERE id = %s",
                updates,
            )
        self.conn.commit()

        matched = sum(1 for pc, _ in updates if pc > 0)
        log.info("play_count sync complete — %d/%d albums matched.", matched, len(updates))

    # ------------------------------------------------------------------ private

    def _fetch_top_albums_paginated(self, api_key, username):
        """Return {(norm_artist, norm_title): playcount} for all scrobbled albums, paginated."""
        result = {}
        page = 1
        while True:
            data = _lastfm_get(api_key, {
                'method': 'user.getTopAlbums',
                'user': username,
                'period': 'overall',
                'limit': '500',
                'page': str(page),
            })
            albums = data.get('topalbums', {}).get('album', [])
            if not albums:
                break
            for album in albums:
                artist = album['artist']['name']
                title = album['name']
                result[(_normalize(artist), _normalize(title))] = int(album['playcount'])
            attrs = data.get('topalbums', {}).get('@attr', {})
            total_pages = int(attrs.get('totalPages', 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(_CALL_INTERVAL)
        return result

    def _fetch_scrobble_map(self):
        """Return {(artist_lower, title_lower): (playcount, artist, title)} for all scrobbled albums."""
        log.info("Fetching scrobbled albums from Last.fm…")
        result = {}
        try:
            top_albums = self.user.get_top_albums(period=pylast.PERIOD_OVERALL)
            time.sleep(_CALL_INTERVAL)
            for item in top_albums:
                artist = item.item.artist.name
                title = item.item.title
                result[(artist.lower(), title.lower())] = (int(item.weight), artist, title)
            log.info("Scrobble map: %d unique albums.", len(result))
        except (pylast.WSError, pylast.MalformedResponseError) as e:
            log.error("Could not fetch scrobbled albums: %s", e)
            raise
        return result

    def _fetch_last_played_map(self):
        """Return {(artist_lower, title_lower): datetime} from recent scrobbles."""
        log.info("Fetching recent tracks for last-played timestamps…")
        result = {}
        try:
            recent = self.user.get_recent_tracks(limit=_RECENT_TRACKS_LIMIT)
            time.sleep(_CALL_INTERVAL)
            for played in recent:
                if not played.album or not played.timestamp:
                    continue
                key = (played.track.artist.name.lower(), played.album.lower())
                ts = datetime.fromtimestamp(int(played.timestamp), tz=timezone.utc)
                if key not in result or ts > result[key]:
                    result[key] = ts
            log.info("Last-played map: %d recent scrobbles.", len(recent))
        except (pylast.WSError, pylast.MalformedResponseError) as e:
            log.warning("Could not fetch recent tracks: %s", e)
        return result

    def _load_album_map(self):
        """Return {(artist_lower, title_lower): (album_id, lastfm_synced_at)} for all DB albums."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, ar.name, a.title, a.lastfm_synced_at
                FROM   albums a
                JOIN   artists ar ON ar.id = a.artist_id
            """)
            return {
                (name.lower(), title.lower()): (album_id, synced_at)
                for album_id, name, title, synced_at in cur.fetchall()
            }

    def _plan(self, scrobble_map, album_map, force, limit):
        """Return list of (album_id, artist, title, playcount) to sync this run."""
        cutoff = datetime.now(tz=timezone.utc).replace(microsecond=0)
        to_sync = []
        in_catalog = 0
        for key, (playcount, artist, title) in scrobble_map.items():
            if key not in album_map:
                continue  # scrobbled but not in our catalog
            in_catalog += 1
            album_id, synced_at = album_map[key]
            if not force and synced_at is not None:
                age = (cutoff - synced_at.replace(tzinfo=timezone.utc)).total_seconds()
                if age < 86400:
                    continue  # synced within last 24 hours
            to_sync.append((album_id, artist, title, playcount))

        log.info(
            "%d scrobbled album(s) found in catalog (of %d scrobbled, %d in DB).",
            in_catalog, len(scrobble_map), len(album_map),
        )
        if limit is not None:
            to_sync = to_sync[:int(limit)]
        return to_sync

    def _fetch_tags(self, artist, title):
        """Return [{tag, rank}, …] for up to 5 top tags."""
        try:
            raw = self.network.get_album(artist, title).get_top_tags(limit=5)
            time.sleep(_CALL_INTERVAL)
        except pylast.WSError as e:
            if _is_not_found(e):
                return []
            raise
        return [{'tag': t.item.name, 'rank': i + 1} for i, t in enumerate(raw)]

    def _store(self, album_id, data):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE albums SET
                    lastfm_playcount   = %(playcount)s,
                    lastfm_last_played = %(last_played)s,
                    lastfm_synced_at   = now()
                WHERE id = %(album_id)s
                """,
                {
                    'playcount':   data['playcount'],
                    'last_played': data['last_played'],
                    'album_id':    album_id,
                },
            )
            cur.execute("DELETE FROM lastfm_tags WHERE album_id = %s", (album_id,))
            for tag in data['tags']:
                cur.execute(
                    """
                    INSERT INTO lastfm_tags (album_id, tag, rank)
                    VALUES (%(album_id)s, %(tag)s, %(rank)s)
                    """,
                    {'album_id': album_id, 'tag': tag['tag'], 'rank': tag['rank']},
                )
        self.conn.commit()


# --------------------------------------------------------------------------- helpers

def _is_not_found(exc):
    return getattr(exc, 'status', None) == 6 or 'not found' in str(exc).lower()


def _normalize(s):
    """Lowercase, strip punctuation, collapse whitespace — for fuzzy album matching."""
    s = s.lower()
    s = re.sub(r'[^\w\s]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def _lastfm_get(api_key, params):
    """GET the Last.fm API and return parsed JSON."""
    p = dict(params, api_key=api_key, format='json')
    url = 'https://ws.audioscrobbler.com/2.0/?' + urllib.parse.urlencode(p)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())
