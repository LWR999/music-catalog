import logging
import os
import time
from datetime import datetime, timezone

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
        self._last_played_cache = None  # built lazily on first sync

    def sync_all(self, force=False, limit=None):
        albums = self._pending_albums(force, limit)
        log.info("%d album(s) to sync with Last.fm.", len(albums))
        self._ensure_last_played_cache()
        synced = no_match = errors = 0
        for album_id, artist, title in albums:
            try:
                data = self._fetch_album_data(artist, title)
                if data is None:
                    log.warning("NO MATCH  %s – %s", artist, title)
                    self._mark_no_match(album_id)
                    no_match += 1
                else:
                    data['last_played'] = self._last_played_cache.get(
                        (artist.lower(), title.lower())
                    )
                    self._store(album_id, data)
                    log.info(
                        "SYNCED    %s – %s  (plays=%s, tags=[%s])",
                        artist, title,
                        data['playcount'],
                        ', '.join(t['tag'] for t in data['tags']),
                    )
                    synced += 1
            except Exception:
                log.exception("Error syncing album %d (%s – %s)", album_id, artist, title)
                self.conn.rollback()
                errors += 1
        log.info(
            "Last.fm sync complete — %d synced, %d no-match, %d errors.",
            synced, no_match, errors,
        )

    # ------------------------------------------------------------------ private

    def _pending_albums(self, force, limit):
        limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
        with self.conn.cursor() as cur:
            if force:
                cur.execute(f"""
                    SELECT a.id, ar.name, a.title
                    FROM   albums a
                    JOIN   artists ar ON ar.id = a.artist_id
                    ORDER  BY ar.sort_name, a.title
                    {limit_clause}
                """)
            else:
                cur.execute(f"""
                    SELECT a.id, ar.name, a.title
                    FROM   albums a
                    JOIN   artists ar ON ar.id = a.artist_id
                    WHERE  a.lastfm_synced_at IS NULL
                       OR  a.lastfm_synced_at < now() - interval '24 hours'
                    ORDER  BY ar.sort_name, a.title
                    {limit_clause}
                """)
            return cur.fetchall()

    def _ensure_last_played_cache(self):
        if self._last_played_cache is not None:
            return
        cache = {}
        log.info("Fetching recent tracks for last-played cache…")
        try:
            recent = self.user.get_recent_tracks(limit=_RECENT_TRACKS_LIMIT)
            time.sleep(_CALL_INTERVAL)
            for played in recent:
                if not played.album or not played.timestamp:
                    continue
                artist_key = played.track.artist.name.lower()
                album_key = played.album.lower()
                ts = datetime.fromtimestamp(int(played.timestamp), tz=timezone.utc)
                key = (artist_key, album_key)
                if key not in cache or ts > cache[key]:
                    cache[key] = ts
            log.info("Last-played cache built from %d recent scrobbles (%d unique albums).", len(recent), len(cache))
        except (pylast.WSError, pylast.MalformedResponseError) as e:
            log.warning("Could not fetch recent tracks for last-played cache: %s", e)
        self._last_played_cache = cache

    def _fetch_album_data(self, artist, title):
        album = self.network.get_album(artist, title)
        try:
            playcount = album.get_playcount()
            time.sleep(_CALL_INTERVAL)
        except pylast.WSError as e:
            if _is_not_found(e):
                return None
            raise

        try:
            raw_tags = album.get_top_tags(limit=5)
            time.sleep(_CALL_INTERVAL)
        except pylast.WSError as e:
            if _is_not_found(e):
                raw_tags = []
            else:
                raise

        tags = [
            {'tag': t.item.name, 'rank': i + 1}
            for i, t in enumerate(raw_tags)
        ]
        return {'playcount': playcount, 'tags': tags}

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
            cur.execute(
                "DELETE FROM lastfm_tags WHERE album_id = %s",
                (album_id,),
            )
            for tag in data['tags']:
                cur.execute(
                    """
                    INSERT INTO lastfm_tags (album_id, tag, rank)
                    VALUES (%(album_id)s, %(tag)s, %(rank)s)
                    """,
                    {'album_id': album_id, 'tag': tag['tag'], 'rank': tag['rank']},
                )
        self.conn.commit()

    def _mark_no_match(self, album_id):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE albums SET lastfm_synced_at = now() WHERE id = %s",
                (album_id,),
            )
        self.conn.commit()


# --------------------------------------------------------------------------- helpers

def _is_not_found(exc):
    """Return True for Last.fm WSError code 6 (item not found)."""
    return getattr(exc, 'status', None) == 6 or 'not found' in str(exc).lower()
