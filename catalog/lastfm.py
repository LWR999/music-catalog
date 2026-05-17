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

    def sync_all(self, force=False, limit=None):
        # Pull the user's full scrobble library and recent tracks up front —
        # far cheaper than one album.getInfo call per DB record.
        scrobble_map = self._fetch_scrobble_map()
        last_played_map = self._fetch_last_played_map()

        albums = self._pending_albums(force, limit)
        log.info("%d album(s) to sync with Last.fm.", len(albums))

        synced = no_match = errors = 0
        for album_id, artist, title in albums:
            key = (artist.lower(), title.lower())
            if key not in scrobble_map:
                log.warning("NO MATCH  %s – %s", artist, title)
                self._mark_no_match(album_id)
                no_match += 1
                continue
            try:
                tags = self._fetch_tags(artist, title)
                data = {
                    'playcount':   scrobble_map[key],
                    'last_played': last_played_map.get(key),
                    'tags':        tags,
                }
                self._store(album_id, data)
                log.info(
                    "SYNCED    %s – %s  (plays=%s, tags=[%s])",
                    artist, title,
                    data['playcount'],
                    ', '.join(t['tag'] for t in tags),
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

    def _fetch_scrobble_map(self):
        """Return {(artist_lower, album_lower): playcount} for all user-scrobbled albums."""
        log.info("Fetching scrobbled albums from Last.fm…")
        result = {}
        try:
            top_albums = self.user.get_top_albums(period=pylast.PERIOD_OVERALL)
            time.sleep(_CALL_INTERVAL)
            for item in top_albums:
                artist = item.item.artist.name
                title = item.item.title
                result[(artist.lower(), title.lower())] = int(item.weight)
            log.info("Scrobble map built: %d unique albums.", len(result))
        except (pylast.WSError, pylast.MalformedResponseError) as e:
            log.error("Could not fetch scrobbled albums: %s", e)
            raise
        return result

    def _fetch_last_played_map(self):
        """Return {(artist_lower, album_lower): datetime} from recent scrobbles."""
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
            log.info("Last-played map built from %d recent scrobbles.", len(recent))
        except (pylast.WSError, pylast.MalformedResponseError) as e:
            log.warning("Could not fetch recent tracks: %s", e)
        return result

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

    def _mark_no_match(self, album_id):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE albums SET lastfm_synced_at = now() WHERE id = %s",
                (album_id,),
            )
        self.conn.commit()


# --------------------------------------------------------------------------- helpers

def _is_not_found(exc):
    return getattr(exc, 'status', None) == 6 or 'not found' in str(exc).lower()
