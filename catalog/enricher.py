import logging
from difflib import SequenceMatcher

import musicbrainzngs

log = logging.getLogger(__name__)

musicbrainzngs.set_useragent("music-catalog", "0.1", "https://github.com/LWR999/music-catalog")
musicbrainzngs.set_rate_limit(limit_or_interval=1.0)
logging.getLogger('musicbrainzngs').setLevel(logging.WARNING)

_MIN_SCORE = 85       # below this threshold treat as no match
_MIN_TITLE_SIM = 0.6  # MB score can be 100 on artist alone; also require title similarity


class Enricher:
    def __init__(self, conn):
        self.conn = conn

    def enrich_all(self, force=False):
        albums = self._pending_albums(force)
        log.info("%d album(s) to enrich.", len(albums))
        matched = no_match = errors = 0
        for album_id, artist, title in albums:
            try:
                result = self._query_mb(artist, title)
                self._store(album_id, result)
                if result['mb_confidence'] == 'none':
                    log.info("NO MATCH  %s – %s", artist, title)
                    no_match += 1
                else:
                    log.info(
                        "%-5s     %s – %s  →  %s",
                        result['mb_confidence'].upper(),
                        artist, title,
                        result['mb_release_id'],
                    )
                    matched += 1
            except Exception:
                log.exception("Error enriching album %d (%s – %s)", album_id, artist, title)
                self.conn.rollback()
                errors += 1
        log.info(
            "Enrichment complete — %d matched, %d no-match, %d errors.",
            matched, no_match, errors,
        )

    # ------------------------------------------------------------------ private

    def _pending_albums(self, force):
        with self.conn.cursor() as cur:
            if force:
                cur.execute("""
                    SELECT a.id, ar.name, a.title
                    FROM   albums a
                    JOIN   artists ar ON ar.id = a.artist_id
                    ORDER  BY ar.sort_name, a.title
                """)
            else:
                cur.execute("""
                    SELECT a.id, ar.name, a.title
                    FROM   albums a
                    JOIN   artists ar ON ar.id = a.artist_id
                    WHERE  a.mb_enriched_at IS NULL
                    ORDER  BY ar.sort_name, a.title
                """)
            return cur.fetchall()

    def _query_mb(self, artist, title):
        try:
            resp = musicbrainzngs.search_releases(artist=artist, release=title, limit=5)
        except musicbrainzngs.WebServiceError as e:
            log.warning("MusicBrainz request failed for '%s – %s': %s", artist, title, e)
            return _no_match()

        releases = resp.get('release-list', [])
        if not releases:
            log.debug("MB returned no candidates")
            return _no_match()

        log.debug("MB candidates:")
        for r in releases:
            log.debug(
                "  [%3s] %s – %s (%s) label=%s country=%s id=%s",
                r.get('ext:score', '?'),
                r.get('artist-credit-phrase', '?'),
                r.get('title', '?'),
                r.get('date', ''),
                _extract_label(r),
                r.get('country', ''),
                r.get('id', ''),
            )

        best = releases[0]
        score = int(best.get('ext:score', 0))
        if score < _MIN_SCORE:
            return _no_match()

        title_sim = SequenceMatcher(None, title.lower(), best.get('title', '').lower()).ratio()
        if title_sim < _MIN_TITLE_SIM:
            log.debug("MB title mismatch (sim=%.2f): '%s' vs '%s'", title_sim, title, best.get('title', ''))
            return _no_match()

        confidence = 'exact' if score == 100 else 'fuzzy'
        result = {
            'mb_release_id':   best.get('id'),
            'release_year':    _extract_year(best.get('date', '')),
            'record_label':    _extract_label(best),
            'release_country': best.get('country'),
            'mb_confidence':   confidence,
        }
        log.debug(
            "MB result (score=%s): id=%s year=%s label=%s country=%s",
            score,
            result['mb_release_id'],
            result['release_year'],
            result['record_label'],
            result['release_country'],
        )
        return result

    def _store(self, album_id, result):
        if result['mb_release_id']:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM albums WHERE mb_release_id = %s AND id != %s",
                    (result['mb_release_id'], album_id),
                )
                if cur.fetchone():
                    log.warning(
                        "MB release %s already assigned to another album; skipping for album %d",
                        result['mb_release_id'], album_id,
                    )
                    result = _no_match()

        log.debug(
            "Writing album %d: %s",
            album_id,
            {k: v for k, v in result.items() if v is not None},
        )
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE albums SET
                    mb_release_id   = %(mb_release_id)s,
                    release_year    = %(release_year)s,
                    record_label    = %(record_label)s,
                    release_country = %(release_country)s,
                    mb_confidence   = %(mb_confidence)s,
                    mb_enriched_at  = now()
                WHERE id = %(album_id)s
                """,
                {**result, 'album_id': album_id},
            )
        self.conn.commit()


# --------------------------------------------------------------------------- helpers

def _no_match():
    return {
        'mb_release_id':   None,
        'release_year':    None,
        'record_label':    None,
        'release_country': None,
        'mb_confidence':   'none',
    }


def _extract_year(date_str):
    """Parse YYYY, YYYY-MM, or YYYY-MM-DD into an integer year."""
    if not date_str:
        return None
    try:
        return int(date_str[:4])
    except (ValueError, IndexError):
        return None


def _extract_label(release):
    """Return the first record label name from a MusicBrainz release dict."""
    for entry in release.get('label-info-list', []):
        label = entry.get('label')
        if label and label.get('name'):
            return label['name']
    return None
