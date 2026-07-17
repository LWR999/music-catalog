import json
import logging
import os
import time
from pathlib import Path

import anthropic
from mutagen.flac import FLAC
import mutagen.flac

log = logging.getLogger(__name__)

CANONICAL_GENRES = [
    "Jazz", "Acid Jazz", "Jazz Fusion", "Contemporary Jazz", "Jazz Vocal", "Latin Jazz",
    "Electronic", "House", "Techno", "Electronica", "Downtempo", "Ambient", "Trip Hop",
    "Disco", "Salsoul",
    "Funk", "Soul", "R&B", "Rare Groove",
    "Hip-Hop",
    "Rock", "Pop", "Alternative",
    "Latin",
    "Blues", "Reggae", "Classical", "World", "Soundtrack", "Library Music", "Easy Listening",
    "Christmas",
]

# Child → additional parents to add programmatically
INHERITANCE = {
    "Acid Jazz":        ["Jazz"],
    "Jazz Fusion":      ["Jazz"],
    "Contemporary Jazz":["Jazz"],
    "Jazz Vocal":       ["Jazz"],
    "Latin Jazz":       ["Jazz", "Latin"],
    "House":            ["Electronic"],
    "Techno":           ["Electronic"],
    "Electronica":      ["Electronic"],
    "Downtempo":        ["Electronic"],
    "Ambient":          ["Electronic"],
    "Trip Hop":         ["Downtempo", "Electronic"],
    "Salsoul":          ["Disco"],
}

_SYSTEM_PROMPT = f"""You are a music genre classifier. Assign genres from this canonical list only:

{", ".join(CANONICAL_GENRES)}

Rules:
1. Return 1–3 genres as a raw JSON array with NO markdown, NO code blocks, NO explanation — e.g. ["Soul", "Funk"]
2. Use ONLY genres from the list above — no others
3. Do NOT add parent genres (e.g. if you pick "Acid Jazz", omit "Jazz" — inheritance is handled separately)
4. Use artist name, album title, and any provided tags as evidence; ignore tags that seem wrong or generic
5. Your entire response must be a single JSON array and nothing else"""

_CALL_INTERVAL = 0.1  # seconds between API calls


def apply_inheritance(tags):
    """Expand tags with their parents, preserving original tags."""
    result = list(tags)
    seen = set(tags)
    for tag in tags:
        for parent in INHERITANCE.get(tag, []):
            if parent not in seen:
                result.append(parent)
                seen.add(parent)
    return result


class GenreNormaliser:
    def __init__(self, conn, nas_root):
        self.conn = conn
        self.nas_root = Path(nas_root)
        self.client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    def normalise_pending(self):
        """Normalise all albums where genre_normalised_at IS NULL."""
        albums = self._load_albums(pending_only=True)
        log.info("%d album(s) pending genre normalisation.", len(albums))
        self._run(albums)

    def normalise_all(self):
        """Clear genre_normalised_at for all albums and reprocess."""
        with self.conn.cursor() as cur:
            cur.execute("UPDATE albums SET genre_normalised_at = NULL")
        self.conn.commit()
        albums = self._load_albums(pending_only=False)
        log.info("%d album(s) queued for genre normalisation.", len(albums))
        self._run(albums)

    # ------------------------------------------------------------------ private

    def _run(self, albums):
        done = errors = 0
        for album in albums:
            try:
                self._normalise_album(*album)
                done += 1
            except Exception:
                log.exception("Failed to normalise album %d (%s – %s)", album[0], album[1], album[2])
                errors += 1
            time.sleep(_CALL_INTERVAL)
        log.info("Genre normalisation complete — %d done, %d errors.", done, errors)
        self._delete_orphaned_genres()

    def _delete_orphaned_genres(self):
        """Delete genre rows left with no albums after normalisation repoints them."""
        with self.conn.cursor() as cur:
            cur.execute("""
                DELETE FROM genres g
                WHERE NOT EXISTS (SELECT 1 FROM album_genres ag WHERE ag.genre_id = g.id)
            """)
            deleted = cur.rowcount
        self.conn.commit()
        if deleted:
            log.info("Deleted %d orphaned genre(s).", deleted)

    def _load_albums(self, pending_only):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, ar.name, a.title, a.nas_path,
                       ARRAY_AGG(DISTINCT g.name)  FILTER (WHERE g.name  IS NOT NULL) AS raw_genres,
                       ARRAY_AGG(DISTINCT lt.tag)  FILTER (WHERE lt.tag  IS NOT NULL) AS lastfm_tags
                FROM   albums a
                JOIN   artists ar ON ar.id = a.artist_id
                LEFT JOIN album_genres ag ON ag.album_id = a.id
                LEFT JOIN genres g         ON g.id = ag.genre_id
                LEFT JOIN lastfm_tags lt   ON lt.album_id = a.id
                WHERE  (%s = FALSE OR a.genre_normalised_at IS NULL)
                GROUP  BY a.id, ar.name, a.title, a.nas_path
                ORDER  BY ar.name, a.title
            """, (pending_only,))
            return cur.fetchall()

    def _normalise_album(self, album_id, artist, title, nas_path, raw_genres, lastfm_tags):
        raw_genres = raw_genres or []
        lastfm_tags = lastfm_tags or []

        assigned = self._call_llm(artist, title, raw_genres, lastfm_tags)
        if not assigned:
            log.warning("No genres returned for %s – %s, skipping.", artist, title)
            return

        final = apply_inheritance(assigned)

        with self.conn.cursor() as cur:
            self._write_db(cur, album_id, final)
        self.conn.commit()

        self._write_flacs(nas_path, final)

        log.info("%-50s  %s → %s", f"{artist} – {title}"[:50], raw_genres, final)

    def _call_llm(self, artist, title, raw_genres, lastfm_tags):
        user_content = (
            f"Artist: {artist}\n"
            f"Album: {title}\n"
            f"FLAC genre tags: {', '.join(raw_genres) or 'none'}\n"
            f"Last.fm tags: {', '.join(lastfm_tags) or 'none'}"
        )
        response = self.client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=64,
            system=[{"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_content}],
        )
        text = response.content[0].text.strip()
        # Strip markdown code fences if the model added them
        if text.startswith('```'):
            text = text.split('\n', 1)[-1].rsplit('```', 1)[0].strip()
        try:
            tags = json.loads(text)
            valid = [t for t in tags if t in CANONICAL_GENRES]
            if not valid:
                log.warning("LLM returned no valid canonical genres for %s – %s: %r", artist, title, tags)
            return valid
        except (json.JSONDecodeError, TypeError):
            log.warning("LLM returned unparseable response for %s – %s: %r", artist, title, text)
            return []

    def _write_db(self, cur, album_id, genres):
        cur.execute("DELETE FROM album_genres WHERE album_id = %s", (album_id,))
        for name in genres:
            cur.execute(
                """
                INSERT INTO genres (name) VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
                """,
                (name,),
            )
            genre_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO album_genres (album_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (album_id, genre_id),
            )
        cur.execute(
            "UPDATE albums SET genre_normalised_at = now() WHERE id = %s",
            (album_id,),
        )

    def _write_flacs(self, nas_path, genres):
        album_dir = self.nas_root / nas_path
        for flac_path in sorted(album_dir.rglob('*.flac')):
            if flac_path.name.startswith('._'):
                continue
            try:
                f = FLAC(str(flac_path))
                f['GENRE'] = genres
                f.save()
            except mutagen.flac.error as e:
                log.warning("Could not write genre to %s: %s", flac_path, e)
