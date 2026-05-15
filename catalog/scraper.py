import hashlib
import logging
import re
from pathlib import Path

from mutagen.flac import FLAC, FLACNoHeaderError

log = logging.getLogger(__name__)

# Matches DISC 1, DISC1, DISK 2, CD1, CD 3, etc.
_DISC_DIR_RE = re.compile(r'^(?:DISC|DISK)\s*\d+$|^CD\s*\d+$', re.IGNORECASE)
# Artwork / scan folders to skip
_ARTWORK_DIR_RE = re.compile(r'^(?:artwork|scans?|covers?|booklet|bonus|extras?)$', re.IGNORECASE)


# --------------------------------------------------------------------------- tag helpers

def _tag(f, key, default=None):
    vals = f.tags.get(key.lower()) or f.tags.get(key.upper()) or []
    return vals[0].strip() if vals else default


def _tag_int(f, key, default=None):
    val = _tag(f, key)
    if not val:
        return default
    try:
        return int(val.split('/')[0].strip())
    except ValueError:
        return default


def _tag_float(f, key, default=None):
    val = _tag(f, key)
    if not val:
        return default
    try:
        return float(val.replace(' dB', '').strip())
    except ValueError:
        return default


def _tag_genres(f):
    """Return list of genres, handling both multi-entry and semicolon-separated."""
    entries = f.tags.get('genre') or f.tags.get('GENRE') or []
    genres = []
    for entry in entries:
        for part in entry.split(';'):
            part = part.strip()
            if part:
                genres.append(part)
    return genres


# --------------------------------------------------------------------------- path helpers

def _is_disc_dir(path):
    return bool(_DISC_DIR_RE.match(path.name))


def _is_artwork_dir(path):
    return bool(_ARTWORK_DIR_RE.match(path.name))


def _disc_number_from_dir(path):
    m = re.search(r'\d+', path.name)
    return int(m.group()) if m else 1


def _flacs_in_dir(d):
    return sorted(f for f in d.iterdir() if f.is_file() and f.suffix.lower() == '.flac')


def _is_album_dir(path):
    """True if path directly contains FLACs or disc subdirectories."""
    if not path.is_dir():
        return False
    for entry in path.iterdir():
        if entry.is_file() and entry.suffix.lower() == '.flac':
            return True
        if entry.is_dir() and _is_disc_dir(entry):
            return True
    return False


def _content_hash(album_dir):
    """
    MD5 of sorted (relative_path, file_size) for all FLACs under album_dir.
    Cheap (no file reads), reliable on NAS where mtime cannot be trusted.
    """
    entries = sorted(
        f"{f.relative_to(album_dir)}:{f.stat().st_size}"
        for f in album_dir.rglob('*.flac')
    )
    return hashlib.md5('\n'.join(entries).encode()).hexdigest()


def _collect_flacs(album_dir):
    """
    Return sorted list of (disc_number, Path) for all FLACs in an album dir.
    Handles flat albums and DISC x / CDx subfolders.
    """
    disc_dirs = sorted(
        (d for d in album_dir.iterdir() if d.is_dir() and _is_disc_dir(d)),
        key=_disc_number_from_dir,
    )
    if disc_dirs:
        flacs = []
        for disc_dir in disc_dirs:
            disc_num = _disc_number_from_dir(disc_dir)
            for f in _flacs_in_dir(disc_dir):
                flacs.append((disc_num, f))
        return flacs
    return [(1, f) for f in _flacs_in_dir(album_dir)]


def _parse_folder_name(name):
    """Split 'Artist - Album Title' into (artist, title). Returns (None, name) if no separator."""
    if ' - ' in name:
        artist, _, title = name.partition(' - ')
        return artist.strip(), title.strip()
    return None, name.strip()


def _sort_name(name):
    for prefix in ('The ', 'A ', 'An '):
        if name.startswith(prefix):
            return name[len(prefix):] + ', ' + prefix.strip()
    return name


# --------------------------------------------------------------------------- scraper

class Scraper:
    def __init__(self, conn):
        self.conn = conn
        self._nas_root = None  # set at start of each scrape; used for relative path storage

    # ------------------------------------------------------------------ public

    def full_scrape(self, nas_path):
        """Process every album on the NAS; remove any DB entries no longer present."""
        log.info("Full scrape: %s", nas_path)
        self._nas_root = Path(nas_path)
        found, count, errors = set(), 0, 0
        for album_dir, format_name, _ in self._walk_nas(self._nas_root):
            path_str = str(album_dir.relative_to(self._nas_root))
            found.add(path_str)
            try:
                self._scrape_album(album_dir, format_name)
                count += 1
                log.info("[%d] %s", count, album_dir.name)
            except Exception:
                errors += 1
                log.exception("Failed to scrape %s", album_dir)
        self._remove_deleted(found)
        log.info("Full scrape complete — %d albums processed, %d errors.", count, errors)

    def incremental_scrape(self, nas_path):
        """Process only albums whose content has changed; remove deleted entries."""
        log.info("Incremental scrape: %s", nas_path)
        self._nas_root = Path(nas_path)
        stored_hashes = self._load_content_hashes()
        found, count, errors = set(), 0, 0
        for album_dir, format_name, _ in self._walk_nas(self._nas_root):
            path_str = str(album_dir.relative_to(self._nas_root))
            found.add(path_str)
            current_hash = _content_hash(album_dir)
            if stored_hashes.get(path_str) != current_hash:
                try:
                    self._scrape_album(album_dir, format_name)
                    count += 1
                    log.info("[%d] %s", count, album_dir.name)
                except Exception:
                    errors += 1
                    log.exception("Failed to scrape %s", album_dir)
        self._remove_deleted(found)
        log.info("Incremental scrape complete — %d albums updated, %d errors.", count, errors)

    # --------------------------------------------------------------- NAS walk

    def _walk_nas(self, nas_root):
        """
        Yield (album_dir, format_name, boxset_name) for every album found.

        Structure:
          nas_root / format_dir / album_dir
          nas_root / format_dir / boxset_dir / album_dir
        """
        for format_dir in sorted(nas_root.iterdir()):
            if not format_dir.is_dir():
                continue
            format_name = format_dir.name
            for entry in sorted(format_dir.iterdir()):
                if not entry.is_dir():
                    continue
                if _is_album_dir(entry):
                    yield entry, format_name, None
                elif not _is_artwork_dir(entry):
                    # Treat as potential boxset — walk one level deeper
                    for sub in sorted(entry.iterdir()):
                        if sub.is_dir() and not _is_artwork_dir(sub) and _is_album_dir(sub):
                            yield sub, format_name, entry.name

    # -------------------------------------------------------------- per-album

    def _scrape_album(self, album_dir, format_name):
        flacs = _collect_flacs(album_dir)
        if not flacs:
            log.debug("No FLACs in %s, skipping.", album_dir)
            return

        _, first_flac = flacs[0]
        try:
            meta = FLAC(str(first_flac))
        except FLACNoHeaderError as e:
            raise ValueError(f"Cannot read FLAC header: {e}") from e

        folder_artist, folder_title = _parse_folder_name(album_dir.name)

        artist_name = (
            _tag(meta, 'albumartist')
            or _tag(meta, 'artist')
            or folder_artist
            or 'Unknown Artist'
        )
        title = _tag(meta, 'album') or folder_title
        year = _tag_int(meta, 'date') or _tag_int(meta, 'year')
        original_year = _tag_int(meta, 'originaldate') or _tag_int(meta, 'originalyear')
        is_compilation = (
            _tag(meta, 'compilation') in ('1', 'true', 'True', 'yes')
            or artist_name.lower() == 'various artists'
        )
        label = _tag(meta, 'label') or _tag(meta, 'organization')
        catalog_number = _tag(meta, 'catalognumber') or _tag(meta, 'catalog')
        disc_subtitle = _tag(meta, 'discsubtitle') or _tag(meta, 'setsubtitle')
        rg_album_gain = _tag_float(meta, 'replaygain_album_gain')
        rg_album_peak = _tag_float(meta, 'replaygain_album_peak')
        genres = _tag_genres(meta)

        # Derive disc_count from tags, falling back to counting disc subdirs
        disc_count = _tag_int(meta, 'disctotal') or _tag_int(meta, 'totaldiscs')
        if not disc_count:
            disc_dirs = [d for d in album_dir.iterdir() if d.is_dir() and _is_disc_dir(d)]
            disc_count = len(disc_dirs) if disc_dirs else 1

        with self.conn.cursor() as cur:
            format_id = self._get_or_create_format(cur, format_name)
            artist_id = self._get_or_create_artist(cur, artist_name)
            album_id = self._upsert_album(cur, {
                'title': title,
                'artist_id': artist_id,
                'format_id': format_id,
                'year': year,
                'is_compilation': is_compilation,
                'disc_count': disc_count,
                'nas_path': str(album_dir.relative_to(self._nas_root)),
                'label': label,
                'catalog_number': catalog_number,
                'original_year': original_year,
                'disc_subtitle': disc_subtitle,
                'rg_album_gain': rg_album_gain,
                'rg_album_peak': rg_album_peak,
                'content_hash': _content_hash(album_dir),
            })
            self._upsert_genres(cur, album_id, genres)
            self._replace_tracks(cur, album_id, flacs, is_compilation)

        self.conn.commit()

    # ------------------------------------------------------------ DB helpers

    def _get_or_create_format(self, cur, name):
        cur.execute(
            """
            INSERT INTO formats (name) VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (name,),
        )
        return cur.fetchone()[0]

    def _get_or_create_artist(self, cur, name):
        cur.execute(
            """
            INSERT INTO artists (name, sort_name) VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET sort_name = EXCLUDED.sort_name
            RETURNING id
            """,
            (name, _sort_name(name)),
        )
        return cur.fetchone()[0]

    def _upsert_album(self, cur, data):
        cur.execute(
            """
            INSERT INTO albums (
                title, artist_id, format_id, year, is_compilation, disc_count,
                nas_path, label, catalog_number, original_year, disc_subtitle,
                rg_album_gain, rg_album_peak, content_hash, last_scraped
            ) VALUES (
                %(title)s, %(artist_id)s, %(format_id)s, %(year)s, %(is_compilation)s,
                %(disc_count)s, %(nas_path)s, %(label)s, %(catalog_number)s,
                %(original_year)s, %(disc_subtitle)s, %(rg_album_gain)s,
                %(rg_album_peak)s, %(content_hash)s, now()
            )
            ON CONFLICT (nas_path) DO UPDATE SET
                title          = EXCLUDED.title,
                artist_id      = EXCLUDED.artist_id,
                format_id      = EXCLUDED.format_id,
                year           = EXCLUDED.year,
                is_compilation = EXCLUDED.is_compilation,
                disc_count     = EXCLUDED.disc_count,
                label          = EXCLUDED.label,
                catalog_number = EXCLUDED.catalog_number,
                original_year  = EXCLUDED.original_year,
                disc_subtitle  = EXCLUDED.disc_subtitle,
                rg_album_gain  = EXCLUDED.rg_album_gain,
                rg_album_peak  = EXCLUDED.rg_album_peak,
                content_hash   = EXCLUDED.content_hash,
                last_scraped   = now()
            RETURNING id
            """,
            data,
        )
        return cur.fetchone()[0]

    def _upsert_genres(self, cur, album_id, genre_names):
        cur.execute("DELETE FROM album_genres WHERE album_id = %s", (album_id,))
        for name in genre_names:
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

    def _replace_tracks(self, cur, album_id, flacs, album_is_compilation):
        cur.execute("DELETE FROM tracks WHERE album_id = %s", (album_id,))
        for disc_num_from_dir, flac_path in flacs:
            try:
                self._insert_track(cur, album_id, disc_num_from_dir, flac_path, album_is_compilation)
            except Exception:
                log.exception("Skipping track %s", flac_path)

    def _insert_track(self, cur, album_id, disc_num_from_dir, flac_path, album_is_compilation):
        try:
            meta = FLAC(str(flac_path))
        except FLACNoHeaderError as e:
            raise ValueError(f"Cannot read FLAC header: {e}") from e

        title = _tag(meta, 'title') or flac_path.stem
        track_number = _tag_int(meta, 'tracknumber')
        track_count = _tag_int(meta, 'tracktotal') or _tag_int(meta, 'totaltracks')
        disc_number = _tag_int(meta, 'discnumber') or disc_num_from_dir
        is_compilation = (
            _tag(meta, 'compilation') in ('1', 'true', 'True', 'yes')
            or album_is_compilation
        )

        # For compilations, store the per-track artist; otherwise leave NULL
        # (implying same as album artist) to avoid duplicating data.
        artist_id = None
        if is_compilation:
            track_artist = _tag(meta, 'artist')
            if track_artist:
                artist_id = self._get_or_create_artist(cur, track_artist)

        # Prefer front cover (type 3); fall back to first picture available.
        artwork = artwork_mime = None
        if meta.pictures:
            pic = next((p for p in meta.pictures if p.type == 3), meta.pictures[0])
            artwork = pic.data
            artwork_mime = pic.mime

        cur.execute(
            """
            INSERT INTO tracks (
                album_id, artist_id, title,
                track_number, track_count, disc_number,
                duration_secs, file_path, is_compilation,
                bit_depth, sample_rate, channels,
                composer, comment, rg_track_gain, rg_track_peak,
                artwork, artwork_mime
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
            """,
            (
                album_id,
                artist_id,
                title,
                track_number,
                track_count,
                disc_number,
                meta.info.length,
                str(flac_path.relative_to(self._nas_root)),
                is_compilation,
                meta.info.bits_per_sample,
                meta.info.sample_rate,
                meta.info.channels,
                _tag(meta, 'composer'),
                _tag(meta, 'comment'),
                _tag_float(meta, 'replaygain_track_gain'),
                _tag_float(meta, 'replaygain_track_peak'),
                artwork,
                artwork_mime,
            ),
        )

    # --------------------------------------------------------- incremental helpers

    def _load_content_hashes(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT nas_path, content_hash FROM albums WHERE content_hash IS NOT NULL")
            return {row[0]: row[1] for row in cur.fetchall()}

    def _remove_deleted(self, found_paths):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id, nas_path FROM albums")
            to_delete = [row[0] for row in cur.fetchall() if row[1] not in found_paths]
        if to_delete:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM albums WHERE id = ANY(%s)", (to_delete,))
            self.conn.commit()
            log.info("Removed %d deleted albums.", len(to_delete))
