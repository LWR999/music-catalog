#!/usr/bin/env python3
"""One-off: write canonical genre tags to playlist FLACs under ~/drobos/hibiki/Portable/Playlists/"""

import logging
from pathlib import Path

import mutagen.flac
from mutagen.flac import FLAC

log = logging.getLogger(__name__)

PLAYLISTS_ROOT = Path.home() / 'drobos/hibiki/Portable/Playlists'

PLAYLIST_GENRES = {
    '_Coulou - Funky Jazz House No. 1':                         ['Electronic', 'Funk', 'House', 'Jazz'],
    '_Crate Guest - Fusion Jazz by Santiago Barros':            ['Jazz', 'Jazz Fusion'],
    '_Daily Mix - Flight Facilities':                           ['Electronic', 'House'],
    '_Janes JazzFusion Playlist':                               ['Jazz', 'Jazz Fusion'],
    "_Leon - 90's Favourites":                                  ['R&B', 'Soul'],
    '_MAJ Presents - Coco Maria Afro Cuban Latin Jazz Grooves': ['Jazz', 'Latin', 'Latin Jazz'],
    '_Soulpersona and Bryan Corbett - Magentica Influences':    ['Jazz', 'Soul'],
    '_Soulpersona and Carl Hudson - Magic Bullet Musical Influences': ['Funk', 'Soul'],
    '_Soulpersona and Carl Hudson - Space Disco Time Influences':     ['Disco', 'Soul'],
    '_Soulpersona - Jazz Funk Classics with Colin Curtis':      ['Funk', 'Jazz', 'Jazz Fusion'],
    '_Stellasonics - Yacht Rock 81':                            ['Pop', 'Rock'],
    '_Summer House 2026':                                       ['Electronic', 'House'],
}


def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    for folder_name, genres in PLAYLIST_GENRES.items():
        folder = PLAYLISTS_ROOT / folder_name
        if not folder.exists():
            log.warning("NOT FOUND: %s", folder)
            continue

        flacs = sorted(f for f in folder.rglob('*.flac') if not f.name.startswith('._'))
        if not flacs:
            log.warning("NO FLACs:  %s", folder_name)
            continue

        updated = 0
        for flac_path in flacs:
            try:
                f = FLAC(str(flac_path))
                f['GENRE'] = genres
                f.save()
                updated += 1
            except mutagen.flac.error as e:
                log.warning("Cannot write %s: %s", flac_path, e)

        log.info("%-60s  %d FLACs → %s", folder_name[:60], updated, genres)


if __name__ == '__main__':
    main()
