# Music Catalog Project

## Overview
A PostgreSQL-backed music catalog database built by scraping FLAC file metadata 
from a NAS. Designed to be queried by web applications. Written in Python.

## Environments
- Dev: downloadserver.local (user: dl, db: music_catalog)
- Prod: musicserver.local (user: music, db: music_catalog)

## NAS Configuration (set in .env, never hardcode)
- Dev NAS path: ~/drobos/hibiki/Media/Music/Lossless/
- Prod NAS path: ~/drobos/hibiki/Media/Music/Lossless/

## NAS Taxonomy
- Top level: format folders (e.g. 'FLAC 16-Bit CD', 'FLAC 24-Bit HiRes', 'DSD')
- Next level: '<artist> - <album title>' folders
- Track files: FLAC files directly in album folder
- Multi-disc: subfolders named 'DISC x' or 'CDx' containing FLACs
- Boxsets: occasional overarching collection folder above album folders
- Compilations: 'Various Artists' as artist; tracks have compilation flag set
- Ignore non-FLAC files and artwork subfolders

## Database
- PostgreSQL
- Dev credentials in .env (never commit)
- Schema should support: albums, artists, tracks, genres, formats
- Must support artist index, genre index, album queries
- Optimised for read-heavy web app queries

## Python
- Use virtual environment (venv)
- Use mutagen for FLAC metadata extraction
- Use psycopg2 for PostgreSQL
- Use python-dotenv for config
- Dependency managed via requirements.txt

## Scraping Behaviour
1. Full scrape: build complete catalog from scratch
2. Incremental scrape: detect adds/deletes/edits since last run (for daily cron)
3. Incremental and scheduled scrape are the same operation
- Track last_scraped timestamp per album
- Cron runs daily on musicserver.local

## Git
- Remote: git@github.com:LWR999/LWR999.git
- Never commit .env files
- Always commit requirements.txt
