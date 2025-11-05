music-catalog

Read-only cataloger for large NAS-hosted music libraries (FLAC/DSF).
Optimized for fast initial indexing, change-only refreshes, and gentle NAS I/O.
No sidecars. No writes to the library. All state kept in SQLite.

Features

SQLite schema + migrations (mc migrate)

Threaded FAST pass: header probe with Mutagen (codec/bit-depth/sample-rate/channels/duration)
→ single DB writer, batched commits, progress ticker

CHANGE-ONLY pass: skips untouched albums via in-DB fingerprint (built from stat() only), marks adds/changes, flags deletions, optional debounce to avoid half-copied files

DEEP pass: tag/artwork read for items marked DIRTY_META

DB maintenance: soft clear (truncate) or hard reset (recreate file)

Inventory status: quick health snapshot of totals, dirty queue, errors, recent activity, albums/discs

Install
# clone & create venv
cd /home/dl/development
git clone <your repo url> music-catalog
cd music-catalog
python3 -m venv .venv
source .venv/bin/activate

# editable install
pip install -U pip
pip install -e .

# create config
mkdir -p /home/dl/development/.config/music-catalog
cp ./config-example.yaml /home/dl/development/.config/music-catalog/dev.yaml

# ensure DB path exists and is writable
sudo mkdir -p /var/lib/music-catalog/{state,reports}
sudo chown -R dl:dl /var/lib/music-catalog


Python 3.10+ recommended.

Configuration

/home/dl/development/.config/music-catalog/dev.yaml (example shipped as config-example.yaml):

env: dev
catalog:
  db_path: "/var/lib/music-catalog/catalog.db"
  state_dir: "/var/lib/music-catalog/state"
report:
  out_dir: "/var/lib/music-catalog/reports"
roots:
  - path: "/home/dl/drobos/hibiki/Media/Music/Lossless"
    tiers:
      - name: "DSD"
        dir: "DSD"
      - name: "FLAC_Redbook"
        dir: "FLAC 16-Bit CD"
      - name: "FLAC_HiRes"
        dir: "FLAC 24-Bit HiRes"
scan:
  concurrency: 4
  ignore_patterns: ["@eaDir", ".*", "Thumbs.db"]
  boxset_wrappers: ["Box Sets", "Boxsets"]
audits:
  enforce_tier_rules: true
  require_disctotal_when_multidisc: true
  replaygain_all_or_none: true
normalization:
  unicode: "NFC"
  casefold_locale: "en"


Only roots[].path and catalog.db_path are essential to start.

Commands
1) Schema
mc migrate --config /home/dl/development/.config/music-catalog/dev.yaml


Creates/updates tables and indexes. Safe to run anytime.

2) FAST (initial/full passes)

Threaded header probe (Mutagen) + single DB writer.

# recommended defaults for NAS
mc inventory fast \
  --config /home/dl/development/.config/music-catalog/dev.yaml \
  --workers 8 \
  --batch-size 1500


You’ll see periodic tick lines like:

[12:47:03] [FAST] processed ~2000 files… (21.4 files/sec)


Flags

--workers N (default 6): header-probe worker threads (I/O-bound; 6–8 is a good range)

--batch-size N (default 1000): rows per transaction commit

3) CHANGED (incremental maintenance)

Skips untouched albums using an in-DB fingerprint (no sidecars; stat() only).
Marks adds/updates DIRTY_META. Flags deletions via is_missing=1. Very NAS-friendly.

mc inventory changed \
  --config /home/dl/development/.config/music-catalog/dev.yaml \
  --debounce-sec 10


Flags

--debounce-sec N (default 5–10): ignore files modified in the last N seconds to avoid mid-copy/retag flapping

Example output:

[12:15:44] [CHANGED] processed ~200 albums…
CHANGED inventory albums=1234, touched_rows=5678, time=89.1s

4) DEEP (tags & artwork)

Reads full tags/artwork for items marked DIRTY_META (from FAST/CHANGED).

# unlimited (overnight)
mc inventory deep --config /home/dl/development/.config/music-catalog/dev.yaml

# or gentle chunks on NAS
mc inventory deep \
  --config /home/dl/development/.config/music-catalog/dev.yaml \
  --limit 500

5) DB maintenance
# soft clear: delete rows, keep schema
mc db clear --config /home/dl/development/.config/music-catalog/dev.yaml -y

# hard reset: delete DB file and recreate schema
mc db clear --config /home/dl/development/.config/music-catalog/dev.yaml --hard -y

6) Inventory status (NEW)

Quick health snapshot: dirty queue size, tagged count, errors, recent activity, albums/discs, last run.

mc inventory status \
  --config /home/dl/development/.config/music-catalog/dev.yaml \
  --since-minutes 60


Shows

Tracks: total, dirty (DIRTY_META/NEW/DEEP_PENDING), tagged, errors, missing, recent activity (within window)

Albums/Discs: counts and how many albums have stored fingerprints

Last run: last run_event id, timestamp, and command

Quick breakdowns: by status, and top (codec, bit_depth, sample_rate) combos

How it works

FAST: Walks library → Mutagen opens each file (header only) → upserts album + track → marks DIRTY_META.

CHANGED: Groups by album → computes fingerprint from relative_path|size|mtime_ns (in memory) → if unchanged, marks tracks seen and skips; if changed, updates per-file rows and flags deletions (is_missing=1). Fingerprint is stored in SQLite only.

DEEP: For DIRTY_META tracks, loads tags/artwork and updates tag_digest, sets status TAGGED.

Guarantees

Never writes to your music shares (no sidecars, no manifests).

Entirely read-only on NAS; all writes are to SQLite.

Performance guidance

On typical SMB/NFS NAS (Drobo-class), expect:

FAST @ 8 workers: ~20 files/sec (header reads dominate I/O wait)

CHANGED: significantly faster (mostly directory stats and small updates)

If NAS struggles: reduce FAST --workers to 6; keep --batch-size ~1000–2000.

Keep DEEP limited in chunks on busy boxes.

Useful shell checks

# live count & updates
watch -n 2 'sqlite3 /var/lib/music-catalog/catalog.db "select count(*) from track;"'

# dirty queue size
sqlite3 /var/lib/music-catalog/catalog.db \
 'select count(*) from track where status in ("DIRTY_META","NEW","DEEP_PENDING");'

Troubleshooting

CLI flags not recognized: ensure venv is active and you’re using the venv’s mc:

source /home/dl/development/music-catalog/.venv/bin/activate
which mc
python -m music_catalog.cli --help


No visible progress in DB during FAST: large --batch-size delays commits; use 1000–2000 while monitoring.

Historical disc-id overflow (old builds): fixed by using auto-increment disc.id and a unique (album_id, disc_number).

Roadmap

Stats/report commands (counts by codec/bit-depth/rate; missing art)

Optional head/tail verification for suspicious retags (read-only, ~128 KiB/sample)

Album/disc rollups and tier compliance audits

License

MIT