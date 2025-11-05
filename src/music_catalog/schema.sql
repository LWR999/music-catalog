PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER NOT NULL
);

INSERT INTO schema_version(version) SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM schema_version);

CREATE TABLE IF NOT EXISTS run_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT NOT NULL,
  command TEXT NOT NULL,
  config_hash TEXT,
  items_processed INTEGER DEFAULT 0,
  duration_ms INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS album (
  id INTEGER PRIMARY KEY,
  folder_path TEXT UNIQUE NOT NULL,
  folder_artist TEXT,
  folder_title TEXT,
  tier_declared TEXT,
  format_observed TEXT,
  bit_depth_set TEXT,        -- e.g. "16" or "24" or "16,24"
  sample_rates_set TEXT,     -- e.g. "44100,48000"
  dsd_rates_set TEXT,        -- e.g. "DSD64,DSD128"
  disc_count INTEGER,
  track_count INTEGER,
  status TEXT,
  updated_at TEXT,
  album_fingerprint TEXT,
  item_count INTEGER
);

CREATE TABLE IF NOT EXISTS disc (
  id INTEGER PRIMARY KEY,
  album_id INTEGER NOT NULL REFERENCES album(id) ON DELETE CASCADE,
  disc_number INTEGER,
  disc_title TEXT,
  path TEXT,
  track_count INTEGER
);

CREATE TABLE IF NOT EXISTS track (
  id INTEGER PRIMARY KEY,
  path TEXT UNIQUE NOT NULL,
  album_id INTEGER REFERENCES album(id) ON DELETE SET NULL,
  disc_id INTEGER REFERENCES disc(id) ON DELETE SET NULL,
  size_bytes INTEGER,
  mtime_ns INTEGER,
  codec TEXT,
  bit_depth INTEGER,
  sample_rate_hz INTEGER,
  channels INTEGER,
  duration_sec REAL,
  tag_digest TEXT,          -- hash of normalized tags (for change detection)
  status TEXT,              -- NEW, DIRTY_META, DEEP_PENDING, TAGGED, ERROR
  last_error TEXT,
  last_seen TEXT,
  seen_run_id INTEGER,
  is_missing INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_track_album ON track(album_id);
CREATE INDEX IF NOT EXISTS idx_track_mtime ON track(mtime_ns);
CREATE UNIQUE INDEX IF NOT EXISTS ux_disc_album_discno ON disc(album_id, disc_number);
CREATE INDEX IF NOT EXISTS idx_track_album_seen ON track(album_id, seen_run_id);
CREATE INDEX IF NOT EXISTS idx_track_missing ON track(is_missing);