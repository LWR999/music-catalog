-- Migration 003: track count per disc
-- disc x of y: tracks.disc_number of albums.disc_count (already present)
-- track x of y: tracks.track_number of tracks.track_count (added here)

ALTER TABLE tracks ADD COLUMN IF NOT EXISTS track_count SMALLINT;
