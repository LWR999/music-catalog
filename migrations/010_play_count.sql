-- Migration 010: Album play count from Last.fm bulk sync

ALTER TABLE albums ADD COLUMN IF NOT EXISTS play_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_albums_play_count ON albums (play_count);
