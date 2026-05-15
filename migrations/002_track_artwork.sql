-- Migration 002: per-track embedded artwork

ALTER TABLE tracks ADD COLUMN IF NOT EXISTS artwork BYTEA;
ALTER TABLE tracks ADD COLUMN IF NOT EXISTS artwork_mime VARCHAR(50);
