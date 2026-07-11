-- Migration 011: Track when genre normalisation was last run per album

ALTER TABLE albums ADD COLUMN IF NOT EXISTS genre_normalised_at TIMESTAMPTZ;
