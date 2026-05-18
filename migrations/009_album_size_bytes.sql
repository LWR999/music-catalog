-- Migration 009: Album size on disk

ALTER TABLE albums ADD COLUMN IF NOT EXISTS size_bytes BIGINT;
