-- Migration 005: content hash for reliable incremental scrape detection
-- Replaces mtime-based change detection, which is unreliable on NAS/Drobo.
-- Hash is computed from sorted (relative_path, file_size) of all FLACs in the album dir.

ALTER TABLE albums ADD COLUMN IF NOT EXISTS content_hash VARCHAR(32);
