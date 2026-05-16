-- Migration 007: MusicBrainz enrichment columns

ALTER TABLE albums ADD COLUMN IF NOT EXISTS mb_release_id   UUID;
ALTER TABLE albums ADD COLUMN IF NOT EXISTS release_year     SMALLINT;
ALTER TABLE albums ADD COLUMN IF NOT EXISTS record_label     VARCHAR(500);
ALTER TABLE albums ADD COLUMN IF NOT EXISTS release_country  CHAR(2);
ALTER TABLE albums ADD COLUMN IF NOT EXISTS mb_confidence    VARCHAR(10)
    CHECK (mb_confidence IN ('exact', 'fuzzy', 'none'));
ALTER TABLE albums ADD COLUMN IF NOT EXISTS mb_enriched_at   TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_albums_mb_release_id
    ON albums (mb_release_id) WHERE mb_release_id IS NOT NULL;
