-- Migration 008: Last.fm sync columns

ALTER TABLE albums ADD COLUMN IF NOT EXISTS lastfm_playcount   INTEGER;
ALTER TABLE albums ADD COLUMN IF NOT EXISTS lastfm_last_played TIMESTAMPTZ;
ALTER TABLE albums ADD COLUMN IF NOT EXISTS lastfm_synced_at   TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_albums_lastfm_synced_at ON albums (lastfm_synced_at);

CREATE TABLE IF NOT EXISTS lastfm_tags (
    album_id INTEGER  NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    tag      VARCHAR(200) NOT NULL,
    rank     SMALLINT NOT NULL CHECK (rank BETWEEN 1 AND 5),
    PRIMARY KEY (album_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_lastfm_tags_tag ON lastfm_tags (tag);
