-- Migration 001: initial schema

CREATE TABLE IF NOT EXISTS formats (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    CONSTRAINT uq_formats_name UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS artists (
    id        SERIAL PRIMARY KEY,
    name      VARCHAR(500) NOT NULL,
    sort_name VARCHAR(500) NOT NULL,
    CONSTRAINT uq_artists_name UNIQUE (name)
);

CREATE INDEX IF NOT EXISTS idx_artists_sort_name ON artists (sort_name);

CREATE TABLE IF NOT EXISTS albums (
    id            SERIAL PRIMARY KEY,
    title         VARCHAR(1000) NOT NULL,
    artist_id     INTEGER      NOT NULL REFERENCES artists(id),
    format_id     INTEGER      NOT NULL REFERENCES formats(id),
    year          SMALLINT,
    is_compilation BOOLEAN     NOT NULL DEFAULT FALSE,
    disc_count    SMALLINT     NOT NULL DEFAULT 1,
    nas_path      VARCHAR(2000) NOT NULL,
    last_scraped  TIMESTAMPTZ,
    CONSTRAINT uq_albums_nas_path UNIQUE (nas_path)
);

CREATE INDEX IF NOT EXISTS idx_albums_artist_id ON albums (artist_id);
CREATE INDEX IF NOT EXISTS idx_albums_year      ON albums (year);

CREATE TABLE IF NOT EXISTS genres (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    CONSTRAINT uq_genres_name UNIQUE (name)
);

CREATE INDEX IF NOT EXISTS idx_genres_name ON genres (name);

CREATE TABLE IF NOT EXISTS album_genres (
    album_id INTEGER NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (album_id, genre_id)
);

CREATE INDEX IF NOT EXISTS idx_album_genres_genre_id ON album_genres (genre_id);

CREATE TABLE IF NOT EXISTS tracks (
    id             SERIAL PRIMARY KEY,
    album_id       INTEGER       NOT NULL REFERENCES albums(id) ON DELETE CASCADE,
    -- NULL means track artist is the same as album artist
    artist_id      INTEGER       REFERENCES artists(id),
    title          VARCHAR(1000) NOT NULL,
    track_number   SMALLINT,
    disc_number    SMALLINT      NOT NULL DEFAULT 1,
    duration_secs  REAL,
    file_path      VARCHAR(2000) NOT NULL,
    is_compilation BOOLEAN       NOT NULL DEFAULT FALSE,
    bit_depth      SMALLINT,
    sample_rate    INTEGER,
    CONSTRAINT uq_tracks_file_path UNIQUE (file_path)
);

CREATE INDEX IF NOT EXISTS idx_tracks_album_id  ON tracks (album_id);
CREATE INDEX IF NOT EXISTS idx_tracks_artist_id ON tracks (artist_id);
