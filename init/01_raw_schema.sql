CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.gh_events (
    event_id    BIGINT       NOT NULL,
    event_type  TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL,
    source_file TEXT         NOT NULL,
    loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload     JSONB        NOT NULL,
    PRIMARY KEY (event_id, source_file)
);