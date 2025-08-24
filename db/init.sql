-- DB schema
CREATE TABLE IF NOT EXISTS wiki_rc(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  page TEXT, user_name TEXT, comment TEXT, delta INT, url TEXT
);
CREATE INDEX IF NOT EXISTS wiki_rc_ts_idx ON wiki_rc(ts);

CREATE TABLE IF NOT EXISTS hn_posts(
  id BIGINT PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  title TEXT, by_user TEXT, score INT, descendants INT, url TEXT
);
CREATE INDEX IF NOT EXISTS hn_posts_ts_idx ON hn_posts(ts);

CREATE TABLE IF NOT EXISTS hn_scores(
  id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL,
  score INT, comments INT,
  PRIMARY KEY(id, ts)
);

CREATE TABLE IF NOT EXISTS spikes_wiki(
  ts TIMESTAMPTZ NOT NULL, page TEXT NOT NULL,
  edits_15m INT NOT NULL, score_norm DOUBLE PRECISION NOT NULL,
  PRIMARY KEY(ts, page)
);

CREATE TABLE IF NOT EXISTS spikes_hn(
  ts TIMESTAMPTZ NOT NULL, id BIGINT NOT NULL,
  title TEXT NOT NULL, velocity DOUBLE PRECISION NOT NULL, velocity_norm DOUBLE PRECISION NOT NULL,
  score INT, comments INT, url TEXT,
  PRIMARY KEY(ts, id)
);
