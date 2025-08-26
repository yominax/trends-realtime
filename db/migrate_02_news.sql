-- Articles presse FR
CREATE TABLE IF NOT EXISTS news_articles(
  id BIGSERIAL PRIMARY KEY,
  published_ts TIMESTAMPTZ NOT NULL,
  ts_ingest    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT, title TEXT, url TEXT, summary TEXT, kind TEXT
);
CREATE INDEX IF NOT EXISTS news_articles_published_idx ON news_articles(published_ts DESC);
CREATE INDEX IF NOT EXISTS news_articles_kind_idx ON news_articles(kind, published_ts DESC);

-- Spikes de mots-clés (fenêtre)
CREATE TABLE IF NOT EXISTS spikes_news(
  ts TIMESTAMPTZ NOT NULL,
  keyword TEXT NOT NULL,
  count_30m INT NOT NULL,
  score_norm DOUBLE PRECISION NOT NULL,
  PRIMARY KEY(ts, keyword)
);
