CREATE TABLE IF NOT EXISTS spikes_entities(
  ts TIMESTAMPTZ NOT NULL,
  phrase TEXT NOT NULL,
  kind TEXT NOT NULL,          -- 'news' | 'wiki' | 'mix'
  mentions INT NOT NULL,       -- nombre d'occurrences (titres + pages)
  sources INT NOT NULL,        -- nombre de sources distinctes où la phrase apparaît
  score DOUBLE PRECISION NOT NULL,
  PRIMARY KEY(ts, phrase, kind)
);
CREATE INDEX IF NOT EXISTS spikes_entities_ts_idx ON spikes_entities(ts DESC);
