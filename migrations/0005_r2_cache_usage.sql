-- Running total of bytes in the R2 cache, so the Worker can stop caching before the free 10 GB is exceeded.
-- One row (id = 1). Cache writes add to total_bytes; the cron job replaces it with a recount of the bucket.
CREATE TABLE IF NOT EXISTS r2_cache_usage (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  total_bytes INTEGER NOT NULL DEFAULT 0,
  object_count INTEGER NOT NULL DEFAULT 0,
  recounted_at TEXT
);
INSERT OR IGNORE INTO r2_cache_usage (id) VALUES (1);
