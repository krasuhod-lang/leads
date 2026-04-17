CREATE TABLE IF NOT EXISTS daily_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    source_id TEXT,
    source_name TEXT,
    offer_name TEXT NOT NULL DEFAULT 'Unknown',
    sub1 TEXT NOT NULL DEFAULT '',
    clicks INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    approved INTEGER DEFAULT 0,
    revenue REAL DEFAULT 0,
    UNIQUE(date, source_id, offer_name, sub1)
);