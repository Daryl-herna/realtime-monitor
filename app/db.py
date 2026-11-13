"""SQLite 存储层。WAL 模式；每线程独立连接；事件表即事件总线日志。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- ===== 事件总线日志（原始事件落盘，有序、可重放） =====
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT    NOT NULL UNIQUE,
    source      TEXT    NOT NULL,
    kind        TEXT    NOT NULL,          -- quote | baseline | news | notice
    subject     TEXT,
    event_time  INTEGER NOT NULL,          -- 数据源标注的时间（毫秒）
    ingest_time INTEGER NOT NULL,          -- 本地入库时间（毫秒）
    dedup_key   TEXT,
    payload     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_time    ON events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_subject ON events(subject, event_time);
CREATE INDEX IF NOT EXISTS idx_events_source  ON events(source, seq);

-- ===== 消费位点（ACK 落盘，重启后从位点续跑） =====
CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer   TEXT PRIMARY KEY,
    last_seq   INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);

-- ===== 监控任务 =====
CREATE TABLE IF NOT EXISTS tasks (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    type       TEXT NOT NULL,              -- price | info
    enabled    INTEGER NOT NULL DEFAULT 1,
    config     TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- ===== 告警 =====
CREATE TABLE IF NOT EXISTS alerts (
    alert_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id    TEXT NOT NULL UNIQUE,
    fingerprint TEXT NOT NULL,
    task_id     TEXT,
    rule_id     TEXT,
    subject     TEXT,
    severity    TEXT NOT NULL,             -- P1 | P2 | P3
    status      TEXT NOT NULL,             -- firing | resolved | digest
    title       TEXT NOT NULL,
    detail      TEXT,
    event_time  INTEGER,
    created_at  INTEGER NOT NULL,
    resolved_at INTEGER,
    channel     TEXT,                      -- sse | poll-fallback
    payload     TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_fp   ON alerts(fingerprint, created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(created_at);

-- ===== 抑制计数 =====
CREATE TABLE IF NOT EXISTS suppressions (
    fingerprint TEXT PRIMARY KEY,
    task_id     TEXT,
    count       INTEGER NOT NULL DEFAULT 0,
    last_at     INTEGER,
    reason      TEXT
);

-- ===== 数据源健康 =====
CREATE TABLE IF NOT EXISTS source_health (
    source               TEXT PRIMARY KEY,
    status               TEXT,
    last_success         INTEGER,
    last_attempt         INTEGER,
    last_error           TEXT,
    consecutive_failures INTEGER DEFAULT 0,
    latency_ms           INTEGER,
    event_count          INTEGER DEFAULT 0
);

-- ===== 运行指标（KV） =====
CREATE TABLE IF NOT EXISTS metrics (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at INTEGER
);

-- ===== 回放记录 =====
CREATE TABLE IF NOT EXISTS replays (
    id         TEXT PRIMARY KEY,
    task_id    TEXT,
    from_ts    INTEGER,
    to_ts      INTEGER,
    event_cnt  INTEGER,
    old_hits   INTEGER,
    new_hits   INTEGER,
    created_at INTEGER,
    detail     TEXT
);
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._local = threading.local()
        self._init_lock = threading.Lock()
        with self._init_lock:
            conn = self._conn()
            conn.executescript(SCHEMA)
            conn.commit()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None,
                                   check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    # ---------- 通用 ----------
    def execute(self, sql, params=()):
        return self._conn().execute(sql, params)

    def executemany(self, sql, seq):
        return self._conn().executemany(sql, seq)

    def query(self, sql, params=()):
        return [dict(r) for r in self._conn().execute(sql, params).fetchall()]

    def query_one(self, sql, params=()):
        r = self._conn().execute(sql, params).fetchone()
        return dict(r) if r else None

    def commit(self):
        self._conn().commit()

    # ---------- 指标 ----------
    def set_metric(self, key: str, value, ts: int):
        self.execute(
            "INSERT INTO metrics(key,value,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, json.dumps(value, ensure_ascii=False), ts),
        )

    def get_metrics(self) -> dict:
        out = {}
        for row in self.query("SELECT key,value FROM metrics"):
            try:
                out[row["key"]] = json.loads(row["value"])
            except (TypeError, ValueError):
                out[row["key"]] = row["value"]
        return out
