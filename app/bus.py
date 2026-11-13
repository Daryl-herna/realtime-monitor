"""事件总线：SQLite 日志（有序 + 可重放）+ 进程内队列分发 + 位点 ACK。

设计取舍
--------
* 有序：events.seq 自增，天然全序。
* 可重放：按 event_time 区间查询 events 表即可回放，无需额外组件。
* 消费可 ACK：consumer_offsets 记录每个消费者的 last_seq，重启后从位点续跑。
* 分发：写入时同时 put 进内存队列，消费者拿不到才回落到轮询 DB（保证不丢）。
"""
from __future__ import annotations

import hashlib
import json
import queue
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Iterable

from .config import now_ms
from .db import Database


@dataclass
class Event:
    event_id: str
    source: str
    kind: str
    subject: str
    event_time: int
    ingest_time: int
    payload: dict
    dedup_key: str = ""
    seq: int = 0

    def to_row(self):
        return (self.event_id, self.source, self.kind, self.subject, self.event_time,
                self.ingest_time, self.dedup_key, json.dumps(self.payload, ensure_ascii=False))

    @staticmethod
    def from_row(row: dict) -> "Event":
        return Event(
            event_id=row["event_id"], source=row["source"], kind=row["kind"],
            subject=row["subject"] or "", event_time=row["event_time"],
            ingest_time=row["ingest_time"], dedup_key=row["dedup_key"] or "",
            payload=json.loads(row["payload"]), seq=row["seq"],
        )


def make_event(source: str, kind: str, subject: str, event_time: int,
               payload: dict, dedup_key: str = "") -> Event:
    """构造事件；event_id 由 (source, kind, subject, event_time, dedup_key) 决定，天然幂等。

    兜底：event_time 不允许晚于当前时间。数据源的时间字段经常只有日期粒度
    （如公告的 00:00:00 表示批次而非时刻），漏到总线里会让分区水位线跳到未来，
    把后续真实数据全部判成迟到——这是静默丢数据，比报错更危险。
    """
    event_time = int(event_time)
    _now = now_ms()
    if event_time > _now:
        event_time = _now
    # 幂等键：有 dedup_key 时以它为准（同一数据源的同一快照恒等），
    # 否则退回 event_time。
    # 不能用 event_time 参与哈希：同一份数据经不同采集器/不同解析路径
    # 可能算出相差几毫秒或几十秒的时间，导致重复入库、重复告警。
    if dedup_key:
        raw = f"{source}|{kind}|{subject}|{dedup_key}"
    else:
        raw = f"{source}|{kind}|{subject}|{event_time}"
    eid = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return Event(event_id=eid, source=source, kind=kind, subject=subject,
                 event_time=int(event_time), ingest_time=now_ms(),
                 payload=payload, dedup_key=dedup_key)


class EventBus:
    def __init__(self, db: Database, poll_interval_ms: int = 100, batch_size: int = 200):
        self.db = db
        self.poll_interval = poll_interval_ms / 1000.0
        self.batch_size = batch_size
        self._q: "queue.Queue[Event]" = queue.Queue(maxsize=20000)
        self._lock = threading.Lock()
        self._subs: list[Callable[[Event], None]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_published_seq = self._max_seq()
        self.stats = {"published": 0, "dropped_dup": 0, "queue_overflow": 0}

    # ---------- 写入 ----------
    def _max_seq(self) -> int:
        row = self.db.query_one("SELECT COALESCE(MAX(seq),0) AS s FROM events")
        return int(row["s"]) if row else 0

    def publish(self, ev: Event) -> bool:
        """落盘 + 入队。重复 event_id 静默丢弃（幂等）。"""
        try:
            cur = self.db.execute(
                "INSERT INTO events(event_id,source,kind,subject,event_time,ingest_time,dedup_key,payload) "
                "VALUES(?,?,?,?,?,?,?,?)", ev.to_row())
            ev.seq = int(cur.lastrowid)
        except Exception as exc:  # sqlite3.IntegrityError -> 重复
            if "UNIQUE" in str(exc).upper():
                self.stats["dropped_dup"] += 1
                return False
            raise
        self.stats["published"] += 1
        with self._lock:
            self._last_published_seq = max(self._last_published_seq, ev.seq)
        try:
            self._q.put_nowait(ev)
        except queue.Full:
            # 队列满不丢数据：事件已在库里，消费者轮询会补上
            self.stats["queue_overflow"] += 1
        return True

    def publish_many(self, evs: Iterable[Event]) -> int:
        n = 0
        for ev in evs:
            if self.publish(ev):
                n += 1
        return n

    # ---------- 订阅 ----------
    def subscribe(self, fn: Callable[[Event], None]):
        with self._lock:
            self._subs.append(fn)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="event-bus", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _dispatch(self, ev: Event):
        with self._lock:
            subs = list(self._subs)
        for fn in subs:
            try:
                fn(ev)
            except Exception:
                # 单个消费者异常不得拖垮总线
                import traceback
                traceback.print_exc()

    def _run(self):
        """先消费内存队列；空闲时回落到 DB 轮询，保证重启/溢出后不丢事件。"""
        while not self._stop.is_set():
            drained = 0
            while drained < self.batch_size:
                try:
                    ev = self._q.get_nowait()
                except queue.Empty:
                    break
                self._dispatch(ev)
                drained += 1
            if drained == 0:
                rows = self.db.query(
                    "SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT ?",
                    (self._last_published_seq, self.batch_size))
                for row in rows:
                    ev = Event.from_row(row)
                    self._last_published_seq = ev.seq
                    self._dispatch(ev)
                if rows:
                    continue
                self._stop.wait(self.poll_interval)

    # ---------- 消费位点 ----------
    def get_offset(self, consumer: str) -> int:
        row = self.db.query_one(
            "SELECT last_seq FROM consumer_offsets WHERE consumer=?", (consumer,))
        return int(row["last_seq"]) if row else 0

    def ack(self, consumer: str, seq: int):
        self.db.execute(
            "INSERT INTO consumer_offsets(consumer,last_seq,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(consumer) DO UPDATE SET last_seq=MAX(last_seq,excluded.last_seq), "
            "updated_at=excluded.updated_at",
            (consumer, seq, now_ms()))

    # ---------- 回放 ----------
    def replay(self, from_ts: int, to_ts: int, kinds: list[str] | None = None,
               subjects: list[str] | None = None, limit: int = 200000) -> list[Event]:
        sql = "SELECT * FROM events WHERE event_time BETWEEN ? AND ?"
        params: list[Any] = [from_ts, to_ts]
        if kinds:
            sql += " AND kind IN (%s)" % ",".join("?" * len(kinds))
            params += kinds
        if subjects:
            sql += " AND subject IN (%s)" % ",".join("?" * len(subjects))
            params += subjects
        sql += " ORDER BY event_time ASC, seq ASC LIMIT ?"
        params.append(limit)
        return [Event.from_row(r) for r in self.db.query(sql, tuple(params))]

    def stats_snapshot(self) -> dict:
        total = self.db.query_one("SELECT COUNT(*) AS c FROM events")
        latest = self.db.query_one("SELECT event_time, ingest_time, seq FROM events ORDER BY seq DESC LIMIT 1")
        oldest = self.db.query_one("SELECT event_time FROM events ORDER BY seq ASC LIMIT 1")
        return {
            "total_events": int(total["c"]) if total else 0,
            "last_event_time": latest["event_time"] if latest else None,
            "last_ingest_time": latest["ingest_time"] if latest else None,
            "last_seq": latest["seq"] if latest else 0,
            "oldest_event_time": oldest["event_time"] if oldest else None,
            **self.stats,
        }