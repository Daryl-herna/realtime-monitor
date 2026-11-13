"""采集器基类：统一限速、指数退避、健康上报、优雅停止。"""
from __future__ import annotations

import json
import threading
import time
import traceback
import urllib.error
import urllib.request
import ssl

from ..bus import Event, EventBus, make_event
from ..config import Config, now_ms
from ..db import Database

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

_SSL_CTX = ssl.create_default_context()


class HttpError(Exception):
    pass


def http_get(url: str, referer: str | None = None, timeout: float = 15.0,
             encoding: str = "utf-8") -> str:
    """标准库 GET。带 UA / Referer，跟随重定向，超时可控。"""
    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Accept-Encoding": "identity",
        "Connection": "close",
    }
    if referer:
        headers["Referer"] = referer
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
            raw = resp.read()
            return raw.decode(encoding, "replace")
    except urllib.error.HTTPError as e:
        raise HttpError(f"HTTP {e.code} {url}") from e
    except Exception as e:
        raise HttpError(f"{type(e).__name__}: {e}") from e


class BaseCollector:
    """子类实现 fetch() -> list[Event]。基类负责调度、退避与健康度。"""

    name = "base"
    referer: str | None = None

    def __init__(self, cfg: Config, db: Database, bus: EventBus):
        self.cfg = cfg
        self.db = db
        self.bus = bus
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.interval = float(cfg.get("sources", self.name, "interval_s", default=60))
        self.min_interval = float(cfg.get("sources", self.name, "min_interval_s", default=5))
        self._failures = 0
        self._last_success: int | None = None
        self._last_error: str | None = None
        self._last_latency_ms: int | None = None
        self._count = 0

    # ---- 子类实现 ----
    def fetch(self) -> list[Event]:
        raise NotImplementedError

    # ---- 生命周期 ----
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"collector-{self.name}", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.get("sources", self.name, "enabled", default=True))

    def _run(self):
        # 启动抖动，避免 7 个采集器同时打同一时刻
        time.sleep(0.2 + (hash(self.name) % 10) * 0.1)
        while not self._stop.is_set():
            if not self.enabled:
                self._stop.wait(2.0)
                continue
            t0 = time.time()
            self._last_attempt = now_ms()
            try:
                events = self.fetch()
                # 列表型接口天然是「新→旧」顺序。若原样入总线，第一条会把该分区
                # 水位线推到最新，后续同批历史条目全被判成迟到丢弃。
                # 按 event_time 升序发布即可让整批数据正常通过。
                events.sort(key=lambda e: (e.event_time, e.subject))
                # 无论事件是否被去重，都把本轮快照交给看板层，
                # 保证「价格没变」的标的也能显示最新值。
                if events and hasattr(self, "on_snapshot"):
                    snap = {}
                    for e in events:
                        if e.kind == "quote" and e.payload.get("price") is not None:
                            snap[e.subject] = e.payload
                    if snap:
                        self.on_snapshot(snap)
                n = self.bus.publish_many(events)
                self._count += n
                self._failures = 0
                self._last_success = now_ms()
                self._last_error = None
                self._last_latency_ms = int((time.time() - t0) * 1000)
                self._save_health("OK")
            except Exception as exc:
                self._failures += 1
                self._last_error = f"{type(exc).__name__}: {exc}"[:300]
                self._save_health("ERROR")
                traceback.print_exc()
            # 退避：连续失败时拉长间隔，最多 8 倍
            delay = self.interval * min(8, 2 ** max(0, self._failures - 1))
            self._stop.wait(delay)

    def _save_health(self, status: str):
        self.db.execute(
            "INSERT INTO source_health(source,status,last_success,last_attempt,last_error,"
            "consecutive_failures,latency_ms,event_count) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(source) DO UPDATE SET status=excluded.status,"
            "last_success=COALESCE(excluded.last_success,source_health.last_success),"
            "last_attempt=excluded.last_attempt,last_error=excluded.last_error,"
            "consecutive_failures=excluded.consecutive_failures,latency_ms=excluded.latency_ms,"
            "event_count=excluded.event_count",
            (self.name, status, self._last_success, getattr(self, "_last_attempt", now_ms()),
             self._last_error, self._failures, self._last_latency_ms, self._count))

    def health(self) -> dict:
        return {
            "source": self.name,
            "enabled": self.enabled,
            "status": "OK" if self._failures == 0 and self._last_success else ("ERROR" if self._failures else "PENDING"),
            "last_success": self._last_success,
            "last_error": self._last_error,
            "consecutive_failures": self._failures,
            "latency_ms": self._last_latency_ms,
            "event_count": self._count,
            "interval_s": self.interval,
        }


def parse_int(v, default=0):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return default


def parse_float(v, default=None):
    try:
        f = float(str(v).strip())
        return f
    except (TypeError, ValueError):
        return default


def local_date_ms(date_str: str, time_str: str = "00:00:00") -> int:
    """把 '2026-11-12' + '11:34:18' 转成本地时区毫秒时间戳。"""
    from datetime import datetime
    try:
        dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        dt = datetime.now()
    return int(dt.timestamp() * 1000)