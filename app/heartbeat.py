"""自监控：独立心跳守卫。

核心信条：没消息 ≠ 没问题。
- 采集器「很久没成功」本身就是告警，而不是「没有数据可显示」。
- 计算链路无进展（last_process_ts 停滞）同样告警。
- 守卫自身有心跳，看板能看出守卫是否还活着。
"""
from __future__ import annotations

import threading
import time

from .config import Config, now_ms
from .db import Database

# 数据源健康状态
OK = "OK"
STALE = "STALE"        # 超过 interval * stale_factor 没成功
DOWN = "DOWN"          # 超过 interval * down_factor 没成功
DISABLED = "DISABLED"
PENDING = "PENDING"    # 刚启动还没成功过


class Heartbeat:
    def __init__(self, cfg: Config, db: Database, collectors: dict, pipeline):
        self.cfg = cfg
        self.db = db
        self.collectors = collectors      # name -> BaseCollector
        self.pipeline = pipeline
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.started_at = now_ms()
        self.last_tick = now_ms()
        self.tick_count = 0
        self.alive = True
        self._state: dict[str, str] = {}
        self._events: list[dict] = []     # 状态变化流水

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        tick = float(self.cfg.get("heartbeat", "tick_s", default=5))
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                import traceback; traceback.print_exc()
            self._stop.wait(tick)

    def tick(self):
        self.last_tick = now_ms()
        self.tick_count += 1
        stale_f = float(self.cfg.get("heartbeat", "stale_factor", default=3.0))
        down_f = float(self.cfg.get("heartbeat", "down_factor", default=8.0))
        now = self.last_tick
        changes = []

        for name, c in self.collectors.items():
            if not c.enabled:
                state = DISABLED
            else:
                last = c._last_success
                if last is None:
                    state = PENDING
                else:
                    age = now - last
                    if age > c.interval * 1000 * down_f:
                        state = DOWN
                    elif age > c.interval * 1000 * stale_f:
                        state = STALE
                    else:
                        state = OK
            prev = self._state.get(name)
            if prev != state:
                self._state[name] = state
                changes.append({"source": name, "from": prev, "to": state, "at": now})
                self._events.append({"source": name, "from": prev, "to": state, "at": now})
                if len(self._events) > 500:
                    self._events = self._events[-500:]
            self.db.execute(
                "INSERT INTO source_health(source,status,last_success,last_attempt,last_error,"
                "consecutive_failures,latency_ms,event_count) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(source) DO UPDATE SET status=excluded.status,"
                "last_success=excluded.last_success,last_attempt=excluded.last_attempt,"
                "last_error=excluded.last_error,consecutive_failures=excluded.consecutive_failures,"
                "latency_ms=excluded.latency_ms,event_count=excluded.event_count",
                (name, state, c._last_success, getattr(c, "_last_attempt", None), c._last_error,
                 c._failures, c._last_latency_ms, c._count))

        # 计算链路停滞
        stale_s = float(self.cfg.get("heartbeat", "pipeline_stale_s", default=120))
        pipe_age = (now - self.pipeline.stats["last_process_ts"]) / 1000.0
        pipe_state = "OK" if pipe_age < stale_s else "STALE"

        snap = self.snapshot_internal(pipe_state)
        self.db.set_metric("heartbeat", snap, now)
        return changes

    def snapshot_internal(self, pipe_state: str = "OK") -> dict:
        now = now_ms()
        sources = []
        for name, c in self.collectors.items():
            last = c._last_success
            sources.append({
                "source": name,
                "state": self._state.get(name, PENDING),
                "enabled": c.enabled,
                "interval_s": c.interval,
                "last_success": last,
                "age_s": round((now - last) / 1000.0, 1) if last else None,
                "last_error": c._last_error,
                "failures": c._failures,
                "latency_ms": c._last_latency_ms,
                "event_count": c._count,
            })
        last_ev = self.pipeline.stats.get("last_event_time")
        last_ingest = self.pipeline.stats.get("last_ingest_time")
        return {
            "guard_alive": self.alive,
            "started_at": self.started_at,
            "last_tick": self.last_tick,
            "tick_count": self.tick_count,
            "pipeline_state": pipe_state,
            "pipeline_age_s": round((now - self.pipeline.stats["last_process_ts"]) / 1000.0, 1),
            "sources": sources,
            "degraded": [s["source"] for s in sources if s["state"] in (STALE, DOWN)],
            # 端到端延迟：数据源标注时间 -> 本地处理完成。
            # 注意：新闻类 event_time 是「发布时刻」，天然落后于当下，
            # 直接相减会得出几十分钟的假延迟。因此同时给出两个口径：
            #   end_to_end_latency_ms  = 最新一条事件的 event_time 到现在的间隔
            #   ingest_age_ms          = 最新一条事件入库到现在的间隔（更接近「链路是否活着」）
            "end_to_end_latency_ms": (now - last_ev) if last_ev else None,
            "ingest_age_ms": (now - last_ingest) if last_ingest else None,
        }

    def snapshot(self) -> dict:
        return self.snapshot_internal()

    def changes(self, limit: int = 50) -> list[dict]:
        return self._events[-limit:]