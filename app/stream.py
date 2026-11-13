"""流计算层：基于 event_time 的窗口 + 水位线。

要点
----
* 只用 event_time 切窗，绝不用处理时间（wall clock）。
* 水位线 = 已见最大 event_time - allowed_lateness。
* 早于水位线的数据视为迟到：
    - late_policy=drop      -> 丢弃并计入 late_dropped 指标（默认）
    - late_policy=recompute -> 重新计算其所属窗口（窗口未过期时）
* 窗口按 subject 隔离，内存中保留最近 window+grace 的数据。
"""
from __future__ import annotations

import bisect
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field

from .bus import Event


class Watermark:
    def __init__(self, allowed_lateness_ms: int):
        self.allowed = int(allowed_lateness_ms)
        self.max_seen = 0
        self._lock = threading.Lock()
        self.stats = {"late_dropped": 0, "late_recomputed": 0, "out_of_order": 0}

    def observe(self, event_time: int) -> bool:
        """返回 True 表示该事件是准时的（未落在水位线之下）。"""
        with self._lock:
            late = event_time < self.max_seen - self.allowed
            if event_time > self.max_seen:
                self.max_seen = event_time
            elif not late:
                self.stats["out_of_order"] += 1
            return not late

    @property
    def value(self) -> int:
        return self.max_seen - self.allowed

    def snapshot(self) -> dict:
        return {"watermark": self.value, "max_seen": self.max_seen, **self.stats}


class WatermarkSet:
    """按流分区维护水位线。

    为什么必须分区：行情是「此刻」的时间戳，新闻是「发布时刻」的时间戳，
    二者天然不同频。用一条全局水位线会把旧新闻全部判成迟到而丢弃——
    这不是「迟到数据策略」，是丢数据。
    分区键 = (kind, source)，即每条采集流各有一条水位线。
    """

    def __init__(self, allowed_lateness_ms: int):
        self.allowed = int(allowed_lateness_ms)
        self._wm: dict[str, Watermark] = {}
        self._lock = threading.Lock()

    def observe(self, key: str, event_time: int) -> bool:
        with self._lock:
            wm = self._wm.get(key)
            if wm is None:
                wm = Watermark(self.allowed)
                self._wm[key] = wm
        return wm.observe(event_time)

    def value(self, key: str) -> int:
        with self._lock:
            wm = self._wm.get(key)
        return wm.value if wm else 0

    def min_value(self) -> int:
        """全局推进位：取各分区水位线的最小值（保守，用于展示）。"""
        with self._lock:
            vals = [w.value for w in self._wm.values() if w.max_seen]
        return min(vals) if vals else 0

    def snapshot(self) -> dict:
        with self._lock:
            per = {k: w.snapshot() for k, w in self._wm.items()}
        agg = {"late_dropped": 0, "late_recomputed": 0, "out_of_order": 0}
        for s in per.values():
            for k in agg:
                agg[k] += s.get(k, 0)
        return {"watermark": self.min_value(), "partitions": per, **agg}

@dataclass
class Point:
    event_time: int
    value: float
    seq: int = 0
    payload: dict = field(default_factory=dict)


class SeriesWindow:
    """单标的时间序列缓冲：支持「最近 N 毫秒内」与「前一个点」查询。"""

    def __init__(self, keep_ms: int):
        self.keep_ms = keep_ms
        self._times: list[int] = []
        self._points: list[Point] = []
        self._lock = threading.Lock()

    def add(self, p: Point):
        with self._lock:
            i = bisect.bisect_right(self._times, p.event_time)
            self._times.insert(i, p.event_time)
            self._points.insert(i, p)
            # 修剪：保留 keep_ms 内的数据
            cutoff = p.event_time - self.keep_ms
            j = bisect.bisect_left(self._times, cutoff)
            if j > 0:
                del self._times[:j]
                del self._points[:j]

    def points_since(self, ts: int) -> list[Point]:
        with self._lock:
            i = bisect.bisect_left(self._times, ts)
            return list(self._points[i:])

    def previous(self, before_ts: int) -> Point | None:
        with self._lock:
            i = bisect.bisect_left(self._times, before_ts)
            return self._points[i - 1] if i > 0 else None

    def latest(self) -> Point | None:
        with self._lock:
            return self._points[-1] if self._points else None

    def size(self) -> int:
        with self._lock:
            return len(self._points)


class WindowStore:
    """按 subject 管理 SeriesWindow，供规则引擎查询。"""

    def __init__(self, keep_ms: int = 3_600_000):
        self.keep_ms = keep_ms
        self._series: dict[str, SeriesWindow] = {}
        self._lock = threading.Lock()

    def get(self, subject: str) -> SeriesWindow:
        with self._lock:
            w = self._series.get(subject)
            if w is None:
                w = SeriesWindow(self.keep_ms)
                self._series[subject] = w
            return w

    def add(self, subject: str, ev: Event, value: float):
        self.get(subject).add(Point(ev.event_time, value, ev.seq, ev.payload))

    def change_rate(self, subject: str, window_ms: int, event_time: int) -> float | None:
        """窗口内变动率 (%)：用窗口起点前最近一个点作为基准。"""
        w = self.get(subject)
        pts = w.points_since(event_time - window_ms)
        if not pts:
            return None
        cur = pts[-1].value
        base_pt = w.previous(event_time - window_ms) or (pts[0] if len(pts) > 1 else None)
        if base_pt is None or not base_pt.value:
            return None
        return (cur / base_pt.value - 1) * 100

    def latest_payload(self, subject: str) -> dict | None:
        """取某标的最近一个点的 payload（含 price/prev_close 等原始字段）。"""
        w = self.get(subject)
        p = w.latest()
        return p.payload if p else None

    def series(self, subject: str, since_ms: int | None = None,
               max_points: int = 400) -> list[list]:
        """取某标的的时间序列 [[ts, value], ...]，超过 max_points 时等距抽稀。"""
        w = self.get(subject)
        pts = w.points_since(since_ms) if since_ms else list(w._points)
        vals = [[p.event_time, p.value] for p in pts]
        if len(vals) > max_points:
            step = len(vals) / max_points
            vals = [vals[int(i * step)] for i in range(max_points)] + [vals[-1]]
        return vals

    def stats(self) -> dict:
        with self._lock:
            return {"series": len(self._series),
                    "points": sum(w.size() for w in self._series.values())}