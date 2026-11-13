"""计算链路：事件总线 -> 水位线 -> 规则引擎 -> 告警决策 -> SSE 推送。"""
from __future__ import annotations

import json
import queue
import threading
import time

from .alerts import AlertDecider
from .bus import Event, EventBus
from .config import Config, now_ms
from .db import Database
from .kline import KLineStore, enrich_with_indicators
from .rules.engine import Hit, RuleEngine
from .stream import WatermarkSet, WindowStore


class Hub:
    """SSE 订阅中心：每客户端一个队列，支持断线重连按 Last-Event-ID 续传。"""

    def __init__(self):
        self._clients: dict[str, "queue.Queue[dict]"] = {}
        self._lock = threading.Lock()
        self._history: list[dict] = []   # 最近 500 条，供 Last-Event-ID 补发
        self.delivered = 0

    def register(self) -> tuple[str, "queue.Queue[dict]"]:
        cid = f"c{int(time.time()*1000)}_{len(self._clients)}"
        q: "queue.Queue[dict]" = queue.Queue(maxsize=1000)
        with self._lock:
            self._clients[cid] = q
        return cid, q

    def unregister(self, cid: str):
        with self._lock:
            self._clients.pop(cid, None)

    def broadcast(self, msg: dict):
        with self._lock:
            self._history.append(msg)
            if len(self._history) > 500:
                self._history = self._history[-500:]
            clients = list(self._clients.items())
        for cid, q in clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass

    def since(self, last_event_id: str | None) -> list[dict]:
        if not last_event_id:
            return []
        with self._lock:
            for i, m in enumerate(self._history):
                if m.get("event_id") == last_event_id:
                    return self._history[i + 1:]
        return []

    def client_count(self) -> int:
        with self._lock:
            return len(self._clients)


class Pipeline:
    def __init__(self, cfg: Config, db: Database, bus: EventBus):
        self.cfg = cfg
        self.db = db
        self.bus = bus
        self.wins = WindowStore(keep_ms=3_600_000)
        self.klines = KLineStore()
        self.engine = RuleEngine(self.wins)
        self.decider = AlertDecider(cfg, db)
        self.hub = Hub()
        self.watermark = WatermarkSet(int(cfg.get("stream", "allowed_lateness_ms", default=3000)))
        self.late_policy = cfg.get("stream", "late_policy", default="drop")
        self._tasks: list[dict] = []
        self._tasks_lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.stats = {"processed": 0, "last_event_time": None, "last_process_ts": now_ms()}
        self._late_dropped = 0
        self._late_recomputed = 0
        self._last_quotes: dict[str, dict] = {}
        self._quotes_lock = threading.Lock()
        self._quotes_ts = 0
        self.reload_tasks()

    # ---------- 任务 ----------
    def reload_tasks(self):
        rows = self.db.query("SELECT * FROM tasks ORDER BY created_at")
        tasks = []
        for r in rows:
            try:
                r["config"] = json.loads(r["config"])
            except (TypeError, ValueError):
                continue
            r["enabled"] = bool(r["enabled"])
            tasks.append(r)
        with self._tasks_lock:
            self._tasks = tasks

    def tasks(self) -> list[dict]:
        with self._tasks_lock:
            return list(self._tasks)

    # ---------- 生命周期 ----------
    def start(self):
        self.bus.subscribe(self.on_event)
        self._stop.clear()
        for target, name in ((self._resolve_loop, "resolve-loop"), (self._digest_loop, "digest-loop")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()

    # ---------- 核心 ----------
    def on_event(self, ev: Event):
        if ev.kind == "baseline":
            # 基线更新后立刻用最新价复核突破/放量规则，
            # 否则要等下一笔行情才会发现「已突破」，最坏滞后一个采集周期。
            self.engine.update_baseline(ev)
            latest = self.wins.get(ev.subject).latest()
            if latest:
                for task in self.tasks():
                    if task.get("type") != "price" or not task.get("enabled"):
                        continue
                    if not self.engine._matches(task, ev):
                        continue
                    fake = Event(event_id=f"baseline-{ev.subject}-{latest.event_time}",
                                 source="baseline", kind="quote", subject=ev.subject,
                                 event_time=latest.event_time, ingest_time=now_ms(),
                                 payload=latest.payload)
                    for hit in self.engine.evaluate(fake, [task], record_state=False):
                        self._dispatch_hit(hit)
            return
        partition = f"{ev.kind}:{ev.source}"
        ok = self.watermark.observe(partition, ev.event_time)
        if not ok:
            if self.late_policy == "recompute":
                self._late_recomputed += 1
            else:
                self._late_dropped += 1
                return
        self.stats["processed"] += 1
        if ev.kind == "quote":
            # 只有回填数据带可信的分钟成交量
            self.klines.update(ev.subject, ev.payload.get("price"), ev.event_time,
                               ev.payload.get("volume") or 0 if ev.payload.get("backfilled") else 0.0,
                               is_cumulative=False)
        self.stats["last_event_time"] = ev.event_time
        self.stats["last_ingest_time"] = ev.ingest_time
        self.stats["last_process_ts"] = now_ms()
        hits = self.engine.evaluate(ev, self.tasks())
        for hit in hits:
            self._dispatch_hit(hit)
        self._check_recovery(ev)

    def _dispatch_hit(self, hit: Hit):
        action = self.decider.decide(hit)
        if action["action"] == "fire":
            self.hub.broadcast({
                "type": "alert", "event_id": action["alert"]["alert_id"],
                "data": action["alert"],
            })

    def _check_recovery(self, ev: Event):
        """价格回到阈值内 / 信息侧不再命中 -> 标记恢复。简化实现：按任务+标的评估当前是否仍有活跃告警。"""
        if ev.kind != "quote":
            return
        price = ev.payload.get("price")
        prev = ev.payload.get("prev_close")
        if price is None or not prev:
            return
        for task in self.tasks():
            if task.get("type") != "price" or not task.get("enabled"):
                continue
            for r in (task["config"].get("rules") or []):
                if r.get("type") != "change_pct":
                    continue
                pct = (price / prev - 1) * 100
                if abs(pct) < abs(r.get("threshold", 0)):
                    self.decider.mark_recovered(task["id"], ev.subject, r.get("id", "change_pct"))

    def _resolve_loop(self):
        while not self._stop.is_set():
            try:
                for alert in self.decider.drain_resolved():
                    self.hub.broadcast({"type": "alert", "event_id": alert["alert_id"], "data": alert})
            except Exception:
                import traceback; traceback.print_exc()
            self._stop.wait(5.0)

    def _digest_loop(self):
        while not self._stop.is_set():
            try:
                alert = self.decider.drain_digest()
                if alert:
                    self.hub.broadcast({"type": "alert", "event_id": alert["alert_id"], "data": alert})
            except Exception:
                import traceback; traceback.print_exc()
            self._stop.wait(5.0)

    # ---------- 回放 ----------
    def replay_compare(self, task_id: str, from_ts: int, to_ts: int,
                       new_config: dict | None = None) -> dict:
        """用落盘历史事件回放，对比「当前规则」与「新规则」的命中差异。"""
        task = self.db.query_one("SELECT * FROM tasks WHERE id=?", (task_id,))
        if not task:
            return {"error": "task not found"}
        old_cfg = json.loads(task["config"])
        task_old = {**task, "config": old_cfg, "enabled": True}
        task_new = {**task, "config": new_config or old_cfg, "enabled": True}

        kinds = ["quote"] if task["type"] == "price" else ["news", "notice"]
        events = self.bus.replay(from_ts, to_ts, kinds=kinds)
        baseline_events = self.bus.replay(from_ts - 86400_000, to_ts, kinds=["baseline"])

        def run(task_def: dict) -> list[dict]:
            engine = RuleEngine(WindowStore(keep_ms=3_600_000))
            for b in baseline_events:
                engine.update_baseline(b)
            out = []
            for ev in events:
                for h in engine.evaluate(ev, [task_def], record_state=True):
                    out.append({"event_time": h.event_time, "rule": h.rule_id,
                                "title": h.title, "severity": h.severity})
            return out

        old_hits = run(task_old)
        new_hits = run(task_new)
        old_keys = {(h["event_time"], h["rule"]) for h in old_hits}
        new_keys = {(h["event_time"], h["rule"]) for h in new_hits}
        result = {
            "task_id": task_id, "from": from_ts, "to": to_ts,
            "events_scanned": len(events),
            "old_hits": len(old_hits), "new_hits": len(new_hits),
            "only_old": [h for h in old_hits if (h["event_time"], h["rule"]) not in new_keys][:100],
            "only_new": [h for h in new_hits if (h["event_time"], h["rule"]) not in old_keys][:100],
            "sample_old": old_hits[:20], "sample_new": new_hits[:20],
        }
        self.db.execute(
            "INSERT INTO replays(id,task_id,from_ts,to_ts,event_cnt,old_hits,new_hits,created_at,detail) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (f"rp{int(time.time()*1000)}", task_id, from_ts, to_ts, len(events),
             len(old_hits), len(new_hits), now_ms(),
             json.dumps({"only_old": len(result["only_old"]), "only_new": len(result["only_new"])},
                        ensure_ascii=False)))
        return result

    # ---------- 行情快照 ----------
    def _load_series_from_db(self, subject: str, since_ms: int, max_points: int = 240) -> list[list]:
        """从落盘事件回填走势序列。

        内存窗口只保留进程启动后的点；重启后看板会空一片。
        历史事件本来就在 events 表里，直接查出来用即可——这也是「可重放」的顺手收益。
        """
        rows = self.db.query(
            "SELECT event_time, payload FROM events "
            "WHERE subject=? AND kind='quote' AND event_time>=? "
            "ORDER BY event_time ASC", (subject, since_ms))
        vals = []
        for r in rows:
            try:
                p = json.loads(r["payload"])
                price = p.get("price")
                if price is not None:
                    vals.append([r["event_time"], float(price)])
            except (TypeError, ValueError):
                continue
        # 先压缩连续相同价格：收盘后每 5s 一个快照但价格不变，
        # 若直接等距抽样，几千个重复点会挤掉白天真正有变化的点，图形变成一条平线。
        if len(vals) > 2:
            compact = [vals[0]]
            for i in range(1, len(vals) - 1):
                if vals[i][1] != vals[i - 1][1] or vals[i][1] != vals[i + 1][1]:
                    compact.append(vals[i])
            compact.append(vals[-1])
            vals = compact
        if len(vals) > max_points:
            step = len(vals) / max_points
            vals = [vals[int(i * step)] for i in range(max_points)] + [vals[-1]]
        return vals

    def _latest_payload_from_db(self, subject: str) -> dict | None:
        r = self.db.query_one(
            "SELECT payload FROM events WHERE subject=? AND kind='quote' "
            "ORDER BY event_time DESC LIMIT 1", (subject,))
        if not r:
            return None
        try:
            return json.loads(r["payload"])
        except (TypeError, ValueError):
            return None

    def set_last_quotes(self, quotes: dict):
        """采集器每轮把「本次实际抓到的全部快照」交上来。

        为什么需要它：事件总线对内容相同的快照做了去重（dedup_key 含价格），
        所以收盘后价格不变时不会产生新事件。若看板只读事件表，就会漏掉
        「采集成功但价格没变」的标的，表现为卡片消失或停在旧值。
        这里保存采集器的实时视图，作为看板的最新值来源。
        """
        with self._quotes_lock:
            self._last_quotes.update(quotes)
            self._quotes_ts = now_ms()
        # 快照同样喂给 K 线：价格不变时也更新「最后一根」的收盘与计数
        for subj, p in quotes.items():
            # 实时快照只更新价格，不传 volume。
            # 原因：新浪快照的 volume 字段在指数上是「成交额」、在个股上是「成交量」，
            # 口径不统一，且与历史 K 线的「每分钟量」完全不同源。
            # 成交量统一由回填的分钟 K 线提供，避免两类数据混算。
            self.klines.update(subj, p.get("price"), now_ms(), 0.0, is_cumulative=False)

    def quotes(self, window_s: int = 1800) -> list[dict]:
        """返回所有监控标的的最新价格、涨跌幅与最近走势序列。"""
        out = []
        seen = set()
        with self._quotes_lock:
            live_snapshot = dict(self._last_quotes)
        subjects = self.watched_subjects()
        # 采集器最新快照里的标的也要纳入（即使还没产生事件）
        for s in live_snapshot:
            if s not in subjects:
                subjects.append(s)
        for subj in subjects:
            if subj in seen:
                continue
            seen.add(subj)
            payload = (live_snapshot.get(subj)
                       or self.wins.latest_payload(subj)
                       or self._latest_payload_from_db(subj))
            if not payload:
                continue
            price = payload.get("price")
            prev = payload.get("prev_close")
            if price is None:
                continue
            chg = payload.get("change_pct")
            if chg is None and prev:
                chg = round((price / prev - 1) * 100, 4)
            now = now_ms()
            since = now - window_s * 1000
            series = self.wins.series(subj, since_ms=since, max_points=240)
            if len(series) < 2:
                series = self._load_series_from_db(subj, since, 240)
            # 关键：窗口内点数不足时（低波动品种 + 短窗口，常见于外汇），
            # 若照实只画 2 个点，会得到一条毫无意义的直线。
            # 此时向后回溯取满至少 MIN_POINTS 个点，并如实告知前端
            # 「实际展示的时间跨度」与「请求的窗口」不一致。
            MIN_POINTS = 12
            expanded = False
            if len(series) < MIN_POINTS:
                wider = self._load_series_from_db(subj, 0, 400)
                if len(wider) > len(series):
                    series = wider[-max(MIN_POINTS, 60):]
                    expanded = True
            out.append({
                "subject": subj,
                "name": payload.get("name") or subj,
                "class": payload.get("class") or "index",
                "price": price,
                "prev_close": prev,
                "change_pct": chg,
                "open": payload.get("open"),
                "high": payload.get("high"),
                "low": payload.get("low"),
                "volume": payload.get("volume"),
                "source_time": payload.get("source_time"),
                "updated_at": series[-1][0] if series else None,
                "stale_s": round((now - series[-1][0]) / 1000) if series else None,
                "live": self._is_live(subj, now - series[-1][0] if series else 9e9),
                "series": series,
                "requested_window_s": window_s,
                "actual_span_s": round((series[-1][0] - series[0][0]) / 1000) if len(series) > 1 else 0,
                "window_expanded": expanded,
            })
        return out

    def _is_live(self, subject: str, age_ms: float) -> bool:
        """数据是否在实时更新。

        判定依据是「数据源标注的时间是否还在前进」，而不是我们自己多久抓一次。
        新浪的外盘/外汇快照本身就是按分钟刷新的，抓得再勤源时间也不动；
        若按固定秒数判断，会把正常更新的标的误判为「已收盘」。
        做法：看该标的最近几个点的 event_time 跨度——跨度 > 0 说明源在动。
        """
        if age_ms is None:
            return False
        # 距现在太久（超过 10 分钟）一定不是实时
        if age_ms > 600_000:
            return False
        recent = self._load_series_from_db(subject, now_ms() - 900_000, 30)
        if len(recent) < 2:
            return age_ms < 180_000
        span = recent[-1][0] - recent[0][0]
        return span > 0 and age_ms < 300_000

    def watched_subjects(self) -> list[str]:
        """从任务配置汇总所有被监控的价格标的。"""
        subs = []
        for task in self.tasks():
            if task.get("type") != "price" or not task.get("enabled"):
                continue
            for s in (task["config"].get("subjects") or []):
                if s not in subs:
                    subs.append(s)
        return subs

    def kline_data(self, subject: str, period: int, limit: int = 200,
                   ma_windows=(5, 10, 20)) -> dict:
        """返回某标的的 K 线与均线。若内存为空，从事件表回填。"""
        bars = self.klines.bars(subject, period, limit)
        if len(bars) < 2:
            self._seed_klines_from_db(subject, period, limit)
            bars = self.klines.bars(subject, period, limit)
        return enrich_with_indicators(bars, ma_windows)

    def compare(self, subjects: list[str], period: int = 300,
                limit: int = 200) -> dict:
        """多标的同屏对比：统一归一化到 100，直接看相对强弱。

        归一化基准取各自序列的第一个收盘价，这样不同价位（沪铜 10 万、螺纹 3 千）
        能画在同一张图上。
        """
        out = []
        for subj in subjects:
            data = self.kline_data(subj, period, limit)
            bars = data.get("bars") or []
            if len(bars) < 2:
                out.append({"subject": subj, "name": subj, "series": [],
                            "change_pct": None, "bars": len(bars)})
                continue
            base = bars[0]["c"]
            series = [[b["t"], round((b["c"] / base) * 100, 3)] for b in bars] if base else []
            last = bars[-1]["c"]
            payload = self.wins.latest_payload(subj) or self._latest_payload_from_db(subj) or {}
            out.append({
                "subject": subj,
                "name": payload.get("name") or subj,
                "series": series,
                "change_pct": round((last / base - 1) * 100, 3) if base else None,
                "bars": len(bars),
                "last": last,
            })
        return {"period": period, "items": out}

    def _seed_klines_from_db(self, subject: str, period: int, limit: int = 200):
        """用事件表里的历史快照重建 K 线（重启后或新标的）。

        回溯窗口 = 周期 × 需要的根数，但至少覆盖最近 12 小时，
        否则重启后 K 线只剩几根，均线全是 None。
        """
        span_ms = max(period * 1000 * min(limit, 480), 12 * 3600 * 1000)
        since = now_ms() - span_ms
        rows = self.db.query(
            "SELECT event_time, payload FROM events "
            "WHERE subject=? AND kind='quote' AND event_time>=? "
            "ORDER BY event_time ASC", (subject, since))
        for r in rows:
            try:
                p = json.loads(r["payload"])
                if p.get("price") is not None:
                    # 回填数据的 volume 是「该分钟增量」，不是当日累计
                    self.klines.update(subject, float(p["price"]), int(r["event_time"]),
                                       float(p.get("volume") or 0),
                                       is_cumulative=not p.get("backfilled"))
            except (TypeError, ValueError):
                continue

    def snapshot(self) -> dict:
        return {
            "processed": self.stats["processed"],
            "last_event_time": self.stats["last_event_time"],
            "last_ingest_time": self.stats.get("last_ingest_time"),
            "last_process_ts": self.stats["last_process_ts"],
            "watermark": {**self.watermark.snapshot(),
                          "late_dropped": self._late_dropped,
                          "late_recomputed": self._late_recomputed},
            "windows": self.wins.stats(),
            "klines": self.klines.snapshot(),
            "engine": self.engine.stats,
            "decider": self.decider.stats,
            "sse_clients": self.hub.client_count(),
        }