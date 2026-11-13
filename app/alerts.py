"""告警决策层：fingerprint 去重 + 冷却 + 聚合 + 分级 + 静默期 + resolved 闭环。"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime

from .config import Config, now_ms
from .db import Database
from .rules.engine import Hit

SEV_ORDER = {"P1": 1, "P2": 2, "P3": 3}


def fingerprint_of(hit: Hit) -> str:
    """同因聚合键。

    价格异动：任务 + 规则 + 标的 -> 同一标的同一规则连续触发算「同一个因」，
    冷却窗口内只推一次。
    信息异动：任务 + 规则 + 条目 -> 每篇不同的文章是「不同的因」，
    不能因为同属一个任务就互相压制（否则一批公告只会剩一条告警）。
    """
    scope = hit.dedup_scope or ""
    raw = f"{hit.task_id}|{hit.rule_id}|{hit.subject}|{scope}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def alert_id_of(fp: str, ts: int) -> str:
    return hashlib.sha256(f"{fp}|{ts}".encode()).hexdigest()[:24]


class AlertDecider:
    """决定一条 Hit 是：立即告警 / 被抑制 / 聚合进摘要 / 恢复。"""

    def __init__(self, cfg: Config, db: Database):
        self.cfg = cfg
        self.db = db
        self._lock = threading.Lock()
        self._last_fire: dict[str, int] = {}      # fingerprint -> 上次推送时间
        self._active: dict[str, dict] = {}        # fingerprint -> 当前 firing 告警
        self._recovered_at: dict[str, int] = {}   # fingerprint -> 条件恢复时间
        self._digest: list[dict] = []
        self._last_digest = now_ms()
        self._minute_window: list[int] = []
        self.stats = {"fired": 0, "suppressed": 0, "digested": 0, "resolved": 0, "storm_blocked": 0}
        self._restore_state()

    def _restore_state(self):
        """从 alerts 表恢复冷却与活跃状态。

        内存字典在重启后会清空；若不复原，重启瞬间同一异动会被重新推送一次，
        破坏「冷却窗口内只推送一次」这条验收标准。
        """
        try:
            rows = self.db.query(
                "SELECT fingerprint, created_at, status FROM alerts "
                "WHERE status='firing' ORDER BY created_at DESC LIMIT 5000")
            for r in rows:
                fp = r["fingerprint"]
                ts = int(r["created_at"] or 0)
                if fp not in self._last_fire or ts > self._last_fire[fp]:
                    self._last_fire[fp] = ts
            # 仍在 firing 的指纹视为活跃，用于 resolved 闭环
            for fp in list(self._last_fire):
                self._active.setdefault(fp, {"fingerprint": fp, "status": "firing"})
        except Exception:
            pass

    # ---------- 静默期 ----------
    def in_silence(self, ts_ms: int | None = None) -> tuple[bool, str]:
        wins = self.cfg.get("alerts", "silence_windows", default=[]) or []
        if not wins:
            return False, ""
        dt = datetime.fromtimestamp((ts_ms or now_ms()) / 1000)
        hhmm = dt.strftime("%H:%M")
        wd = dt.isoweekday()
        for w in wins:
            if w.get("weekdays") and wd not in w["weekdays"]:
                continue
            start, end = w.get("start", "00:00"), w.get("end", "23:59")
            if start <= hhmm <= end:
                return True, w.get("name") or f"{start}-{end}"
        return False, ""

    # ---------- 熔断 ----------
    def _storm(self) -> bool:
        now = now_ms()
        self._minute_window = [t for t in self._minute_window if now - t < 60_000]
        limit = int(self.cfg.get("alerts", "max_alerts_per_minute", default=30))
        return len(self._minute_window) >= limit

    def decide(self, hit: Hit) -> dict:
        """返回动作字典：{action: fire|suppress|digest|resolve, alert: {...}}"""
        fp = fingerprint_of(hit)
        now = hit.event_time or now_ms()
        cooldown = int(hit.payload.get("cooldown_s") or
                       self.cfg.get("alerts", "default_cooldown_s", default=300)) * 1000
        with self._lock:
            silence, reason = self.in_silence(now)
            if silence:
                self._bump_suppression(fp, hit, f"静默期 {reason}")
                return {"action": "suppress", "reason": f"静默期 {reason}"}

            last = self._last_fire.get(fp)
            if last is not None and now - last < cooldown:
                self._bump_suppression(fp, hit, f"冷却中（{int((cooldown - (now - last)) / 1000)}s 后解除）")
                return {"action": "suppress", "reason": "冷却中"}

            if hit.severity == "P3":
                self._digest.append({"hit": hit, "fp": fp})
                self.stats["digested"] += 1
                self._bump_suppression(fp, hit, "P3 聚合")
                return {"action": "digest"}

            if self._storm():
                self.stats["storm_blocked"] += 1
                self._bump_suppression(fp, hit, "告警风暴熔断")
                return {"action": "suppress", "reason": "风暴熔断"}

            aid = alert_id_of(fp, now)
            alert = {
                "alert_id": aid, "fingerprint": fp, "task_id": hit.task_id,
                "rule_id": hit.rule_id, "subject": hit.subject, "severity": hit.severity,
                "status": "firing", "title": hit.title, "detail": hit.detail,
                "event_time": hit.event_time, "created_at": now,
                "payload": {**hit.payload, "subject_name": hit.subject_name,
                            "task_name": hit.task_name, "rule_type": hit.rule_type,
                            "value": hit.value, "threshold": hit.threshold},
            }
            self._last_fire[fp] = now
            self._active[fp] = alert
            self._recovered_at.pop(fp, None)
            self._minute_window.append(now)
            self.stats["fired"] += 1
            self._save_alert(alert)
            return {"action": "fire", "alert": alert}

    def _bump_suppression(self, fp: str, hit: Hit, reason: str):
        self.stats["suppressed"] += 1
        self.db.execute(
            "INSERT INTO suppressions(fingerprint,task_id,count,last_at,reason) VALUES(?,?,1,?,?) "
            "ON CONFLICT(fingerprint) DO UPDATE SET count=count+1, last_at=excluded.last_at, "
            "reason=excluded.reason",
            (fp, hit.task_id, now_ms(), reason))

    def _save_alert(self, a: dict):
        self.db.execute(
            "INSERT OR IGNORE INTO alerts(alert_id,fingerprint,task_id,rule_id,subject,severity,"
            "status,title,detail,event_time,created_at,channel,payload) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (a["alert_id"], a["fingerprint"], a["task_id"], a["rule_id"], a["subject"],
             a["severity"], a["status"], a["title"], a["detail"], a["event_time"],
             a["created_at"], "sse", json.dumps(a["payload"], ensure_ascii=False)))

    # ---------- 恢复 ----------
    def mark_recovered(self, task_id: str, subject: str, rule_id: str, scope: str = ""):
        """条件恢复：为该 fingerprint 排队 resolved（延迟 resolve_after_s 后真正发出）。"""
        fp = hashlib.sha256(f"{task_id}|{rule_id}|{subject}|{scope}".encode()).hexdigest()[:24]
        with self._lock:
            if fp in self._active and fp not in self._recovered_at:
                self._recovered_at[fp] = now_ms()

    def drain_resolved(self) -> list[dict]:
        """把超过 resolve_after_s 的恢复项转成 resolved 告警。"""
        delay = int(self.cfg.get("alerts", "resolve_after_s", default=300)) * 1000
        now = now_ms()
        out = []
        with self._lock:
            for fp, ts in list(self._recovered_at.items()):
                if now - ts < delay:
                    continue
                alert = self._active.pop(fp, None)
                self._recovered_at.pop(fp, None)
                if not alert:
                    continue
                resolved = dict(alert)
                resolved["alert_id"] = hashlib.sha256(f"{fp}|{now}|resolved".encode()).hexdigest()[:24]
                resolved["status"] = "resolved"
                resolved["resolved_at"] = now
                resolved["title"] = f"[已恢复] {alert['title']}"
                self.db.execute(
                    "UPDATE alerts SET status='resolved', resolved_at=? WHERE alert_id=?",
                    (now, alert["alert_id"]))
                self._save_alert(resolved)
                self.stats["resolved"] += 1
                out.append(resolved)
        return out

    # ---------- P3 摘要 ----------
    def drain_digest(self) -> dict | None:
        interval = int(self.cfg.get("alerts", "digest_interval_s", default=120)) * 1000
        now = now_ms()
        with self._lock:
            if not self._digest or now - self._last_digest < interval:
                return None
            items = self._digest
            self._digest = []
            self._last_digest = now
        fp = "digest|" + ",".join(sorted({i["fp"] for i in items}))[:60]
        alert = {
            "alert_id": hashlib.sha256(f"{fp}|{now}".encode()).hexdigest()[:24],
            "fingerprint": fp, "task_id": None, "rule_id": "digest",
            "subject": None, "severity": "P3", "status": "digest",
            "title": f"[摘要] {len(items)} 条低优先级异动",
            "detail": "；".join(i["hit"].title for i in items[:8]),
            "event_time": now, "created_at": now,
            "payload": {"items": [
                {"title": i["hit"].title, "subject": i["hit"].subject,
                 "task": i["hit"].task_name, "event_time": i["hit"].event_time}
                for i in items[:50]]},
        }
        self._save_alert(alert)
        return alert

    def suppression_summary(self, limit: int = 50) -> list[dict]:
        return self.db.query(
            "SELECT * FROM suppressions ORDER BY last_at DESC LIMIT ?", (limit,))