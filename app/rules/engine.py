"""规则引擎：价格异动 + 信息异动。

规则全部来自任务配置（tasks 表），不硬编码。
每条规则命中后产出 Hit，交给告警决策层做去重/冷却/分级。
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from ..bus import Event
from ..config import now_ms
from ..stream import WindowStore


@dataclass
class Hit:
    task_id: str
    task_name: str
    rule_id: str
    rule_type: str
    subject: str
    subject_name: str
    severity: str
    title: str
    detail: str
    value: float | None
    threshold: float | None
    event_time: int
    event_id: str
    payload: dict = field(default_factory=dict)
    resolved: bool = False
    # 去重范围：价格异动按「任务+规则+标的」聚合（同因）；信息异动按
    # 「任务+规则+条目」聚合（每篇不同的文章就是不同的因）。
    dedup_scope: str = ""


# ---------------- 价格规则 ----------------

def _price_rules(task: dict, ev: Event, wins: WindowStore, baseline: dict) -> list[Hit]:
    cfg = task["config"]
    rules = cfg.get("rules") or []
    p = ev.payload
    price = p.get("price")
    prev = p.get("prev_close") or p.get("prev")
    if price is None:
        return []
    hits = []
    subject = ev.subject
    name = p.get("name") or subject

    for r in rules:
        if not r.get("enabled", True):
            continue
        rt = r.get("type")
        threshold = r.get("threshold")
        sev = r.get("severity", "P2")
        hit = None

        if rt == "change_pct" and prev:
            pct = (price / prev - 1) * 100
            if threshold is not None and abs(pct) >= abs(threshold):
                hit = (pct, f"{name} 较昨收 {pct:+.2f}%",
                       f"现价 {price} / 昨收 {prev}，触发阈值 {threshold}%")

        elif rt == "window_change_pct":
            win_ms = int(r.get("window_s", 300)) * 1000
            rate = wins.change_rate(subject, win_ms, ev.event_time)
            if rate is not None and threshold is not None and abs(rate) >= abs(threshold):
                hit = (rate, f"{name} {r.get('window_s', 300)}s 内变动 {rate:+.2f}%",
                       f"窗口 {r.get('window_s', 300)}s，现价 {price}，阈值 {threshold}%")

        elif rt == "breakout":
            n = int(r.get("days", 20))
            b = baseline.get(subject)
            if b:
                if r.get("direction", "both") in ("up", "both") and b.get("n_high") and price > b["n_high"]:
                    hit = (price, f"{name} 突破 {n} 日高点 {b['n_high']}",
                           f"现价 {price} > {n} 日最高 {b['n_high']}")
                elif r.get("direction", "both") in ("down", "both") and b.get("n_low") and price < b["n_low"]:
                    hit = (price, f"{name} 跌破 {n} 日低点 {b['n_low']}",
                           f"现价 {price} < {n} 日最低 {b['n_low']}")

        elif rt == "volume_spike":
            b = baseline.get(subject)
            mult = float(r.get("multiple", 2.0))
            vol = p.get("volume")
            if b and b.get("avg_volume") and vol:
                ratio = vol / b["avg_volume"]
                if ratio >= mult:
                    hit = (ratio, f"{name} 成交量放大 {ratio:.2f}×",
                           f"当前量 {vol:.0f} / 日均量 {b['avg_volume']:.0f}，阈值 {mult}×")

        elif rt == "abs_price_band":
            lo, hi = r.get("min"), r.get("max")
            if (lo is not None and price < lo) or (hi is not None and price > hi):
                hit = (price, f"{name} 价格 {price} 越界",
                       f"设定区间 [{lo}, {hi}]，现价 {price}")

        if hit:
            value, title, detail = hit
            hits.append(Hit(
                task_id=task["id"], task_name=task["name"], rule_id=r.get("id", rt),
                rule_type=rt, subject=subject, subject_name=name, severity=sev,
                title=title, detail=detail, value=value, threshold=threshold,
                event_time=ev.event_time, event_id=ev.event_id,
                payload={"price": price, "prev_close": prev},
            ))
    return hits


# ---------------- 信息规则 ----------------

class InfoState:
    """每个任务的信息侧状态：已见内容哈希集合 + 每个条目的上一版哈希。"""

    def __init__(self):
        self.seen: set[str] = set()
        self.by_item: dict[str, str] = {}
        self.lock = __import__("threading").Lock()


def _info_rules(task: dict, ev: Event, state: InfoState) -> list[Hit]:
    cfg = task["config"]
    rules = cfg.get("rules") or []
    p = ev.payload
    title = p.get("title") or ""
    summary = p.get("summary") or ""
    text = f"{title} {summary}"
    content_hash = p.get("content_hash") or ""
    subj = ev.subject
    item_key = p.get("link") or content_hash or title
    is_new = bool(content_hash) and content_hash not in state.seen

    hits = []
    for r in rules:
        if not r.get("enabled", True):
            continue
        rt = r.get("type")
        sev = r.get("severity", "P2")
        hit = None

        if rt == "new_content":
            if is_new:
                hit = (title, f"新增内容：{title[:60]}", f"来源 {p.get('channel', '')}")

        elif rt == "keyword":
            kws = [k for k in (r.get("keywords") or []) if k]
            if kws:
                matched = [k for k in kws if k.lower() in text.lower()]
                if matched:
                    hit = (title, f"关键词命中 [{','.join(matched)}]：{title[:50]}",
                           f"命中词：{', '.join(matched)}")

        elif rt == "regex":
            pat = r.get("pattern")
            if pat:
                try:
                    m = re.search(pat, text)
                except re.error:
                    m = None
                if m:
                    hit = (title, f"正则命中：{title[:50]}", f"匹配：{m.group(0)[:80]}")

        elif rt == "field_change":
            prev_hash = state.by_item.get(item_key)
            if prev_hash and content_hash and prev_hash != content_hash:
                hit = (title, f"内容变更：{title[:50]}", f"哈希 {prev_hash[:8]} → {content_hash[:8]}")

        elif rt == "new_item":
            # 只按「条目是否出现过」判断（不看内容哈希），适合公告/政策列表
            if is_new:
                hit = (title, f"新条目：{title[:60]}", f"来源 {p.get('channel', '')}")

        if hit:
            value, t, d = hit
            hits.append(Hit(
                task_id=task["id"], task_name=task["name"], rule_id=r.get("id", rt),
                rule_type=rt, subject=subj, subject_name=p.get("channel") or subj,
                severity=sev, title=t, detail=d, value=None, threshold=None,
                event_time=ev.event_time, event_id=ev.event_id,
                payload={"link": p.get("link", ""), "channel": p.get("channel", "")},
                dedup_scope=content_hash or item_key,
            ))
    # 规则评估后再更新状态，保证同一事件内多条规则看到一致视图
    if content_hash:
        state.seen.add(content_hash)
        state.by_item[item_key] = content_hash
    return hits


class RuleEngine:
    def __init__(self, wins: WindowStore):
        self.wins = wins
        self.baseline: dict[str, dict] = {}
        self._info_state: dict[str, InfoState] = {}
        self.stats = {"evaluated": 0, "hits": 0}

    def update_baseline(self, ev: Event):
        if ev.kind == "baseline":
            self.baseline[ev.subject] = ev.payload

    def info_state(self, task_id: str) -> InfoState:
        st = self._info_state.get(task_id)
        if st is None:
            st = InfoState()
            self._info_state[task_id] = st
        return st

    def reset_info_state(self, task_id: str | None = None):
        if task_id is None:
            self._info_state.clear()
        else:
            self._info_state.pop(task_id, None)

    @staticmethod
    def _matches(task: dict, ev: Event) -> bool:
        """任务作用域过滤：价格任务只看自己配的标的，信息任务只看自己配的来源。

        没有这一层，任何任务都会对所有事件触发——任务配置形同虚设。
        """
        cfg = task.get("config") or {}
        if task.get("type") == "price":
            subjects = cfg.get("subjects")
            if subjects:
                return ev.subject in subjects
            return True
        if task.get("type") == "info":
            sources = cfg.get("sources")
            if sources:
                # 来源名与采集器名一致（pbc / em_rss / sina_roll / em_notice）
                return ev.source in sources
            return True
        return False

    def evaluate(self, ev: Event, tasks: list[dict], record_state: bool = True) -> list[Hit]:
        """record_state=False 用于回放对比：只算命中，不污染实时状态。"""
        self.stats["evaluated"] += 1
        hits: list[Hit] = []
        if ev.kind == "quote":
            self.wins.add(ev.subject, ev, float(ev.payload.get("price") or 0))
        for task in tasks:
            if not task.get("enabled"):
                continue
            if not self._matches(task, ev):
                continue
            ttype = task.get("type")
            if ttype == "price" and ev.kind == "quote":
                hits += _price_rules(task, ev, self.wins, self.baseline)
            elif ttype == "info" and ev.kind in ("news", "notice"):
                st = self.info_state(task["id"]) if record_state else InfoState()
                hits += _info_rules(task, ev, st)
        self.stats["hits"] += len(hits)
        return hits