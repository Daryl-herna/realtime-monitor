"""监控任务：默认任务种子 + CRUD。任务全部来自配置/数据库，不硬编码标的。"""
from __future__ import annotations

import json
import uuid

from .config import now_ms
from .db import Database


DEFAULT_TASKS = [
    {
        "id": "t-index-pct",
        "name": "指数日内涨跌幅",
        "type": "price",
        "enabled": 1,
        "config": {
            "subjects": ["sh000001", "sh000300", "sz399001", "sz399006",
                         "sh510300", "sh518880"],
            "rules": [
                {"id": "pct-1", "type": "change_pct", "threshold": 1.0, "severity": "P2", "enabled": True},
                {"id": "pct-2", "type": "change_pct", "threshold": 2.0, "severity": "P1", "enabled": True},
            ],
            "cooldown_s": 300,
        },
    },
    {
        "id": "t-gold-window",
        "name": "黄金主力短窗异动",
        "type": "price",
        "enabled": 1,
        "config": {
            "subjects": ["nf_AU0"],
            "rules": [
                {"id": "w5-08", "type": "window_change_pct", "window_s": 300, "threshold": 0.8,
                 "severity": "P2", "enabled": True},
                {"id": "w5-15", "type": "window_change_pct", "window_s": 300, "threshold": 1.5,
                 "severity": "P1", "enabled": True},
            ],
            "cooldown_s": 180,
        },
    },
    {
        "id": "t-fx",
        "name": "USD/CNY 汇率异动",
        "type": "price",
        "enabled": 1,
        "config": {
            "subjects": ["fx_susdcny", "fx_seurusd"],
            "rules": [
                {"id": "fx-w", "type": "window_change_pct", "window_s": 600, "threshold": 0.3,
                 "severity": "P2", "enabled": True},
            ],
            "cooldown_s": 600,
        },
    },
    {
        "id": "t-breakout",
        "name": "指数突破 20 日高低点",
        "type": "price",
        "enabled": 1,
        "config": {
            "subjects": ["sh000001", "sh000300", "sz399001", "sz399006",
                         "sh510300", "sh518880", "nf_AU0", "nf_CU0", "nf_RB0"],
            "rules": [
                {"id": "bo", "type": "breakout", "days": 20, "direction": "both",
                 "severity": "P1", "enabled": True},
            ],
            "cooldown_s": 3600,
        },
    },
    {
        "id": "t-global-24h",
        "name": "外盘 24 小时品种",
        "type": "price",
        "enabled": 1,
        "config": {
            "subjects": ["hf_GC", "hf_XAU", "hf_XAG", "hf_CL", "fx_seurusd", "fx_susdcny"],
            "rules": [
                {"id": "g5-03", "type": "window_change_pct", "window_s": 300, "threshold": 0.3,
                 "severity": "P2", "enabled": True},
                {"id": "g5-10", "type": "window_change_pct", "window_s": 300, "threshold": 1.0,
                 "severity": "P1", "enabled": True},
                {"id": "g-pct2", "type": "change_pct", "threshold": 2.0,
                 "severity": "P1", "enabled": True},
            ],
            "cooldown_s": 300,
        },
    },
    {
        "id": "t-futures-cn",
        "name": "国内商品期货",
        "type": "price",
        "enabled": 1,
        "config": {
            "subjects": ["nf_CU0", "nf_RB0"],
            "rules": [
                {"id": "fu-w", "type": "window_change_pct", "window_s": 300, "threshold": 0.5,
                 "severity": "P2", "enabled": True},
                {"id": "fu-p", "type": "change_pct", "threshold": 2.0,
                 "severity": "P1", "enabled": True},
            ],
            "cooldown_s": 300,
        },
    },
    {
        "id": "t-pbc-policy",
        "name": "央行政策发布",
        "type": "info",
        "enabled": 1,
        "config": {
            "sources": ["pbc"],
            "rules": [
                {"id": "pbc-new", "type": "new_item", "severity": "P2", "enabled": True},
                {"id": "pbc-kw", "type": "keyword",
                 "keywords": ["降准", "降息", "MLF", "LPR", "逆回购", "存款准备金"],
                 "severity": "P1", "enabled": True},
            ],
            "cooldown_s": 1800,
        },
    },
    {
        "id": "t-news-kw",
        "name": "财经要闻关键词",
        "type": "info",
        "enabled": 1,
        "config": {
            "sources": ["em_rss", "sina_roll"],
            "rules": [
                {"id": "kw", "type": "keyword",
                 "keywords": ["证监会", "降准", "降息", "美联储", "关税", "制裁", "暴跌"],
                 "severity": "P2", "enabled": True},
            ],
            "cooldown_s": 600,
        },
    },
    {
        "id": "t-notice",
        "name": "A股公告监测",
        "type": "info",
        "enabled": 1,
        "config": {
            "sources": ["em_notice"],
            "rules": [
                {"id": "nt-kw", "type": "keyword",
                 "keywords": ["重大合同", "重大资产重组", "停牌", "退市", "业绩预告", "增持", "减持", "问询"],
                 "severity": "P3", "enabled": True},
            ],
            "cooldown_s": 900,
        },
    },
]


# 已废弃的内置任务：升级时从库中移除，避免残留任务继续触发
OBSOLETE_TASKS = ["t-us-index"]


def purge_obsolete_tasks(db: Database) -> int:
    n = 0
    for tid in OBSOLETE_TASKS:
        cur = db.execute("DELETE FROM tasks WHERE id=?", (tid,))
        try:
            n += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except Exception:
            pass
    return n


def seed_tasks(db: Database) -> int:
    """写入默认任务，并同步「内置任务」的标的与规则。

    早期版本只插入不覆盖，导致改了 DEFAULT_TASKS 里的 subjects 后，
    已存在的数据库仍是旧配置（表现为新加的标的永远不触发告警）。
    现在：用户自建的任务（id 不在 DEFAULT_TASKS 里）完全不动；
    内置任务则用新配置覆盖 subjects/sources 与 rules，保留用户的 enabled 开关。
    """
    n = 0
    builtin = {t["id"] for t in DEFAULT_TASKS}
    for t in DEFAULT_TASKS:
        ts = now_ms()
        exists = db.query_one("SELECT id, enabled FROM tasks WHERE id=?", (t["id"],))
        if not exists:
            db.execute(
                "INSERT INTO tasks(id,name,type,enabled,config,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (t["id"], t["name"], t["type"], t["enabled"],
                 json.dumps(t["config"], ensure_ascii=False), ts, ts))
            n += 1
        else:
            # 保留用户手动设置的启停状态，其余按最新内置定义同步
            db.execute(
                "UPDATE tasks SET name=?, type=?, config=?, updated_at=? WHERE id=?",
                (t["name"], t["type"], json.dumps(t["config"], ensure_ascii=False),
                 ts, t["id"]))
    return n


def list_tasks(db: Database) -> list[dict]:
    rows = db.query("SELECT * FROM tasks ORDER BY created_at")
    for r in rows:
        r["config"] = json.loads(r["config"])
        r["enabled"] = bool(r["enabled"])
    return rows


def get_task(db: Database, task_id: str) -> dict | None:
    r = db.query_one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if not r:
        return None
    r["config"] = json.loads(r["config"])
    r["enabled"] = bool(r["enabled"])
    return r


def upsert_task(db: Database, payload: dict) -> dict:
    tid = payload.get("id") or f"t-{uuid.uuid4().hex[:8]}"
    now = now_ms()
    cfg = payload.get("config") or {}
    if isinstance(cfg, str):
        cfg = json.loads(cfg)
    name = payload.get("name") or tid
    ttype = payload.get("type") or "price"
    if ttype not in ("price", "info"):
        raise ValueError("type 必须是 price 或 info")
    enabled = 1 if payload.get("enabled", True) else 0
    exists = db.query_one("SELECT id FROM tasks WHERE id=?", (tid,))
    if exists:
        db.execute(
            "UPDATE tasks SET name=?,type=?,enabled=?,config=?,updated_at=? WHERE id=?",
            (name, ttype, enabled, json.dumps(cfg, ensure_ascii=False), now, tid))
    else:
        db.execute(
            "INSERT INTO tasks(id,name,type,enabled,config,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (tid, name, ttype, enabled, json.dumps(cfg, ensure_ascii=False), now, now))
    return get_task(db, tid)


def delete_task(db: Database, task_id: str) -> bool:
    db.execute("DELETE FROM tasks WHERE id=?", (task_id,))
    return True


def toggle_task(db: Database, task_id: str, enabled: bool) -> dict | None:
    db.execute("UPDATE tasks SET enabled=?, updated_at=? WHERE id=?",
               (1 if enabled else 0, now_ms(), task_id))
    return get_task(db, task_id)