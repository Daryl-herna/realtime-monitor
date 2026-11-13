"""HTTP 服务 + SSE 推送。仅用标准库 http.server。"""
from __future__ import annotations

import json
import mimetypes
import os
import queue
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import Config, now_ms
from .db import Database
from .tasks import delete_task, get_task, list_tasks, toggle_task, upsert_task

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")


class AppState:
    """把所有子系统挂在一处，方便 handler 取用。"""

    def __init__(self, cfg, db, bus, pipeline, heartbeat, collectors, started_at):
        self.cfg = cfg
        self.db = db
        self.bus = bus
        self.pipeline = pipeline
        self.heartbeat = heartbeat
        self.collectors = collectors
        self.started_at = started_at


class Handler(BaseHTTPRequestHandler):
    state: AppState = None
    server_version = "RealtimeMonitor/1.0"
    protocol_version = "HTTP/1.1"

    # ---------- 基础 ----------
    def log_message(self, fmt, *args):
        pass  # 静音；需要时打开

    def _send(self, code: int, body: bytes, ctype="application/json; charset=utf-8", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        if extra:
            for k, v in extra.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))

    def _read_body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        raw = self.rfile.read(n)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    # ---------- 路由 ----------
    def do_GET(self):
        try:
            u = urlparse(self.path)
            path = u.path
            qs = parse_qs(u.query)
            if path == "/":
                return self._static("index.html")
            if path.startswith("/static/"):
                return self._static(path[len("/static/"):])
            if path == "/api/health":
                return self._json(self._health())
            if path == "/api/alerts":
                return self._json(self._alerts(qs))
            if path == "/api/tasks":
                return self._json({"tasks": list_tasks(self.state.db)})
            if path == "/api/sources":
                return self._json(self._sources())
            if path == "/api/kline":
                subj = qs.get("subject", [""])[0]
                period = int(qs.get("period", ["60"])[0])
                limit = int(qs.get("limit", ["200"])[0])
                if not subj:
                    return self._json({"error": "subject required"}, 400)
                if period not in (60, 300, 900, 3600, 86400):
                    return self._json({"error": "period must be 60/300/900/3600/86400"}, 400)
                return self._json({
                    "subject": subj, "period": period,
                    **self.state.pipeline.kline_data(subj, period, limit),
                })
            if path == "/api/compare":
                subs = [x for x in qs.get("subjects", [""])[0].split(",") if x]
                period = int(qs.get("period", ["300"])[0])
                limit = int(qs.get("limit", ["200"])[0])
                if not subs:
                    return self._json({"error": "subjects required"}, 400)
                return self._json(self.state.pipeline.compare(subs, period, limit))
            if path == "/api/quotes":
                win = int(qs.get("window", ["1800"])[0])
                return self._json({"items": self.state.pipeline.quotes(window_s=win),
                                   "now": now_ms()})
            if path == "/api/stats":
                return self._json(self._stats())
            if path == "/api/_debug/tasks":
                # 诊断用：pipeline 内存里的任务表 vs 数据库里的任务表
                mem = self.state.pipeline.tasks()
                db_tasks = list_tasks(self.state.db)
                return self._json({
                    "in_memory": [{"id": x["id"], "enabled": x["enabled"],
                                   "subjects": x["config"].get("subjects"),
                                   "sources": x["config"].get("sources"),
                                   "rules": len(x["config"].get("rules") or [])} for x in mem],
                    "in_db": [{"id": x["id"], "enabled": x["enabled"]} for x in db_tasks],
                })
            if path == "/api/suppressions":
                return self._json({"items": self.state.pipeline.decider.suppression_summary()})
            if path == "/api/events":
                return self._json(self._events(qs))
            if path == "/api/config":
                return self._json(self.state.cfg.as_dict())
            if path == "/api/replays":
                return self._json({"items": self.state.db.query(
                    "SELECT * FROM replays ORDER BY created_at DESC LIMIT 50")})
            if path == "/stream":
                return self._sse()
            return self._json({"error": "not found", "path": path}, 404)
        except BrokenPipeError:
            pass
        except Exception:
            traceback.print_exc()
            try:
                self._json({"error": "internal"}, 500)
            except Exception:
                pass

    def do_POST(self):
        try:
            u = urlparse(self.path)
            path = u.path
            body = self._read_body()
            if path == "/api/tasks":
                task = upsert_task(self.state.db, body)
                self.state.pipeline.reload_tasks()
                return self._json({"ok": True, "task": task})
            if path.startswith("/api/tasks/") and path.endswith("/toggle"):
                tid = path[len("/api/tasks/"):-len("/toggle")]
                task = toggle_task(self.state.db, tid, bool(body.get("enabled")))
                self.state.pipeline.reload_tasks()
                return self._json({"ok": bool(task), "task": task})
            if path.startswith("/api/tasks/") and path.endswith("/delete"):
                tid = path[len("/api/tasks/"):-len("/delete")]
                delete_task(self.state.db, tid)
                self.state.pipeline.reload_tasks()
                return self._json({"ok": True})
            if path == "/api/replay":
                res = self.state.pipeline.replay_compare(
                    body["task_id"], int(body["from"]), int(body["to"]),
                    body.get("new_config"))
                return self._json(res)
            if path == "/api/config":
                for k, v in (body or {}).items():
                    self.state.cfg.set([k], v)
                return self._json({"ok": True, "config": self.state.cfg.as_dict()})
            return self._json({"error": "not found"}, 404)
        except BrokenPipeError:
            pass
        except Exception as e:
            traceback.print_exc()
            try:
                self._json({"error": str(e)}, 500)
            except Exception:
                pass

    # ---------- 静态文件 ----------
    def _static(self, rel: str):
        safe = os.path.normpath(rel).lstrip("\\/")
        full = os.path.join(STATIC_DIR, safe)
        if not os.path.abspath(full).startswith(os.path.abspath(STATIC_DIR)):
            return self._json({"error": "forbidden"}, 403)
        if not os.path.isfile(full):
            return self._json({"error": "not found"}, 404)
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
            ctype += "; charset=utf-8"
        with open(full, "rb") as f:
            body = f.read()
        self._send(200, body, ctype)

    # ---------- 数据组装 ----------
    def _health(self):
        hb = self.state.heartbeat.snapshot()
        bus = self.state.bus.stats_snapshot()
        return {
            "ok": True,
            "now": now_ms(),
            "started_at": self.state.started_at,
            "uptime_s": int((now_ms() - self.state.started_at) / 1000),
            "data_mode": "LIVE",
            "heartbeat": hb,
            "bus": bus,
            "pipeline": self.state.pipeline.snapshot(),
        }

    def _alerts(self, qs):
        limit = int(qs.get("limit", ["50"])[0])
        sev = qs.get("severity", [None])[0]
        status = qs.get("status", [None])[0]
        sql = "SELECT * FROM alerts WHERE 1=1"
        params = []
        if sev:
            sql += " AND severity=?"
            params.append(sev)
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self.state.db.query(sql, tuple(params))
        for r in rows:
            try:
                r["payload"] = json.loads(r["payload"]) if r["payload"] else {}
            except Exception:
                r["payload"] = {}
        return {"items": rows, "count": len(rows)}

    def _sources(self):
        out = []
        for name, c in self.state.collectors.items():
            out.append({
                "source": name,
                "enabled": c.enabled,
                "interval_s": c.interval,
                "last_success": c._last_success,
                "last_error": c._last_error,
                "failures": c._failures,
                "latency_ms": c._last_latency_ms,
                "event_count": c._count,
                "state": self.state.heartbeat._state.get(name, "PENDING"),
            })
        return {"items": out}

    def _stats(self):
        db = self.state.db
        total = db.query_one("SELECT COUNT(*) c FROM events")["c"]
        alerts = db.query_one("SELECT COUNT(*) c FROM alerts")["c"]
        by_sev = {r["severity"]: r["c"] for r in db.query(
            "SELECT severity, COUNT(*) c FROM alerts GROUP BY severity")}
        return {
            "events_total": total,
            "alerts_total": alerts,
            "alerts_by_severity": by_sev,
            "pipeline": self.state.pipeline.snapshot(),
            "heartbeat": self.state.heartbeat.snapshot(),
        }

    def _events(self, qs):
        limit = int(qs.get("limit", ["100"])[0])
        subject = qs.get("subject", [None])[0]
        sql = "SELECT seq,source,kind,subject,event_time,ingest_time,payload FROM events"
        params = []
        if subject:
            sql += " WHERE subject=?"
            params.append(subject)
        sql += " ORDER BY seq DESC LIMIT ?"
        params.append(limit)
        rows = self.state.db.query(sql, tuple(params))
        for r in rows:
            try:
                r["payload"] = json.loads(r["payload"])
            except Exception:
                pass
        return {"items": rows}

    # ---------- SSE ----------
    def _sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        cid, q = self.state.pipeline.hub.register()
        last_id = self.headers.get("Last-Event-ID")
        try:
            # 幂等：断线重连时先补发该客户端漏掉的事件（按 Last-Event-ID）
            for msg in self.state.pipeline.hub.since(last_id):
                self._sse_write(msg)
            self._sse_write({"type": "hello", "event_id": f"hello-{cid}",
                             "data": {"client": cid, "server_time": now_ms()}})
            self.wfile.flush()
            last_ping = time.time()
            while True:
                try:
                    msg = q.get(timeout=1.0)
                    self._sse_write(msg)
                except queue.Empty:
                    if time.time() - last_ping > 15:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        last_ping = time.time()
                else:
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            self.state.pipeline.hub.unregister(cid)

    def _sse_write(self, msg: dict):
        eid = msg.get("event_id") or ""
        payload = json.dumps(msg, ensure_ascii=False, default=str)
        chunk = f"id: {eid}\nevent: {msg.get('type','message')}\ndata: {payload}\n\n"
        self.wfile.write(chunk.encode("utf-8"))


def make_server(state: AppState) -> ThreadingHTTPServer:
    Handler.state = state
    host = state.cfg.get("server", "host", default="127.0.0.1")
    port = int(state.cfg.get("server", "port", default=8848))
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    return httpd