"""配置加载与校验。配置与代码分离：全部可调参数在 config.json。"""
from __future__ import annotations

import json
import os
import threading
import time

DEFAULTS = {
    "server": {"host": "127.0.0.1", "port": 8848},
    "storage": {"db_path": "data/monitor.db", "retention_days": 30},
    "bus": {"poll_interval_ms": 100, "batch_size": 200},
    "stream": {
        "allowed_lateness_ms": 3000,      # 水位线容忍的乱序程度
        "late_policy": "drop",            # drop | recompute
        "window_grace_ms": 2000,          # 窗口关闭前额外等待
    },
    "alerts": {
        "default_cooldown_s": 300,
        "silence_windows": [],            # [{"start":"12:00","end":"13:00","weekdays":[1,2,3,4,5]}]
        "digest_interval_s": 120,         # P3 聚合摘要间隔
        "resolve_after_s": 300,           # 条件恢复多久后发 resolved
        "max_alerts_per_minute": 30,      # 告警风暴熔断
    },
    "heartbeat": {
        "tick_s": 5,
        "stale_factor": 3.0,              # 超过 采集间隔*3 未成功 -> DEGRADED
        "down_factor": 8.0,               # 超过 采集间隔*8 未成功 -> DOWN
        "pipeline_stale_s": 120,          # 计算链路超过该秒数无进展 -> 告警
    },
    "sources": {
        "sina":      {"enabled": True,  "interval_s": 5,   "min_interval_s": 3},
        "tencent":   {"enabled": True,  "interval_s": 5,   "min_interval_s": 3},
        "em_kline":  {"enabled": True,  "interval_s": 300, "min_interval_s": 60},
        "em_rss":    {"enabled": True,  "interval_s": 60,  "min_interval_s": 30},
        "sina_roll": {"enabled": True,  "interval_s": 60,  "min_interval_s": 30},
        "em_notice": {"enabled": True,  "interval_s": 60,  "min_interval_s": 30},
        "pbc":       {"enabled": True,  "interval_s": 300, "min_interval_s": 120},
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class Config:
    """线程安全的配置容器，支持运行时热更新（看板改任务/改频率）。"""

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self._lock = threading.RLock()
        self._data = {}
        self._mtime = 0
        self.load()

    def load(self) -> dict:
        with self._lock:
            raw = {}
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                self._mtime = os.path.getmtime(self.path)
            self._data = _deep_merge(DEFAULTS, raw)
            self._validate()
            return self._data

    def maybe_reload(self) -> bool:
        """文件被外部改动时自动重载。"""
        try:
            if os.path.getmtime(self.path) > self._mtime:
                self.load()
                return True
        except OSError:
            pass
        return False

    def _validate(self):
        s = self._data["server"]
        if not (0 < int(s["port"]) < 65536):
            raise ValueError("server.port 非法")
        for name, src in self._data["sources"].items():
            if float(src["interval_s"]) < float(src["min_interval_s"]):
                raise ValueError(f"数据源 {name} 的 interval_s 小于 min_interval_s")

    def get(self, *keys, default=None):
        with self._lock:
            cur = self._data
            for k in keys:
                if not isinstance(cur, dict) or k not in cur:
                    return default
                cur = cur[k]
            return cur

    def set(self, keys, value):
        with self._lock:
            cur = self._data
            for k in keys[:-1]:
                cur = cur.setdefault(k, {})
            cur[keys[-1]] = value
            self.save()

    def save(self):
        with self._lock:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
            self._mtime = os.path.getmtime(self.path)

    def as_dict(self) -> dict:
        with self._lock:
            return json.loads(json.dumps(self._data, ensure_ascii=False))


def now_ms() -> int:
    return int(time.time() * 1000)