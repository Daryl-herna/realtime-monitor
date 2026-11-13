"""应用入口：装配所有子系统并启动。

用法：
    python run.py                # 用 config.json
    python run.py --config x.json
    python run.py --port 9000
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
import traceback

from .bus import EventBus
from .config import Config, now_ms
from .db import Database
from .heartbeat import Heartbeat
from .pipeline import Pipeline
from .server import AppState, make_server
from .tasks import purge_obsolete_tasks, seed_tasks
from .collectors.quotes import BaselineCollector, SinaQuoteCollector, TencentQuoteCollector
from .collectors.news import (EastmoneyNoticeCollector, EastmoneyRssCollector,
                              PbcCollector, SinaRollCollector)
from .collectors.backfill import HistoryBackfill

COLLECTOR_CLASSES = [
    HistoryBackfill,
    SinaQuoteCollector,
    TencentQuoteCollector,
    BaselineCollector,
    EastmoneyRssCollector,
    SinaRollCollector,
    EastmoneyNoticeCollector,
    PbcCollector,
]


def build(config_path: str, port_override: int | None = None):
    cfg = Config(config_path)
    if port_override:
        cfg.set(["server", "port"], port_override)
    db_path = cfg.get("storage", "db_path", default="data/monitor.db")
    if not os.path.isabs(db_path):
        db_path = os.path.join(os.path.dirname(os.path.abspath(config_path)), db_path)
    db = Database(db_path)
    bus = EventBus(db,
                   poll_interval_ms=int(cfg.get("bus", "poll_interval_ms", default=100)),
                   batch_size=int(cfg.get("bus", "batch_size", default=200)))
    pipeline = Pipeline(cfg, db, bus)
    collectors = {C.name: C(cfg, db, bus) for C in COLLECTOR_CLASSES}
    # 采集器的每轮快照直接喂给看板层（绕过事件去重）
    for c in collectors.values():
        c.on_snapshot = pipeline.set_last_quotes
    heartbeat = Heartbeat(cfg, db, collectors, pipeline)
    purged = purge_obsolete_tasks(db)
    if purged:
        print(f"[迁移] 移除废弃任务 {purged} 条")
    seeded = seed_tasks(db)
    # 关键：Pipeline 构造时任务表还是空的，种子写入后必须重新加载，
    # 否则规则引擎拿到的任务列表永远是空，表现为「一切正常但永不告警」。
    pipeline.reload_tasks()
    return cfg, db, bus, pipeline, collectors, heartbeat, seeded


def main(argv=None):
    ap = argparse.ArgumentParser(description="信息 / 价格异动实时监控")
    ap.add_argument("--config", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json"))
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--no-collect", action="store_true", help="只启动看板，不启动采集（用于离线查看）")
    args = ap.parse_args(argv)

    started_at = now_ms()
    cfg, db, bus, pipeline, collectors, heartbeat, seeded = build(args.config, args.port)

    print(f"[启动] 配置: {cfg.path}")
    print(f"[启动] 数据库: {db.path}")
    if seeded:
        print(f"[启动] 写入默认任务 {seeded} 条")

    pipeline.start()
    bus.start()
    heartbeat.start()

    # 历史回填：启动时跑一次，补齐新标的的当日走势
    if not args.no_collect and "backfill" in collectors:
        bf = collectors["backfill"]
        try:
            evs = bf.fetch()
            n = bus.publish_many(evs)
            print(f"[回填] 历史 K 线 {n} 条（{len(bf.subjects)} 个标的）")
        except Exception as exc:
            print(f"[回填] 失败: {type(exc).__name__}: {exc}")

    if not args.no_collect:
        for name, c in collectors.items():
            if name == "backfill":
                continue
            if c.enabled:
                c.start()
                print(f"[采集] {name} 已启动 interval={c.interval}s")
    else:
        print("[采集] --no-collect：未启动采集器")

    # 预热 K 线：用已落盘的历史事件重建，避免刚启动时图上空白
    if not args.no_collect:
        try:
            warmed = 0
            for subj in pipeline.watched_subjects():
                for period in (60, 300, 900, 3600):
                    pipeline._seed_klines_from_db(subj, period, 300)
                warmed += 1
            print(f"[预热] K 线已用历史事件重建（{warmed} 个标的）")
        except Exception as exc:
            print(f"[预热] 失败: {type(exc).__name__}: {exc}")

    state = AppState(cfg, db, bus, pipeline, heartbeat, collectors, started_at)
    httpd = make_server(state)
    host = cfg.get("server", "host", default="127.0.0.1")
    port = cfg.get("server", "port", default=8848)
    print(f"[看板] http://{host}:{port}/")

    stop = threading.Event()

    def _shutdown(*_):
        if stop.is_set():
            return
        stop.set()
        print("\n[退出] 正在关闭…")
        for c in collectors.values():
            c.stop()
        heartbeat.stop()
        pipeline.stop()
        bus.stop()
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, _shutdown)
    try:
        signal.signal(signal.SIGTERM, _shutdown)
    except (AttributeError, ValueError):
        pass

    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        _shutdown()
    finally:
        try:
            httpd.server_close()
        except Exception:
            pass
    print("[退出] 已停止")
    return 0


if __name__ == "__main__":
    sys.exit(main())