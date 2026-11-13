#!/usr/bin/env python
"""CI 自检脚本：不启动服务，只验证代码可导入、配置可解析、指标可计算。

设计原则：不依赖网络、不写文件、3 秒内跑完。
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def check_imports():
    from app.main import build  # noqa: F401
    print("[OK] 模块导入")


def check_config():
    from app.config import Config
    cfg = Config(os.path.join(ROOT, "config.json"))
    port = cfg.get("server", "port")
    assert isinstance(port, int) and 0 < port < 65536, f"端口非法: {port}"
    print(f"[OK] 配置解析，端口 {port}")


def check_indicators():
    from app.kline import boll, kdj, macd, moving_average

    bars = [
        {"t": i * 60000, "o": 100 + i, "h": 102 + i, "l": 98 + i,
         "c": 100 + i, "v": 1000, "n": 1}
        for i in range(30)
    ]
    assert len(macd(bars)["dif"]) == 30, "MACD 长度不符"
    assert len(kdj(bars)["k"]) == 30, "KDJ 长度不符"
    assert len(boll(bars)["mid"]) == 30, "BOLL 长度不符"
    assert moving_average(bars, 5)[-1] is not None, "MA 计算失败"
    # 边界：空数据不应抛异常
    assert macd([])["dif"] == [], "空数据 MACD 应返回空"
    assert kdj([])["k"] == [], "空数据 KDJ 应返回空"
    print("[OK] 指标计算")


def check_kline_aggregation():
    from app.kline import KLineStore

    store = KLineStore(periods=[60])
    base = 1794449640000
    # 同一分钟内多个快照应聚合为一根
    for i, cum in enumerate([100, 200, 350, 500]):
        store.update("TEST", 10.0 + i * 0.1, base + i * 10000, cum)
    bars = store.bars("TEST", 60)
    assert len(bars) == 1, f"应聚合成 1 根，实际 {len(bars)}"
    bar = bars[0]
    assert bar["o"] == 10.0, f"开盘价错误: {bar['o']}"
    assert abs(bar["c"] - 10.3) < 1e-9, f"收盘价错误: {bar['c']}"
    assert bar["h"] == 10.3, f"最高价错误: {bar['h']}"
    print("[OK] K 线聚合")


def check_rules():
    from app.rules.engine import RuleEngine
    from app.stream import WindowStore

    engine = RuleEngine(WindowStore())
    task = {
        "id": "t", "name": "test", "type": "price", "enabled": True,
        "config": {
            "subjects": ["A"],
            "rules": [{"id": "r", "type": "change_pct", "threshold": 1.0,
                       "severity": "P2", "enabled": True}],
        },
    }
    from app.bus import make_event

    ev = make_event("test", "quote", "A", 1794449640000,
                    {"price": 102.0, "prev_close": 100.0, "name": "A"}, "k1")
    hits = engine.evaluate(ev, [task])
    assert len(hits) == 1, f"应命中 1 条，实际 {len(hits)}"
    assert hits[0].severity == "P2"
    # 作用域过滤：不同标的的任务不应命中
    other = dict(task, config={**task["config"], "subjects": ["B"]})
    assert engine.evaluate(ev, [other]) == [], "作用域过滤失效"
    print("[OK] 规则引擎")


def main():
    checks = [check_imports, check_config, check_indicators,
              check_kline_aggregation, check_rules]
    failed = 0
    for fn in checks:
        try:
            fn()
        except Exception as exc:
            print(f"[FAIL] {fn.__name__}: {type(exc).__name__}: {exc}")
            failed += 1
    print(f"\n{len(checks) - failed}/{len(checks)} 项通过")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
