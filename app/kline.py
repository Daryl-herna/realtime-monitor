"""标准 K 线聚合：把行情快照按周期聚合成 OHLCV，并计算均线。

为什么要自己聚合：
公开免费源不提供可靠的实时 K 线（东财/腾讯分钟线接口经常限流），
但我们本来就在高频采集快照，用快照聚合成 K 线是纯计算，不依赖新数据源。

K 线周期用「事件时间」对齐（不是处理时间），与流计算层保持一致。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .config import now_ms

# 周期（秒）-> 名称
PERIODS = {60: "1m", 300: "5m", 900: "15m", 3600: "1h", 86400: "1d"}


@dataclass
class Bar:
    period: int
    start: int              # 桶起始时间（毫秒，按周期对齐）
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    count: int = 1
    last_ts: int = 0
    # 上一笔的「累计成交量」。行情接口的 volume 是当日累计值，不是增量，
    # 直接累加会得到天文数字（实测出现 3.2 万亿）。必须做差分。
    prev_cum_volume: float | None = None
    volume_from_cumulative: bool = False   # 该桶的 volume 是否来自累计量差分

    def to_dict(self) -> dict:
        return {"t": self.start, "o": round(self.open, 4), "h": round(self.high, 4),
                "l": round(self.low, 4), "c": round(self.close, 4),
                "v": self.volume, "n": self.count}

    def update(self, price: float, cum_volume: float, ts: int):
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        # 累计量差分：本次累计 - 上次累计 = 本段增量
        if cum_volume:
            self.volume_from_cumulative = True
            if self.prev_cum_volume is None:
                self.volume += cum_volume          # 桶内第一笔：从开盘算起的累计量
            elif cum_volume >= self.prev_cum_volume:
                self.volume += cum_volume - self.prev_cum_volume
            # 若本次累计量变小（跨日/数据源重置），忽略这一笔的增量
            self.prev_cum_volume = cum_volume
        self.count += 1
        self.last_ts = ts


class KLineStore:
    """按标的 + 周期维护 K 线序列。

    * 桶起点 = floor(event_time / period) * period，保证同一时刻的数据进同一个桶。
    * 只保留最近 MAX_BARS 根，内存占用可控。
    * 线程安全：采集线程写入，HTTP 线程读取。
    """

    MAX_BARS = 500

    def __init__(self, periods=None):
        self.periods = periods or list(PERIODS.keys())
        self._data: dict[tuple[str, int], list[Bar]] = {}
        self._lock = threading.Lock()
        self.stats = {"updates": 0, "bars_created": 0}

    def update(self, subject: str, price: float, event_time: int, volume: float = 0.0,
               is_cumulative: bool = True):
        """写入一个数据点。

        is_cumulative=True  : volume 是「当日累计成交量」（实时快照），做差分
        is_cumulative=False : volume 是「该周期内的增量」（历史 K 线回填），直接使用
        """
        if price is None:
            return
        with self._lock:
            for p in self.periods:
                start = (event_time // (p * 1000)) * (p * 1000)
                key = (subject, p)
                bars = self._data.get(key)
                if bars is None:
                    bars = []
                    self._data[key] = bars
                if bars and bars[-1].start == start:
                    if is_cumulative:
                        bars[-1].update(price, volume, event_time)
                    else:
                        # 回填增量数据：若该桶已有实时累计量写入，不要覆盖，
                        # 两者口径不同（累计额 vs 分钟量），混用会算出天文数字。
                        bars[-1].high = max(bars[-1].high, price)
                        bars[-1].low = min(bars[-1].low, price)
                        bars[-1].close = price
                        if not bars[-1].volume_from_cumulative:
                            bars[-1].volume = volume
                        bars[-1].count += 1
                        bars[-1].last_ts = event_time
                else:
                    if is_cumulative:
                        prev_cum = bars[-1].prev_cum_volume if bars else None
                        bar = Bar(period=p, start=start, open=price, high=price,
                                  low=price, close=price, volume=0.0,
                                  last_ts=event_time, prev_cum_volume=prev_cum)
                        bar.update(price, volume, event_time)
                    else:
                        bar = Bar(period=p, start=start, open=price, high=price,
                                  low=price, close=price, volume=volume,
                                  last_ts=event_time)
                    bars.append(bar)
                    self.stats["bars_created"] += 1
                    if len(bars) > self.MAX_BARS:
                        del bars[:len(bars) - self.MAX_BARS]
            self.stats["updates"] += 1

    def bars(self, subject: str, period: int, limit: int = 200) -> list[dict]:
        with self._lock:
            bars = self._data.get((subject, period), [])
            return [b.to_dict() for b in bars[-limit:]]

    def seed_from_events(self, subject: str, rows: list[dict]):
        """用历史事件预填充 K 线（重启后曲线不至于空）。"""
        for r in rows:
            try:
                price = r.get("price")
                if price is None:
                    continue
                self.update(subject, float(price), int(r["event_time"]),
                            float(r.get("volume") or 0))
            except (TypeError, ValueError, KeyError):
                continue

    def snapshot(self) -> dict:
        with self._lock:
            return {"series": len(self._data),
                    "bars": sum(len(v) for v in self._data.values()), **self.stats}


def moving_average(bars: list[dict], window: int) -> list[float | None]:
    """简单移动平均。返回与 bars 等长的数组，前 window-1 个为 None。"""
    out: list[float | None] = []
    s = 0.0
    for i, b in enumerate(bars):
        s += b["c"]
        if i >= window:
            s -= bars[i - window]["c"]
        out.append(round(s / window, 4) if i >= window - 1 else None)
    return out


def enrich_with_ma(bars: list[dict], windows=(5, 10, 20)) -> dict:
    """把 K 线数组转成前端要的结构，附带多条均线。"""
    return {
        "bars": bars,
        "ma": {f"ma{w}": moving_average(bars, w) for w in windows},
    }

def ema(values: list[float], period: int) -> list[float | None]:
    """指数移动平均。首值用第一个价格，之后递推。"""
    out: list[float | None] = []
    k = 2.0 / (period + 1)
    prev = None
    for v in values:
        if prev is None:
            prev = v
        else:
            prev = v * k + prev * (1 - k)
        out.append(round(prev, 4))
    return out


def macd(bars: list[dict], fast=12, slow=26, signal=9) -> dict:
    """MACD：DIF、DEA、柱状图（MACD = 2*(DIF-DEA)）。"""
    closes = [b["c"] for b in bars]
    if len(closes) < 2:
        return {"dif": [], "dea": [], "hist": []}
    ef = ema(closes, fast)
    es = ema(closes, slow)
    dif = [round(a - b, 4) if a is not None and b is not None else None
           for a, b in zip(ef, es)]
    dea = ema([d if d is not None else 0 for d in dif], signal)
    hist = [round(2 * (d - e), 4) if d is not None and e is not None else None
            for d, e in zip(dif, dea)]
    return {"dif": dif, "dea": dea, "hist": hist}


def kdj(bars: list[dict], n=9, m1=3, m2=3) -> dict:
    """KDJ：RSV -> K -> D -> J。"""
    if not bars:
        return {"k": [], "d": [], "j": []}
    k_list, d_list, j_list = [], [], []
    k_prev, d_prev = 50.0, 50.0
    for i, b in enumerate(bars):
        lo = min(x["l"] for x in bars[max(0, i - n + 1):i + 1])
        hi = max(x["h"] for x in bars[max(0, i - n + 1):i + 1])
        rsv = (b["c"] - lo) / (hi - lo) * 100 if hi > lo else 50.0
        k_cur = (m1 - 1) / m1 * k_prev + 1 / m1 * rsv
        d_cur = (m2 - 1) / m2 * d_prev + 1 / m2 * k_cur
        j_cur = 3 * k_cur - 2 * d_cur
        k_list.append(round(k_cur, 2))
        d_list.append(round(d_cur, 2))
        j_list.append(round(j_cur, 2))
        k_prev, d_prev = k_cur, d_cur
    return {"k": k_list, "d": d_list, "j": j_list}


def boll(bars: list[dict], n=20, k=2.0) -> dict:
    """布林带：中轨=MA(n)，上下轨=中轨±k*标准差。"""
    if not bars:
        return {"mid": [], "up": [], "low": []}
    closes = [b["c"] for b in bars]
    mid, up, low = [], [], []
    for i in range(len(closes)):
        if i < n - 1:
            mid.append(None); up.append(None); low.append(None)
            continue
        window = closes[i - n + 1:i + 1]
        m = sum(window) / n
        var = sum((x - m) ** 2 for x in window) / n
        sd = var ** 0.5
        mid.append(round(m, 4))
        up.append(round(m + k * sd, 4))
        low.append(round(m - k * sd, 4))
    return {"mid": mid, "up": up, "low": low}


def volume_ma(bars: list[dict], windows=(5, 10)) -> dict:
    """成交量均线：对成交量序列做简单移动平均。"""
    vols = [{"c": b.get("v") or 0} for b in bars]
    return {f"vma{w}": moving_average(vols, w) for w in windows}


def enrich_with_indicators(bars: list[dict], ma_windows=(5, 10, 20)) -> dict:
    """K 线 + 全部指标，供前端一次拿全。"""
    return {
        "bars": bars,
        "ma": {f"ma{w}": moving_average(bars, w) for w in ma_windows},
        "macd": macd(bars),
        "kdj": kdj(bars),
        "boll": boll(bars),
        "vma": volume_ma(bars),
    }