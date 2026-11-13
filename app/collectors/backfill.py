"""历史 K 线回填：用公开的历史接口补齐走势数据。

用途：
1. 新加入的标的，当天没有历史，曲线是平的
2. 已收盘的品种（A股 15:00 后），无法再采到新数据，但历史 K 线可以补

数据源（均实测可用）：
  A股/ETF  新浪 CN_MarketDataService.getKLineData  scale=1/5  -> 200 根
  期货     新浪 InnerFuturesNewService.getFewMinLine  type=1/5 -> 1023 根
"""
from __future__ import annotations

import json
import re

from ..bus import Event, make_event
from ..config import now_ms
from .base import BaseCollector, http_get, local_date_ms, parse_float


class HistoryBackfill(BaseCollector):
    """启动时跑一次，把历史 K 线写入事件表（kind=quote）。

    已存在的点由 event_id 幂等去重，重复执行不会产生脏数据。
    """
    name = "backfill"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.subjects = self.cfg.get("watchlist", "backfill", default=[])

    def fetch(self) -> list[Event]:
        events = []
        for subj in self.subjects:
            if not subj.get("enabled", True):
                continue
            sym = subj["symbol"]
            klass = subj.get("class", "index")
            name = subj.get("name", sym)
            days = int(subj.get("days", 2))
            try:
                if klass == "futures":
                    events += self._futures(sym, name, days)
                else:
                    events += self._equity(sym, name, days)
            except Exception:
                continue
        return events

    @staticmethod
    def _jsonp(raw: str):
        m = re.search(r"\((\[.*\])\)", raw, re.S)
        if not m:
            return []
        try:
            return json.loads(m.group(1))
        except ValueError:
            return []

    def _futures(self, sym: str, name: str, days: int) -> list[Event]:
        """新浪期货 1 分钟线（1023 根，约 3 个交易日）。"""
        code = sym.split("_", 1)[1] if "_" in sym else sym
        url = (f"https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
               f"var%20_{code}=/InnerFuturesNewService.getFewMinLine?symbol={code}&type=1")
        rows = self._jsonp(http_get(url, referer="https://finance.sina.com.cn", timeout=25))
        cutoff = now_ms() - days * 86400000
        events = []
        for r in rows:
            d = r.get("d") or ""
            if len(d) < 19:
                continue
            try:
                ts = local_date_ms(d[0:10], d[11:19])
            except Exception:
                continue
            if ts < cutoff:
                continue
            c = parse_float(r.get("c"))
            if c is None:
                continue
            payload = {
                "symbol": sym, "name": name, "price": c,
                "open": parse_float(r.get("o")), "high": parse_float(r.get("h")),
                "low": parse_float(r.get("l")), "volume": parse_float(r.get("v")),
                "source_time": d, "class": "futures", "backfilled": True,
            }
            events.append(make_event("history", "quote", sym, ts, payload, dedup_key=d))
        return events

    def _equity(self, sym: str, name: str, days: int) -> list[Event]:
        """A股/ETF：新浪 1 分钟线（200 根）。"""
        url = (f"https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_{sym}_1=/"
               f"CN_MarketDataService.getKLineData?symbol={sym}&scale=1&ma=no&datalen=200")
        rows = self._jsonp(http_get(url, referer="https://finance.sina.com.cn", timeout=25))
        cutoff = now_ms() - days * 86400000
        events = []
        for r in rows:
            d = r.get("day") or ""
            if len(d) < 16:
                continue
            try:
                ts = local_date_ms(d[0:10], d[11:19])
            except Exception:
                continue
            if ts < cutoff:
                continue
            c = parse_float(r.get("close"))
            if c is None:
                continue
            payload = {
                "symbol": sym, "name": name, "price": c,
                "open": parse_float(r.get("open")), "high": parse_float(r.get("high")),
                "low": parse_float(r.get("low")), "volume": parse_float(r.get("volume")),
                "source_time": d, "class": "index", "backfilled": True,
            }
            events.append(make_event("history", "quote", sym, ts, payload, dedup_key=d))
        return events
