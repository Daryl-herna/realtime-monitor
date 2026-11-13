"""行情采集器：新浪 / 腾讯 / 东财分钟K线（基线）。全部走真实公开接口。"""
from __future__ import annotations

import json

from ..bus import Event, make_event
from ..config import now_ms
from .base import BaseCollector, HttpError, http_get, local_date_ms, parse_float, parse_int
from datetime import datetime, timedelta


# 美东时区（含夏令时判断：3月第二个周日 ~ 11月第一个周日）
def _us_eastern_offset_hours(dt_utc_guess: datetime) -> int:
    """返回美东相对 UTC 的小时偏移（-5 标准时 / -4 夏令时）。"""
    y = dt_utc_guess.year
    # 3 月第二个周日
    mar1 = datetime(y, 3, 1)
    dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)
    # 11 月第一个周日
    nov1 = datetime(y, 11, 1)
    dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    d = dt_utc_guess
    if dst_start <= d < dst_end:
        return -4
    return -5


def parse_tencent_time(raw: str, sym: str) -> int:
    """解析腾讯行情的时间字段，统一成 UTC 毫秒时间戳。

    三种格式：
      A股/深市  20261112161412         -> 北京时间
      港股      2026/11/12 18:31:05   -> 北京时间
      美股      2026-11-11 16:43:58   -> **美东时间**

    美股必须按美东解析：直接当北京时间会多算 13 小时，
    界面上就会出现「28 小时未更新」这种荒谬值。
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    raw = (raw or "").strip()
    if not raw:
        return now_ms()

    def _to_ms(local_dt, tz_hours: int) -> int:
        # local_dt 是「某时区的墙上时间」，减去偏移得到 UTC
        utc_dt = local_dt - _td(hours=tz_hours)
        return int(utc_dt.replace(tzinfo=_tz.utc).timestamp() * 1000)

    try:
        if len(raw) == 14 and raw.isdigit():
            d = _dt.strptime(raw, "%Y%m%d%H%M%S")
            return _to_ms(d, 8)          # 北京时间
        norm = raw.replace("/", "-")
        if len(norm) >= 19:
            d = _dt.strptime(norm[:19], "%Y-%m-%d %H:%M:%S")
            if sym.startswith("us."):
                off = _us_eastern_offset_hours(d)   # 美东偏移：-5 或 -4
                return _to_ms(d, off)               # UTC = 墙上时间 - (-5) = +5 小时
            return _to_ms(d, 8)          # 港股 = 北京时间
    except Exception:
        pass
    return now_ms()



class SinaQuoteCollector(BaseCollector):
    """新浪 hq.sinajs.cn —— A股/指数/商品期货/外汇。

    必须带 Referer: https://finance.sina.com.cn，否则返回 403。
    字段按品种不同，分别解析：
      指数(shXXXXXX/szXXXXXX): 0名称 1今开 2昨收 3现价 4最高 5最低 8成交量 9成交额 30日期 31时间
      商品期货(nf_XXXX)      : 0名称 1时间(HHMMSS) 2开 3高 4低 5? 6买 7卖 8最新 13成交量 14持仓 17日期
      外汇(fx_XXXXXX)        : 0时间 1买 2卖 3? 5? 8? 17日期
    """
    name = "sina"
    referer = "https://finance.sina.com.cn"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.subjects = self.cfg.get("watchlist", "sina", default=[])

    def _symbols(self) -> list[str]:
        return [s["symbol"] for s in self.subjects if s.get("enabled", True)]

    def fetch(self) -> list[Event]:
        syms = self._symbols()
        if not syms:
            return []
        url = "https://hq.sinajs.cn/list=" + ",".join(syms)
        text = http_get(url, referer=self.referer, encoding="gbk")
        events = []
        meta = {s["symbol"]: s for s in self.subjects}
        for line in text.split("\n"):
            line = line.strip()
            if not line.startswith("var hq_str_"):
                continue
            sym = line.split("=", 1)[0].replace("var hq_str_", "").strip()
            if '="' not in line:
                continue
            body = line.split('="', 1)[1].rstrip('";')
            if not body:
                continue
            fields = body.split(",")
            m = meta.get(sym, {})
            try:
                ev = self._parse(sym, fields, m)
            except Exception:
                continue
            if ev:
                events.append(ev)
        return events

    def _parse(self, sym: str, f: list[str], meta: dict):
        klass = meta.get("class", "index")
        if klass == "futures":
            if len(f) < 18:
                return None
            price = parse_float(f[8])
            prev = parse_float(f[2]) or price
            ts = local_date_ms(f[17], f[1][:2] + ":" + f[1][2:4] + ":" + f[1][4:6] if len(f[1]) >= 6 else "00:00:00")
            payload = {
                "symbol": sym, "name": f[0], "price": price, "prev_close": prev,
                "open": parse_float(f[2]), "high": parse_float(f[3]), "low": parse_float(f[4]),
                "volume": parse_float(f[13]), "open_interest": parse_float(f[14]),
                "source_time": f"{f[17]} {f[1]}", "class": klass,
            }
        elif klass == "global":
            # 外盘（伦敦金/白银、纽约原油、布伦特原油），14~15 字段：
            # 0现价 1? 2买 3卖 4最高 5最低 6时间 7昨收 8开盘 12日期 13名称
            # 这类品种 24 小时交易，是夜间唯一真正在跳动的行情。
            if len(f) < 14:
                return None
            price = parse_float(f[0])
            prev = parse_float(f[7]) or price
            ts = local_date_ms(f[12], f[6] if ":" in f[6] else "00:00:00")
            payload = {
                "symbol": sym, "name": f[13] if len(f) > 13 else sym,
                "price": price, "prev_close": prev,
                "open": parse_float(f[8]) if len(f) > 8 else None,
                "high": parse_float(f[4]), "low": parse_float(f[5]),
                "source_time": f"{f[12]} {f[6]}", "class": klass,
            }
        elif klass == "fx":
            if len(f) < 18:
                return None
            price = parse_float(f[1])
            prev = parse_float(f[3]) or price
            ts = local_date_ms(f[17], f[0] if ":" in f[0] else "00:00:00")
            payload = {
                "symbol": sym, "name": f[9] or sym, "price": price, "prev_close": prev,
                "bid": parse_float(f[1]), "ask": parse_float(f[2]),
                "source_time": f"{f[17]} {f[0]}", "class": klass,
            }
        else:  # index / stock
            if len(f) < 32:
                return None
            price = parse_float(f[3])
            prev = parse_float(f[2]) or price
            ts = local_date_ms(f[30], f[31])
            payload = {
                "symbol": sym, "name": f[0], "price": price, "prev_close": prev,
                "open": parse_float(f[1]), "high": parse_float(f[4]), "low": parse_float(f[5]),
                "volume": parse_float(f[8]), "amount": parse_float(f[9]),
                "source_time": f"{f[30]} {f[31]}", "class": klass,
            }
        if payload.get("price") is None:
            return None
        payload["change_pct"] = round((payload["price"] / prev - 1) * 100, 4) if prev else None
        # dedup_key 只用源时间：同一时刻的快照即同一条数据，
        # 与价格无关（价格变了但源时间没变，说明源还没刷新，不该算新事件）。
        return make_event(self.name, "quote", sym, ts, payload,
                          dedup_key=str(payload.get("source_time")))


class TencentQuoteCollector(BaseCollector):
    """腾讯 qt.gtimg.cn —— A股/港股/美股，作为新浪的交叉校验源。

    字段（~ 分隔，实测 88 段）：1名称 2代码 3现价 4昨收 5今开 6成交量 30时间(yyyyMMddHHmmss)
    31涨跌额 32涨跌幅 33最高 34最低
    """
    name = "tencent"
    referer = "https://gu.qq.com/"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.subjects = self.cfg.get("watchlist", "tencent", default=[])

    def fetch(self) -> list[Event]:
        syms = [s["symbol"] for s in self.subjects if s.get("enabled", True)]
        if not syms:
            return []
        text = http_get("https://qt.gtimg.cn/q=" + ",".join(syms), referer=self.referer, encoding="gbk")
        events = []
        for line in text.split("\n"):
            line = line.strip()
            if not line.startswith("v_") or '="' not in line:
                continue
            sym = line.split("=", 1)[0][2:].strip()
            body = line.split('="', 1)[1].rstrip('";')
            f = body.split("~")
            if len(f) < 35:
                continue
            price = parse_float(f[3])
            prev = parse_float(f[4])
            if price is None:
                continue
            ts = parse_tencent_time(f[30] if len(f) > 30 else "", sym)
            payload = {
                "symbol": sym, "name": f[1], "price": price, "prev_close": prev,
                "open": parse_float(f[5]), "high": parse_float(f[33]), "low": parse_float(f[34]),
                "volume": parse_float(f[6]), "change": parse_float(f[31]),
                "change_pct": parse_float(f[32]), "source_time": f[30],
            }
            # 去重键只用「源时间」：同一时刻的快照就是同一条数据。
            # 若把价格也放进去，时区修正/字段微调会导致同一份数据算出不同
            # event_id，从而重复入库、重复告警。
            events.append(make_event(self.name, "quote", sym, ts, payload,
                                     dedup_key=str(f[30])))
        return events


class BaselineCollector(BaseCollector):
    """日线基线采集：为「突破 N 日高低点」与「成交量放大」提供基准。

    提供者降级链（本机实测）：
      1) 东方财富 push2his  —— 首选，但实测被限流 RemoteDisconnected
      2) 腾讯 ifzq.gtimg.cn —— 备选，实测稳定
    降级时 health 里 last_error 会记录原因，界面显示 DEGRADED。
    """
    name = "em_kline"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.subjects = self.cfg.get("watchlist", "em_kline", default=[])
        self._last_provider = None

    @staticmethod
    def to_secid(symbol: str) -> str | None:
        """把 sh000001 / sz399001 转成东财 secid（1.000001 / 0.399001）。"""
        s = symbol.lower()
        if s.startswith("sh"):
            return "1." + s[2:]
        if s.startswith("sz"):
            return "0." + s[2:]
        if "." in s:
            return s
        return None

    @staticmethod
    def to_tencent(symbol: str) -> str | None:
        s = symbol.lower()
        if s.startswith(("sh", "sz", "hk", "us")):
            return s
        return None

    def _from_eastmoney(self, secid: str, days: int):
        real = self.to_secid(secid)
        if not real:
            raise ValueError(f"东财不支持该标的: {secid}")
        url = (f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
               f"secid={real}&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55,f56,f57"
               f"&klt=101&fqt=1&end=20500101&lmt={days + 1}")
        data = json.loads(http_get(url, referer="https://quote.eastmoney.com/"))
        d = data.get("data") or {}
        rows = []
        for k in (d.get("klines") or []):
            p = k.split(",")
            rows.append({"date": p[0], "open": float(p[1]), "close": float(p[2]),
                         "high": float(p[3]), "low": float(p[4]), "volume": float(p[5])})
        return rows, d.get("name", secid)

    def _from_tencent(self, secid: str, days: int):
        sym = self.to_tencent(secid)
        if not sym:
            raise ValueError(f"腾讯不支持该标的: {secid}")
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
               f"param={sym},day,,,{days + 1},qfq")
        data = json.loads(http_get(url, referer="https://gu.qq.com/"))
        node = ((data.get("data") or {}).get(sym)) or {}
        raw = node.get("qfqday") or node.get("day") or []
        rows = []
        for p in raw:
            # 腾讯格式: [date, open, close, high, low, volume]
            rows.append({"date": p[0], "open": float(p[1]), "close": float(p[2]),
                         "high": float(p[3]), "low": float(p[4]),
                         "volume": float(p[5]) if len(p) > 5 else 0.0})
        name = secid
        qt = node.get("qt")
        if isinstance(qt, dict):
            arr = qt.get(sym)
            if isinstance(arr, list) and len(arr) > 1:
                name = arr[1]
        return rows, name

    def fetch(self) -> list[Event]:
        events = []
        errors = []
        for subj in self.subjects:
            if not subj.get("enabled", True):
                continue
            secid = subj["symbol"]
            days = int(subj.get("days", 20))
            rows, name = None, subj.get("name", secid)
            for provider, fn in (("eastmoney", self._from_eastmoney), ("tencent", self._from_tencent)):
                try:
                    rows, name = fn(secid, days)
                    if rows:
                        self._last_provider = provider
                        break
                except Exception as exc:
                    errors.append(f"{provider}:{type(exc).__name__}")
                    rows = None
            if not rows or len(rows) < 2:
                continue
            # 最后一根可能是当日未收盘，基线只用其之前 days 根
            hist = rows[:-1][-days:]
            if not hist:
                continue
            base_high = max(r["high"] for r in hist)
            base_low = min(r["low"] for r in hist)
            base_close = rows[-2]["close"]
            avg_vol = sum(r["volume"] for r in hist) / len(hist) if hist else 0.0
            payload = {
                "symbol": secid, "name": name, "days": days,
                "n_high": base_high, "n_low": base_low, "prev_close": base_close,
                "avg_volume": avg_vol, "bars": len(hist),
                "provider": self._last_provider,
            }
            events.append(make_event(self.name, "baseline", secid, now_ms(), payload,
                                     dedup_key=f"{base_high}|{base_low}|{len(hist)}|{self._last_provider}"))
        if errors and not events:
            raise RuntimeError("全部基线提供者失败: " + ",".join(errors))
        if errors:
            self._last_error = "降级: " + ",".join(errors)
        return events