"""信息采集器：RSS / 滚动新闻 / 公告 / 政府网站。统一产出 kind=news|notice 事件。"""
from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

from ..bus import Event, make_event
from ..config import now_ms
from .base import BaseCollector, HttpError, http_get, local_date_ms, parse_int

CST = timezone(timedelta(hours=8))


def _hash(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


class EastmoneyRssCollector(BaseCollector):
    """东方财富 RSS —— 财经资讯。RSS 自带 pubDate，可直接作为 event_time。"""
    name = "em_rss"
    referer = "https://www.eastmoney.com/"
    url = "https://rss.eastmoney.com/rss_partener.xml"

    def fetch(self) -> list[Event]:
        raw = http_get(self.url, referer=self.referer, timeout=20)
        root = ET.fromstring(raw.encode("utf-8"))
        events = []
        for item in root.findall(".//item"):
            title = _norm(item.findtext("title") or "")
            link = (item.findtext("link") or "").strip()
            desc = _norm(item.findtext("description") or "")
            pub = item.findtext("pubDate") or ""
            try:
                dt = parsedate_to_datetime(pub)
                ts = int(dt.timestamp() * 1000)
            except Exception:
                ts = now_ms()
            if not title:
                continue
            content_hash = _hash(title, desc)
            payload = {
                "title": title, "link": link, "summary": desc[:500],
                "content_hash": content_hash, "pub_date": pub,
                "channel": "eastmoney-rss",
            }
            events.append(make_event(self.name, "news", "em_rss:finance", ts, payload,
                                     dedup_key=content_hash))
        return events


class SinaRollCollector(BaseCollector):
    """新浪财经滚动新闻 —— JSON，ctime 为秒级时间戳。"""
    name = "sina_roll"
    referer = "https://finance.sina.com.cn/"
    url = ("https://feed.mix.sina.com.cn/api/roll/get?"
           "pageid=153&lid=2516&k=&num=30&page=1")

    def fetch(self) -> list[Event]:
        raw = http_get(self.url, referer=self.referer, timeout=20)
        data = json.loads(raw)
        items = ((data.get("result") or {}).get("data")) or []
        events = []
        for it in items:
            title = _norm(it.get("title") or "")
            if not title:
                continue
            link = it.get("url") or ""
            ts = parse_int(it.get("ctime")) * 1000 or now_ms()
            summary = _norm(it.get("intro") or "")[:500]
            content_hash = _hash(title, summary)
            payload = {
                "title": title, "link": link, "summary": summary,
                "content_hash": content_hash, "channel": "sina-roll",
                "media": it.get("media_name") or "",
            }
            events.append(make_event(self.name, "news", "sina_roll:finance", ts, payload,
                                     dedup_key=content_hash))
        return events


class EastmoneyNoticeCollector(BaseCollector):
    """东方财富 A 股公告 —— JSON，含 art_code / title / notice_date。"""
    name = "em_notice"
    referer = "https://data.eastmoney.com/"
    url = ("https://np-anotice-stock.eastmoney.com/api/security/ann?"
           "sr=-1&page_size=30&page_index=1&ann_type=A&client_source=web")

    @staticmethod
    def _parse_notice_time(it: dict) -> int:
        """解析公告时间。

        坑：东财 notice_date 常常只有日期（形如 "2026-11-13 00:00:00"），
        它表示「该公告属于 11-13 这一批」，不是「11-13 零点发布」。
        若直接当精确时刻，会产出未来时间戳，进而让水位线跳到未来、
        把后续真实数据全判成迟到，并让延迟指标变成负数。
        处理：时间是 00:00:00 的一律视为「当日批次」，用当前时间兜底；
        其余情况若解析结果比现在晚，也退回当前时间。
        """
        ts_raw = it.get("notice_date") or it.get("display_time") or ""
        try:
            dt = datetime.strptime(str(ts_raw)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)
        except Exception:
            return now_ms()
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            return now_ms()
        ts = int(dt.timestamp() * 1000)
        return min(ts, now_ms())

    def fetch(self) -> list[Event]:
        raw = http_get(self.url, referer=self.referer, timeout=20)
        data = json.loads(raw)
        items = ((data.get("data") or {}).get("list")) or []
        events = []
        for it in items:
            title = _norm(it.get("title") or "")
            if not title:
                continue
            code = (it.get("codes") or [{}])[0]
            stock = code.get("short_name") or code.get("stock_code") or ""
            ts = self._parse_notice_time(it)
            art = it.get("art_code") or ""
            content_hash = _hash(art, title)
            payload = {
                "title": title, "link": f"https://data.eastmoney.com/notices/detail/{code.get('stock_code','')}/{art}.html",
                "summary": "", "content_hash": content_hash, "channel": "eastmoney-notice",
                "stock": stock, "stock_code": code.get("stock_code") or "", "art_code": art,
            }
            events.append(make_event(self.name, "notice", "em_notice:A", ts, payload,
                                     dedup_key=content_hash))
        return events


class PbcCollector(BaseCollector):
    """中国人民银行·沟通交流 —— HTML 列表页，抓标题+链接。

    页面结构：<a href="/goutongjiaoliu/113456/113469/2026111116292271519/index.html">标题</a>
    列表页不含发布时间，从 URL 前 14 位解析（yyyyMMddHHmmss）。
    """
    name = "pbc"
    referer = "https://www.pbc.gov.cn/"
    url = "https://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html"

    PATTERN = re.compile(
        r'<a[^>]+href="([^"]*?/goutongjiaoliu/113456/113469/(\d{14,})/index\.html)"[^>]*>\s*([^<]{4,120}?)\s*</a>')

    def fetch(self) -> list[Event]:
        html = http_get(self.url, referer=self.referer, timeout=20)
        events = []
        for href, stamp, title in self.PATTERN.findall(html):
            title = _norm(title)
            if not title:
                continue
            try:
                ts = int(datetime.strptime(stamp[:14], "%Y%m%d%H%M%S").replace(tzinfo=CST).timestamp() * 1000)
            except Exception:
                ts = now_ms()
            url = href if href.startswith("http") else "https://www.pbc.gov.cn" + href
            content_hash = _hash(title)
            payload = {
                "title": title, "link": url, "summary": "",
                "content_hash": content_hash, "channel": "pbc",
            }
            events.append(make_event(self.name, "news", "pbc:monetary", ts, payload,
                                     dedup_key=content_hash))
        return events