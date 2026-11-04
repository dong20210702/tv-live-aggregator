#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直播源聚合 & 测速生成器（仅用标准库）

逻辑还原自「影视仓接口助手 V3.0」的 com.ysc.lineverify.LiveEngine，流程如下：

  1. 读取 sources/seeds.json：每个 seed 是一组镜像地址，按顺序尝试（gh-proxy 镜像在前，直连在后）
  2. 并发抓取所有 seed -> 解析 M3U / TXT -> 按 URL 去重（最多 MAX_ENTRIES 条）
  3. 并发探测每条：
       播放列表 GET（超时 2s，记录 TTFB）
       -> 若是媒体播放列表：取第一个分片，用 Range: bytes=0-1023 探测（超时 1.5~4s）
       -> 若是主播放列表（含 #EXT-X-STREAM-INF）：取 BANDWIDTH 最大的变体，得到码率与分辨率
  4. 计算 score，按频道归类到 8 个分组，每个频道保留前 N 条
  5. 输出 output/live.m3u、output/live.txt、output/live.json

用法：
  python3 scripts/build_live.py --seeds sources/seeds.json --out output
  python3 scripts/build_live.py --allow-private --seeds test/seeds.json --out /tmp/x   # 仅本地联调
"""

import argparse
import concurrent.futures as cf
import ipaddress
import json
import math
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# 常量（数值取自原版字节码）
# ---------------------------------------------------------------------------
LIST_TIMEOUT = 35.0          # 列表抓取超时（原版 Http.fetch(url, 35000)）
LIST_ATTEMPTS = 2            # 每个镜像最多尝试 2 次
LIST_MAX_BYTES = 8 * 1024 * 1024
PLAYLIST_TIMEOUT = 2.0       # 探测播放列表（原版 fetch(url, 2000)）
PLAYLIST_MAX_BYTES = 2 * 1024 * 1024
SEGMENT_TIMEOUT = 3.0        # 探测分片（原版 probeSegment(url, 3000)），钳制到 [1.5, 4.0]
SEGMENT_MIN, SEGMENT_MAX = 1.5, 4.0
SEGMENT_RANGE = "bytes=0-1023"
UA = "okhttp/4.12.0"          # 原版探测使用的 UA
MAX_ENTRIES = 10000          # 原版上限
MAX_PER_CHANNEL = 5          # UI 中单频道最多显示 5 条

DEFAULT_PROBE_WORKERS = 128
DEFAULT_FETCH_WORKERS = 16
DEFAULT_PROBE_BUDGET = 900.0 # 原版探测总预算 900s

GROUPS = ["央视频道", "卫视频道", "地方频道", "影视频道", "体育频道", "少儿频道", "港澳台", "其他"]

STD_CCTV = [f"CCTV-{i}" for i in range(1, 18)]
STD_CCTV.insert(STD_CCTV.index("CCTV-5") + 1, "CCTV-5+")
STD_CCTV.append("CGTN")

STD_WSTV = [
    "北京卫视", "天津卫视", "河北卫视", "山西卫视", "内蒙古卫视", "辽宁卫视", "吉林卫视",
    "黑龙江卫视", "东方卫视", "江苏卫视", "浙江卫视", "安徽卫视", "东南卫视", "江西卫视",
    "山东卫视", "河南卫视", "湖北卫视", "湖南卫视", "广东卫视", "广西卫视", "海南卫视",
    "重庆卫视", "四川卫视", "贵州卫视", "云南卫视", "陕西卫视", "甘肃卫视", "青海卫视",
    "宁夏卫视", "新疆卫视", "西藏卫视", "深圳卫视", "厦门卫视", "延边卫视", "三沙卫视",
    "康巴卫视", "兵团卫视",
]

LOCAL_WORDS = [
    "北京", "天津", "上海", "重庆", "河北", "山西", "辽宁", "吉林", "黑龙江", "江苏", "浙江",
    "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东", "广西", "海南", "四川",
    "贵州", "云南", "陕西", "甘肃", "青海", "宁夏", "新疆", "西藏", "内蒙古", "石家庄", "太原",
    "沈阳", "大连", "长春", "哈尔滨", "南京", "苏州", "无锡", "常州", "徐州", "杭州", "宁波",
    "温州", "金华", "合肥", "福州", "厦门", "泉州", "南昌", "济南", "青岛", "烟台", "潍坊",
    "郑州", "洛阳", "武汉", "长沙", "株洲", "广州", "深圳", "珠海", "佛山", "东莞", "中山",
    "惠州", "汕头", "南宁", "桂林", "海口", "三亚", "成都", "绵阳", "贵阳", "昆明", "西安",
    "兰州", "西宁", "银川", "乌鲁木齐", "拉萨", "呼和浩特", "包头", "唐山", "保定", "邯郸",
]

# 清晰度档位（原版 LiveEngine.rank）
QUALITY_RANK = {"4K": 5, "1080P": 4, "720P": 3, "高清": 2, "480P": 1}

URL_PREFIXES = ("http://", "https://", "rtmp://", "rtsp://", "p3p://", "mitv://")
BW_RE = re.compile(r"BANDWIDTH=(\d+)")
RES_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")

# TLS 使用 urllib 默认上下文（GitHub Runner / 系统 CA 即可）
_SSL_CTX = None


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------
@dataclass
class Entry:
    name: str
    url: str
    group: str = ""          # M3U group-title / TXT 分组标题
    logo: str = ""
    category: str = ""       # 归类后的 8 个分组之一
    probed: bool = False
    ok: bool = False
    note: str = ""
    ttfb_ms: int = 0
    first_seg: str = ""
    res_h: int = 0           # 来自主播放列表的 RESOLUTION 高度
    bitrate: int = 0         # kbps，来自 BANDWIDTH/1000
    quality: str = ""
    score: int = 0


@dataclass
class Resp:
    ok: bool = False
    status: int = 0
    ttfb_ms: int = 0
    ctype: str = ""
    binary: bool = False
    text: str | None = None
    body: bytes = b""
    error: str = ""
    dead: bool = False       # 仅分片探测使用：分片不可用


# ---------------------------------------------------------------------------
# 网络层（对应原版 Http 类）
# ---------------------------------------------------------------------------
def host_allowed(url, allow_private):
    """原版 dnsAllowed/ipAllowed：拒绝解析到内网、回环、链路本地等地址（防 SSRF）。"""
    if allow_private:
        return True
    host = urllib.parse.urlsplit(url).hostname
    if not host:
        return False
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0].split("%")[0])
        except ValueError:
            return False
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified or ip.is_reserved):
            return False
    return True


def is_binary(data):
    if not data:
        return False
    if b"\x00" in data[:512]:
        return True
    sample = data[:512]
    bad = sum(1 for c in sample if c < 9 or (13 < c < 32))
    return bad / len(sample) > 0.1


def decode_bytes(data):
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("gb18030", errors="replace")


def http_get(url, timeout, read_limit, allow_private, headers=None):
    r = Resp()
    if not host_allowed(url, allow_private):
        r.error = "地址不允许"
        return r
    req_headers = {"User-Agent": UA, "Accept-Encoding": "identity"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
            r.ttfb_ms = int((time.monotonic() - t0) * 1000)
            r.status = resp.status
            r.ctype = (resp.headers.get("Content-Type") or "").lower()
            if r.status >= 400:
                r.error = f"HTTP {r.status}"
                return r
            r.body = resp.read(read_limit)
    except urllib.error.HTTPError as e:
        r.status = e.code
        r.error = f"HTTP {e.code}"
        return r
    except (TimeoutError, socket.timeout):
        r.error = "超时"
        return r
    except (urllib.error.URLError, OSError, ValueError):
        r.error = "不可达"
        return r
    r.ok = True
    r.binary = is_binary(r.body)
    if not r.binary:
        r.text = decode_bytes(r.body)
    return r


def probe_segment(url, timeout, allow_private):
    """原版 Http.probeSegment：Range 探测首个分片，返回 Resp.dead 表示不可用。"""
    t = min(max(timeout, SEGMENT_MIN), SEGMENT_MAX)
    r = http_get(url, t, 1024, allow_private, headers={"Range": SEGMENT_RANGE})
    if r.status >= 400:
        r.dead, r.error = True, f"分片 HTTP {r.status}"
    elif not r.ok:
        r.dead, r.error = True, r.error or "分片读取失败"
    elif "html" in r.ctype:
        r.dead, r.error = True, "分片返回网页"
    elif r.body[:1] in (b"<", b"{"):
        r.dead, r.error = True, "分片返回网页"
    elif not r.body:
        r.dead, r.error = True, "分片读取失败"
    return r


# ---------------------------------------------------------------------------
# 解析（对应原版 Live.parse / parseM3u / parseTxt / add / cleanName）
# ---------------------------------------------------------------------------
def looks_url(u):
    return u.lower().startswith(URL_PREFIXES)


def strip_nested_scheme(u):
    m = re.match(r"(?i)^https?://(https?://.*)$", u)
    return m.group(1) if m else u


def prepare_url(u):
    u = u.replace("\u200b", "").replace("\ufeff", "").strip().replace(" ", "%20")
    return strip_nested_scheme(u)


def clean_name(s):
    s = (s or "").replace("\ufeff", "").replace("\u00a0", " ").strip()
    if '="' in s:
        s = re.sub(r'[A-Za-z0-9_-]+\s*=\s*"[^"]*"', "", s).strip()
    s = re.sub(r"[,;\s]+$", "", s).strip()
    return s


def attr(line, key):
    m = re.search(rf'{re.escape(key)}="([^"]*)"', line)
    return m.group(1).strip() if m else ""


def after_comma(line):
    # 先剔除引号内的属性值，避免属性里的逗号干扰
    stripped = re.sub(r'"[^"]*"', '""', line)
    if "," not in stripped:
        return ""
    return stripped.split(",", 1)[1].strip()


def parse_m3u(text):
    out, pending = [], None
    for raw in text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            pending = line
            continue
        if line.startswith("#"):
            continue
        if pending is not None:
            name = clean_name(after_comma(pending) or attr(pending, "tvg-name") or attr(pending, "tvg-id"))
            if name and looks_url(line):
                out.append(Entry(name=name, url=prepare_url(line),
                                 group=attr(pending, "group-title"),
                                 logo=attr(pending, "tvg-logo")))
            pending = None
    return out


def parse_txt(text):
    out, group = [], ""
    for raw in text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if "#genre#" in line:                      # 「分组名,#genre#」或「#genre#」
            group = line[: line.index("#genre#")].replace(",", "").strip()
            continue
        if line.startswith("#"):
            continue
        if "," not in line:
            continue
        name, url = line.split(",", 1)
        name, url = clean_name(name), url.strip()
        if name and looks_url(url):
            out.append(Entry(name=name, url=prepare_url(url), group=group))
    return out


def parse_list(text):
    if "#EXTINF" in text or text.strip().startswith("#EXTM3U"):
        return parse_m3u(text)
    return parse_txt(text)


# ---------------------------------------------------------------------------
# 归类 / 命名（对应原版 Live.classify、LiveEngine.baseName、CCTV 规范化）
# ---------------------------------------------------------------------------
def base_name(name):
    """频道名规范化：去括号注释、去清晰度词、统一 CCTV-N 写法。"""
    s = (name or "").replace("\ufeff", "").replace("\u200b", "").strip()
    s = re.sub(r"[（(\[【][^）)\]】]*[）)\]】]", "", s)
    s = re.sub(r"(?i)\b(hd|fhd|uhd|sd|1080p|720p|480p|2160p|4k|8k)\b", "", s)
    for w in ("高清", "超清", "蓝光", "标清"):
        s = s.replace(w, "")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[-_·|/\\]+$", "", s).strip()
    if s.upper().startswith("CCTV"):
        rest = re.sub(r"^[-_ ]+", "", s[4:].strip())
        m = re.match(r"^(\d+)(.*)$", rest)
        if m:
            s = "CCTV-" + m.group(1) + m.group(2)
            s = re.sub(r"\s+", " ", s).strip()
    return s or (name or "").strip()


def canonical_cctv(bn):
    up = bn.upper().replace(" ", "")
    if up == "CGTN" or up.startswith("CGTN"):
        return "CGTN"
    m = re.match(r"^CCTV-?(\d+)(\+?)", up)
    if m:
        return f"CCTV-{m.group(1)}" + ("+" if m.group(2) else "")
    return None


def by_name_strong(name):
    bn = base_name(name)
    c = canonical_cctv(bn)
    if c and c in STD_CCTV:
        return "央视频道"
    if bn in STD_WSTV:
        return "卫视频道"
    return None


def by_name_weak(name):
    bn = base_name(name)
    if canonical_cctv(bn) or "央视" in name or "CCTV" in name.upper():
        return "央视频道"
    if "卫视" in name:
        return "卫视频道"
    if any(w in name for w in LOCAL_WORDS):
        return "地方频道"
    # 增强（原版未见）：名称里带港澳台字样时归入港澳台
    if any(w in name for w in ("香港", "澳门", "台湾", "港台", "翡翠", "TVB")):
        return "港澳台"
    return "其他"


def classify(name, group_title):
    strong = by_name_strong(name)
    if strong:
        return strong
    t = (group_title or "").strip().lower()
    if not t:
        return by_name_weak(name)
    if "央视" in t or "cctv" in t or "卫视" in t:
        return by_name_weak(name)
    if any(k in t for k in ("地方", "城市", "本地")):
        return "地方频道"
    if any(k in t for k in ("电影", "影视", "剧场", "影院")):
        return "影视频道"
    if any(k in t for k in ("体育", "赛事")):
        return "体育频道"
    if any(k in t for k in ("少儿", "儿童", "卡通", "动画")):
        return "少儿频道"
    if any(k in t for k in ("港澳", "港台", "台湾", "香港", "澳门")):
        return "港澳台"
    return by_name_weak(name)


def natural_key(s):
    return [(0, int(p), "") if p.isdigit() else (1, 0, p.upper())
            for p in re.split(r"(\d+)", s) if p != ""]


# ---------------------------------------------------------------------------
# 探测（对应原版 LiveEngine.probe / firstSegmentUrl / bestVariant）
# ---------------------------------------------------------------------------
def first_segment_url(base, text):
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ".m3u8" in line.lower():
            continue
        return urllib.parse.urljoin(base, line)
    return None


def best_variant(text):
    """主播放列表中 BANDWIDTH 最大的变体 -> (kbps, 高度)。"""
    best_bw, best_h, found = 0, 0, False
    for raw in text.split("\n"):
        line = raw.strip()
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        m_bw = BW_RE.search(line)
        bw = int(m_bw.group(1)) if m_bw else 0
        m_res = RES_RE.search(line)
        h = int(m_res.group(2)) if m_res else 0
        if not found or bw > best_bw:
            best_bw, best_h, found = bw, h, True
    if not found:
        return None
    return best_bw // 1000, best_h


def height_of(quality):
    q = quality or ""
    for key, h in (("4K", 2160), ("1080", 1080), ("720", 720), ("480", 480)):
        if key in q:
            return h
    return 0


def quality_of(e):
    """清晰度：优先实测高度，其次名称关键字，最后按码率估计。"""
    if e.res_h >= 2000:
        return "4K"
    if e.res_h >= 1000:
        return "1080P"
    if e.res_h >= 700:
        return "720P"
    if e.res_h >= 400:
        return "480P"
    n = (e.name or "").lower()
    if "4k" in n or "2160" in n:
        return "4K"
    if "1080" in n or "fhd" in n:
        return "1080P"
    if "720" in n:
        return "720P"
    if "480" in n:
        return "480P"
    if e.bitrate >= 4000:
        return "1080P"
    if e.bitrate >= 2000:
        return "720P"
    if e.bitrate >= 800:
        return "480P"
    if "hd" in n or "高清" in n:
        return "高清"
    return ""


def probe(e, allow_private):
    try:
        _probe(e, allow_private)
    except Exception as ex:  # 单条失败不影响整体
        e.ok = False
        e.note = f"探测异常: {type(ex).__name__}"
    finally:
        e.probed = True


def _probe(e, allow_private):
    e.quality = quality_of(e)
    if not e.url.lower().startswith(("http://", "https://")):
        e.note = "非 http 流"
        return
    r = http_get(e.url, PLAYLIST_TIMEOUT, PLAYLIST_MAX_BYTES, allow_private)
    e.ttfb_ms = r.ttfb_ms
    if not (r.ok and not r.binary and r.text is not None):
        e.note = r.error or "不可达"
        return
    text = r.text
    if "#EXTM3U" not in text and "#EXTINF" not in text:
        e.note = "不是直播流"
        return
    e.ok = True
    e.first_seg = first_segment_url(e.url, text) or ""
    is_master = "#EXT-X-STREAM-INF" in text
    if not e.first_seg and not is_master:
        e.ok = False
        e.note = "播放列表无分片"
        return
    if e.first_seg:
        s = probe_segment(e.first_seg, SEGMENT_TIMEOUT, allow_private)
        if s.dead:
            e.ok = False
            e.note = s.error or "分片不可用"
            return
    if is_master:
        v = best_variant(text)
        if v:
            e.bitrate, e.res_h = v
    e.quality = quality_of(e)


def score(e):
    """原版 LiveEngine.score：25 + 40*速度 + 35*(0.6*码率项 + 0.4*分辨率项)，四舍五入。"""
    if not e.ok:
        return 0
    speed = 1.0 - min(max(e.ttfb_ms, 0), 3000) / 3000.0
    bitrate_term = min(e.bitrate / 12000.0, 1.0) if e.bitrate > 0 else 0.0
    h = e.res_h if e.res_h > 0 else height_of(e.quality)
    res_term = min(h / 1920.0, 1.0) if h > 0 else 0.0
    q = 0.6 * bitrate_term + 0.4 * res_term
    return int(math.floor(25 + 40 * speed + 35 * q + 0.5))


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def fetch_seed(urls, allow_private, log):
    """按顺序尝试镜像；每个地址最多 LIST_ATTEMPTS 次；404 跳过；拿到非空结果即返回。"""
    for url in urls:
        for _ in range(LIST_ATTEMPTS):
            r = http_get(url, LIST_TIMEOUT, LIST_MAX_BYTES, allow_private)
            if r.status == 404:
                continue
            if r.ok and not r.binary and r.text:
                items = parse_list(r.text)
                if items:
                    log(f"  [列表ok] {len(items):>5} 条  {url}")
                    return items
    log(f"  [列表失败] {urls[0]}")
    return []


def build_channels(entries, per_channel):
    buckets = {}
    for e in entries:
        if not e.ok:
            continue
        e.category = classify(e.name, e.group)
        bn = base_name(e.name)
        display = canonical_cctv(bn) or bn      # 「CCTV-1 综合」统一显示为 CCTV-1
        if display in STD_CCTV:
            display = canonical_cctv(bn)
        key = (e.category, display)
        buckets.setdefault(key, []).append(e)
    channels = []
    for (cat, name), srcs in buckets.items():
        srcs.sort(key=lambda x: (-QUALITY_RANK.get(x.quality, 0), -x.score, x.ttfb_ms))
        channels.append({"group": cat, "name": name, "sources": srcs[:per_channel]})
    gidx = {g: i for i, g in enumerate(GROUPS)}
    channels.sort(key=lambda c: (gidx.get(c["group"], 99), natural_key(c["name"])))
    return channels


def write_outputs(channels, meta, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    m3u = ["#EXTM3U"]
    txt = []
    groups_json = {}
    for ch in channels:
        g = ch["group"]
        groups_json.setdefault(g, []).append({
            "name": ch["name"],
            "sources": [{
                "url": s.url, "quality": s.quality, "ttfb_ms": s.ttfb_ms,
                "bitrate_kbps": s.bitrate, "height": s.res_h, "score": s.score,
            } for s in ch["sources"]],
        })
        for s in ch["sources"]:
            m3u.append(f'#EXTINF:-1 tvg-name="{ch["name"]}" group-title="{g}",{ch["name"]}')
            m3u.append(s.url)
    for g in GROUPS:
        if g not in groups_json:
            continue
        txt.append(f"{g},#genre#")
        for ch in groups_json[g]:
            for src in ch["sources"]:
                txt.append(f'{ch["name"]},{src["url"]}')

    with open(os.path.join(out_dir, "live.m3u"), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(m3u) + "\n")
    with open(os.path.join(out_dir, "live.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(txt) + "\n")
    payload = {"meta": meta, "groups": [{"group": g, "channels": groups_json[g]}
                                        for g in GROUPS if g in groups_json]}
    with open(os.path.join(out_dir, "live.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)


def main():
    ap = argparse.ArgumentParser(description="直播源聚合与测速")
    ap.add_argument("--seeds", default="sources/seeds.json")
    ap.add_argument("--out", default="output")
    ap.add_argument("--max-entries", type=int, default=MAX_ENTRIES)
    ap.add_argument("--per-channel", type=int, default=MAX_PER_CHANNEL)
    ap.add_argument("--probe-workers", type=int, default=int(os.environ.get("PROBE_WORKERS", DEFAULT_PROBE_WORKERS)))
    ap.add_argument("--fetch-workers", type=int, default=DEFAULT_FETCH_WORKERS)
    ap.add_argument("--probe-budget", type=float, default=DEFAULT_PROBE_BUDGET)
    ap.add_argument("--min-ok", type=int, default=1, help="可用条目少于该数量时不覆盖输出并退出 1")
    ap.add_argument("--allow-private", action="store_true", help="允许内网地址（仅本地联调用）")
    args = ap.parse_args()

    def log(msg):
        print(msg, flush=True)

    with open(args.seeds, encoding="utf-8") as f:
        seeds = json.load(f)["seeds"]
    t_start = time.monotonic()
    log(f"== 抓取列表：{len(seeds)} 个 seed")

    with cf.ThreadPoolExecutor(max_workers=args.fetch_workers) as ex:
        results = list(ex.map(lambda s: fetch_seed(s, args.allow_private, log), seeds))
    lists_ok = sum(1 for r in results if r)

    entries, seen = [], set()
    for lst in results:
        for e in lst:
            if e.url in seen:
                continue
            seen.add(e.url)
            entries.append(e)
            if len(entries) >= args.max_entries:
                break
        if len(entries) >= args.max_entries:
            break
    log(f"== 去重后 {len(entries)} 条，列表成功 {lists_ok}/{len(seeds)}")
    if not entries:
        log("!! 没有抓到任何条目，保留旧输出，退出")
        sys.exit(1)

    log("== 探测")
    ex = cf.ThreadPoolExecutor(max_workers=args.probe_workers)
    futs = [ex.submit(probe, e, args.allow_private) for e in entries]
    done, not_done = cf.wait(futs, timeout=args.probe_budget)
    for f, e in zip(futs, entries):
        if f in not_done:
            e.probed = True
            e.note = "探测超时"
    ex.shutdown(wait=False, cancel_futures=True)

    for e in entries:
        e.score = score(e)
    ok_entries = [e for e in entries if e.ok]
    log(f"== 可用 {len(ok_entries)} / 已探测 {sum(1 for e in entries if e.probed)}")

    if len(ok_entries) < args.min_ok:
        log("!! 可用条目过少，保留旧输出，退出")
        sys.exit(1)

    channels = build_channels(ok_entries, args.per_channel)
    meta = {
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "elapsed_sec": round(time.monotonic() - t_start, 1),
        "seeds_total": len(seeds),
        "seeds_ok": lists_ok,
        "entries_raw": sum(len(r) for r in results),
        "entries_unique": len(entries),
        "entries_ok": len(ok_entries),
        "channels": len(channels),
        "per_channel_limit": args.per_channel,
    }
    write_outputs(channels, meta, args.out)
    log(f"== 完成：{len(channels)} 个频道，写入 {args.out}/live.m3u|live.txt|live.json")


if __name__ == "__main__":
    main()
