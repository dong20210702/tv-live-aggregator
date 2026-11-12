#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直播源聚合 & 测速生成器（仅用标准库）

逻辑还原自「影视仓接口助手 V3.0」的 com.ysc.lineverify.LiveEngine，流程如下：

  1. 读取 sources/seeds.json：每个 seed 是一组镜像地址，按顺序尝试（gh-proxy 镜像在前，直连在后）
  2. 并发抓取所有 seed -> 解析 M3U / TXT -> 按 URL 去重（最多 MAX_ENTRIES 条）
  3. 并发探测每条：
       播放列表 GET（超时 2s，记录 TTFB）
       -> 若是主播放列表（含 #EXT-X-STREAM-INF）：取**中位档**变体，得到码率与分辨率
       -> 取第一个分片，用 Range 拉一小段（默认 256KB）测 TTFB 与**吞吐**，
          吞吐显著低于码率时标 bw_ok=False（仍保留，只降序，不直接判死）
  4. 计算 score，按频道归类到 8 个分组；排序以**延时优先**，
     并保证候选多样性（至少留一条低码率快源）
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
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
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
SEGMENT_TIMEOUT = 4.0        # 分片探测（要读一小段算吞吐），钳制到 [2.0, 5.0]
SEGMENT_MIN, SEGMENT_MAX = 2.0, 5.0
# 只读 1KB 能证明「分片存在」，证明不了「带宽撑得住码率」——这是首包很快、播放却卡的
# 根因。改成拉一小段并算吞吐，顺带把 throughput_kbps 输出给客户端做选路参考。
THROUGHPUT_SAMPLE = 128 * 1024      # 采样量上限
THROUGHPUT_WINDOW = 1.5             # 采样时间窗（秒）：到点就停，避免慢源拖垮总预算
THROUGHPUT_MIN_BYTES = 24 * 1024    # 低于此视为没测出来（记 0，不参与判定）
# 吞吐探测会真占下行带宽，并发太高会互相抢带宽、把好源误判成「带宽不足」
THROUGHPUT_CONCURRENCY = 48
# 带宽判定的余量：吞吐要到码率的 1.15 倍才算「撑得住」
BANDWIDTH_RATIO = 1.15
# 探测端中位吞吐高于此值就认为「探测机带宽不是瓶颈」，可用绝对判定
PROBER_OK_KBPS = 8000
# 码率未知时（媒体播放列表没有 BANDWIDTH），按清晰度估一个期望码率参与判定
QUALITY_KBPS = {"4K": 8000, "1080P": 4500, "720P": 2500, "480P": 1200, "高清": 2000}
# 多样性门槛：低于此码率的源视为「低带宽友好」，每频道至少留一条
CHEAP_BITRATE = 2500
UA = "okhttp/4.12.0"          # 原版探测使用的 UA
# 原版上限 10000，但那是手机端一屏展示的量级。放到服务端后实测 10000 条探测仅耗
# 135s（预算 900s），而种子总量已到 1.8 万条，旧上限会把后半批种子整组挤掉。
MAX_ENTRIES = 30000
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
# 吞吐探测会占真实下行带宽，并发太高会互相抢带宽、把好源误判成「带宽不足」
_BW_SEM = threading.Semaphore(THROUGHPUT_CONCURRENCY)


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
    seg_ttfb_ms: int = 0     # 分片首包，比播放列表 TTFB 更接近「点开多久出画面」
    throughput: int = 0      # kbps，实测下行吞吐（0 = 未测出）
    bw_ok: bool = True       # 吞吐是否撑得住码率（未知时视为 True，不惩罚）
    res_h: int = 0           # 来自主播放列表的 RESOLUTION 高度
    bitrate: int = 0         # kbps，中位档 BANDWIDTH/1000
    peak_bitrate: int = 0    # kbps，最高档 BANDWIDTH/1000（给客户端做自适应参考）
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
    bytes_read: int = 0
    throughput: int = 0
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


def _throughput_kbps(got, head_len, t_first, t_last, t_start):
    """由「已读字节 / 耗时」估下行吞吐（kbps）。

    优先用首包之后的部分算（更能反映带宽而非 RTT）；样本不足时退回含首包的
    保守估计；都不够就返回 0 表示「没测出来」，避免拿噪声去判死源。
    """
    body, dt = got - head_len, t_last - t_first
    if body >= THROUGHPUT_MIN_BYTES and dt >= 0.03:
        return int(body * 8 / dt / 1000)
    if got >= THROUGHPUT_MIN_BYTES // 2:
        return int(got * 8 / max(t_last - t_start, 1e-3) / 1000)
    return 0


def _seg_fetch(req, t, r):
    """在信号量内做真实网络读取，返回计时与字节数。失败时置 r.dead。"""
    t_start = t_first = t_last = time.monotonic()
    got = head_len = 0
    try:
        resp = urllib.request.urlopen(req, timeout=t, context=_SSL_CTX)
    except urllib.error.HTTPError as e:
        r.status, r.dead, r.error = e.code, True, f"分片 HTTP {e.code}"
        return t_start, t_first, t_last, got, head_len
    except (TimeoutError, socket.timeout):
        r.dead, r.error = True, "分片超时"
        return t_start, t_first, t_last, got, head_len
    except (urllib.error.URLError, OSError, ValueError):
        r.dead, r.error = True, "分片不可达"
        return t_start, t_first, t_last, got, head_len
    try:
        with resp:
            r.status = resp.status
            r.ctype = (resp.headers.get("Content-Type") or "").lower()
            if r.status >= 400:
                r.dead, r.error = True, f"分片 HTTP {r.status}"
                return t_start, t_first, t_last, got, head_len
            head = resp.read(1024)
            t_first = time.monotonic()
            r.ttfb_ms = int((t_first - t_start) * 1000)
            head_len = got = len(head)
            r.body = head
            if not head:
                r.dead, r.error = True, "分片读取失败"
                return t_start, t_first, t_last, got, head_len
            try:
                while got < THROUGHPUT_SAMPLE:
                    if time.monotonic() - t_first > THROUGHPUT_WINDOW:
                        break
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    got += len(chunk)
            except (TimeoutError, socket.timeout):
                pass          # 读不完不影响判定，按已读部分估吞吐
            t_last = time.monotonic()
    except (TimeoutError, socket.timeout):
        t_last = time.monotonic()
    except (urllib.error.URLError, OSError, ValueError):
        t_last = time.monotonic()
    return t_start, t_first, t_last, got, head_len


def probe_segment(url, timeout, allow_private):
    """探测首个分片：既确认可用，也测一小段吞吐。

    旧实现只 Range 读 1KB，只能证明「分片存在」，证明不了「带宽撑得住码率」——
    这正是首包几十毫秒、一放就卡的那类源。

    采样限定在 THROUGHPUT_WINDOW 时间窗内：到点就停，慢源不会把总预算拖爆；
    读不完也不丢，按已收到的部分估，只有真的一字节都读不到才判死。
    计时在拿到信号量之后才起，否则排队时间会被算进首包。
    """
    t = min(max(timeout, SEGMENT_MIN), SEGMENT_MAX)
    r = Resp()
    if not host_allowed(url, allow_private):
        r.dead, r.error = True, "地址不允许"
        return r
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept-Encoding": "identity",
        "Range": f"bytes=0-{THROUGHPUT_SAMPLE - 1}",
    })
    with _BW_SEM:
        t_start, t_first, t_last, got, head_len = _seg_fetch(req, t, r)
    if r.dead:
        return r

    r.bytes_read = got
    r.ok = True
    if r.body[:7] == b"#EXTM3U":
        # 二级播放列表被当成「分片」拉下来了：能访问就算这条源活着，
        # 但别用一个播放列表的长度去折算吞吐。
        r.throughput = 0
        r.error = "分片是二级播放列表"
        return r
    r.throughput = _throughput_kbps(got, head_len, t_first, t_last, t_start)
    if "html" in r.ctype:
        r.dead, r.error = True, "分片返回网页"
    elif r.body[:1] in (b"<", b"{"):
        r.dead, r.error = True, "分片返回网页"
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
    # 头部也要清：上游常见「,,宁夏卫视」这种双逗号写法，只清尾部会让同一个频道
    # 变成「宁夏卫视」和「,宁夏卫视」两个不同的 bucket。
    s = re.sub(r"^[,;:、，；：。\s]+", "", s).strip()
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
    s = re.sub(r"^[-_·|/\\,;:、，；：。]+", "", s).strip()
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
    """主播放列表 -> (中位档 kbps, 中位档 URI, 最高档 kbps, 最高档高度)。

    旧实现取 BANDWIDTH **最大**的变体，而码率在 score 里是加分项——等于系统性
    把最重的那条流顶到最前面，国内窄带上就是卡顿的直接来源。

    现在两个用途分开取：
      - **探测/判定**用中位档（可持续的那个），顺便下钻它才能验证真能不能播；
      - **清晰度标注**用最高档（频道真实上限），否则 4K 频道会被标成 480P。
    """
    lines = text.split("\n")
    variants = []
    for i, raw in enumerate(lines):
        line = raw.strip()
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        m_bw = BW_RE.search(line)
        bw = int(m_bw.group(1)) if m_bw else 0
        m_res = RES_RE.search(line)
        h = int(m_res.group(2)) if m_res else 0
        uri = ""
        for nxt in lines[i + 1:]:
            nxt = nxt.strip()
            if not nxt:
                continue
            if nxt.startswith("#"):
                break
            uri = nxt
            break
        variants.append((bw // 1000, h, uri))
    if not variants:
        return None
    usable = [v for v in variants if v[0] > 0] or variants
    usable.sort(key=lambda v: v[0])
    mid = usable[len(usable) // 2]
    top = usable[-1]
    return mid[0], mid[2], top[0], top[1]


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


def expected_kbps(e):
    """判定用的期望码率：优先用实测变体码率，其次按清晰度估一个。"""
    return e.bitrate or QUALITY_KBPS.get(e.quality, 0)


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
    is_master = "#EXT-X-STREAM-INF" in text
    variant_uri = ""
    # 变体要在分片探测之前解析：带宽判定需要先有码率
    if is_master:
        v = best_variant(text)
        if v:
            mid_kbps, mid_uri, top_kbps, top_h = v
            e.bitrate = mid_kbps          # 判定 / 打分用可持续档
            e.peak_bitrate = top_kbps
            e.res_h = top_h               # 清晰度标注用真实上限
            variant_uri = mid_uri
            e.quality = quality_of(e)
    e.first_seg = first_segment_url(e.url, text) or ""
    if not e.first_seg and is_master:
        # 主播放列表本身没有分片（旧实现到这一步就直接判「可用」了，是假通过）。
        # 下钻一层到中位变体的媒体播放列表，再取它的首个分片。
        if variant_uri:
            vurl = urllib.parse.urljoin(e.url, variant_uri)
            r2 = http_get(vurl, PLAYLIST_TIMEOUT, PLAYLIST_MAX_BYTES, allow_private)
            if r2.ok and not r2.binary and r2.text:
                e.first_seg = first_segment_url(vurl, r2.text) or ""
    if not e.first_seg and not is_master:
        e.ok = False
        e.note = "播放列表无分片"
        return
    if not e.first_seg:
        e.note = "主列表未取到分片"      # 保留，但标明未做分片级验证
        return
    s = probe_segment(e.first_seg, SEGMENT_TIMEOUT, allow_private)
    if s.dead:
        e.ok = False
        e.note = s.error or "分片不可用"
        return
    e.seg_ttfb_ms = s.ttfb_ms
    e.throughput = s.throughput
    e.quality = quality_of(e)


def score(e):
    """原版 LiveEngine.score：25 + 40*速度 + 35*(0.6*码率项 + 0.4*分辨率项)，四舍五入。

    速度项用**分片首包**（seg_ttfb_ms），而不是播放列表 TTFB。原版只有播放列表
    可测，所以用 ttfb_ms；现在已经有分片级数据了，还拿列表响应时间当速度，
    会把「列表秒回、分片却要 2 秒」的源评成高分——这正是 CCTV-13 首选源
    2091ms 却排第一的原因。列表 TTFB 只在没有分片数据时兜底。
    """
    if not e.ok:
        return 0
    speed_ms = e.seg_ttfb_ms or e.ttfb_ms
    speed = 1.0 - min(max(speed_ms, 0), 3000) / 3000.0
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


# 这些名字各地都有，同名未必同台，仍需靠分组区分，避免把不同城市的源并到一起
GENERIC_NAMES = {
    "新闻综合", "综合", "公共", "公共频道", "影视", "电影", "体育", "少儿",
    "都市", "生活", "新闻", "综合频道", "经济", "科教", "文旅", "纪实",
}


def channel_key(cat, display):
    """频道主键。

    旧实现用 (分组, 名称) 做 key，而分组是由各源自己的 group-title 推出来的，
    于是「上虞新闻综合」在 A 列表落「地方频道」、在 B 列表落「其他」，被劈成两个
    频道、各带 1 条源。改为默认只认名称，分组在聚合时按多数票决定。
    """
    if display in GENERIC_NAMES or len(display) <= 3:
        return (cat, display)
    return ("", display)


def allocate_entries(results, max_entries):
    """按 seed 公平分配探测名额，返回 (entries, 去重总数, 丢弃数)。

    旧实现按 seed 顺序先到先得，一碰到上限就整组 break。实测 6 号种子
    (CCSH/IPTV) 一家贡献 6964 条，累计到 7 号就卡满 10000，后面的 8~15 号种子
    共 8011 条（44%）从未进入探测——不是没源，是没测。

    现在：先给每个 seed 一个保底名额，剩余名额在所有 seed 之间轮转回填，
    单个大列表再也不可能把其他列表挤死。
    """
    seen, queues = set(), []
    for lst in results:
        q = []
        for e in lst:
            if e.url in seen:
                continue
            seen.add(e.url)
            q.append(e)
        queues.append(q)

    total = sum(len(q) for q in queues)
    if total <= max_entries:
        return [e for q in queues for e in q], total, 0

    n = len(queues)
    floor = max(1, max_entries // (n * 4))      # 每组的保底份额
    entries, leftover = [], []
    for q in queues:
        entries.extend(q[:floor])
        leftover.append(q[floor:])

    ptr = [0] * n
    while len(entries) < max_entries:
        progressed = False
        for i in range(n):
            if ptr[i] < len(leftover[i]):
                entries.append(leftover[i][ptr[i]])
                ptr[i] += 1
                progressed = True
                if len(entries) >= max_entries:
                    break
        if not progressed:
            break
    return entries, total, total - len(entries)


def apply_bandwidth_judgement(entries):
    """按实测吞吐给源打 bw_ok 标记，返回被降序的条数。

    难点在于「测出来的慢」可能是源站慢，也可能是探测机自己的带宽不够（家用宽带
    常见只有 1~10Mbps，128 并发时更是互相抢带宽）。所以先看探测端本身够不够快：

      - 探测端中位吞吐 >= PROBER_OK_KBPS：绝对判定可信，吞吐低于码率×1.15 就算不足；
      - 否则：只抓明显比别人慢的（低于中位数的 1/3），避免整轮结果被探测端拖累。

    无论哪种，bw_ok=False 只是降序、不判死——「能播但会卡」总比「频道直接消失」好。
    """
    vals = sorted(e.throughput for e in entries if e.ok and e.throughput > 0)
    if len(vals) < 30:
        return 0
    med = vals[len(vals) // 2]
    prober_fast = med >= PROBER_OK_KBPS
    slow_line = max(300, med // 3)
    n = 0
    for e in entries:
        if not e.ok or not e.throughput:
            continue
        want = expected_kbps(e)
        if not want or e.throughput >= want * BANDWIDTH_RATIO:
            continue
        if prober_fast or e.throughput < slow_line:
            e.bw_ok = False
            e.note = f"带宽不足 {e.throughput}kbps < 码率 {want}kbps"
            n += 1
    return n


def rank_key(s):
    """排序：带宽达标优先 -> 综合评分高 -> 分片延时低。

    试过纯延时优先，结果是把结果全推成 480P（首条源 480P 从 318 涨到 1235，
    720P/1080P 都在掉）——卡顿治了，清晰度塌了，用户不会满意。
    改用 score：它本身就是「延时 40 + 码率/分辨率 35」的原版加权公式，两头都照顾；
    而「挑到带不动的源」这个问题已经由 bw_ok 在源头解决了，不需要靠排序兜。
    """
    return (0 if s.bw_ok else 1,
            -s.score,
            s.seg_ttfb_ms or s.ttfb_ms or 99999)


def pick_sources(srcs, limit):
    """排序取前 N 条，并保证候选多样性。"""
    srcs.sort(key=rank_key)
    if len(srcs) <= limit:
        return srcs
    picked = srcs[:limit]
    # 若入选的全是高码率源，换掉最后一条，保证至少留一条低码率快源：
    # 弱网下它往往是唯一能播的那条，也是客户端选路的兜底
    has_cheap = any(s.bitrate and s.bitrate <= CHEAP_BITRATE for s in picked)
    if not has_cheap:
        cheap = [s for s in srcs if s.bitrate and s.bitrate <= CHEAP_BITRATE]
        if cheap:
            picked = picked[:-1] + [cheap[0]]
    return picked


def build_channels(entries, per_channel):
    buckets, names = {}, {}
    for e in entries:
        if not e.ok:
            continue
        e.category = classify(e.name, e.group)
        bn = base_name(e.name)
        display = canonical_cctv(bn) or bn      # 「CCTV-1 综合」统一显示为 CCTV-1
        key = channel_key(e.category, display)
        names[key] = display
        buckets.setdefault(key, []).append(e)
    channels = []
    for key, srcs in buckets.items():
        # 分组取多数票，但「其他」是兜底桶、不算有效表态：否则一个大列表里没写
        # group-title 的源会把「XXX体育」从体育频道投进「其他」。
        cnt = Counter(x.category for x in srcs if x.category != "其他")
        if not cnt:
            cnt = Counter({"其他": 1})
        cat = min(cnt, key=lambda c: (-cnt[c], GROUPS.index(c) if c in GROUPS else 99))
        name = names[key]
        channels.append({"group": cat, "name": name,
                         "sources": pick_sources(srcs, per_channel)})
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
                "url": s.url, "quality": s.quality,
                "ttfb_ms": s.ttfb_ms, "seg_ttfb_ms": s.seg_ttfb_ms,
                "throughput_kbps": s.throughput, "bandwidth_ok": s.bw_ok,
                "bitrate_kbps": s.bitrate, "peak_bitrate_kbps": s.peak_bitrate,
                "height": s.res_h, "score": s.score,
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

    entries, entries_total, dropped = allocate_entries(results, args.max_entries)
    log(f"== 去重后 {entries_total} 条，取 {len(entries)} 条探测，"
        f"丢弃 {dropped} 条，列表成功 {lists_ok}/{len(seeds)}")
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
    demoted = apply_bandwidth_judgement(entries)
    ok_entries = [e for e in entries if e.ok]
    log(f"== 可用 {len(ok_entries)} / 已探测 {sum(1 for e in entries if e.probed)}"
        f"，带宽不足降序 {demoted}")

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
        "entries_unique": entries_total,
        "entries_probed": len(entries),
        "entries_dropped": dropped,
        "entries_ok": len(ok_entries),
        "channels": len(channels),
        "per_channel_limit": args.per_channel,
    }
    write_outputs(channels, meta, args.out)
    log(f"== 完成：{len(channels)} 个频道，写入 {args.out}/live.m3u|live.txt|live.json")


if __name__ == "__main__":
    main()
