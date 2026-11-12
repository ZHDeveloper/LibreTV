"""Aggregate, validate, and group IPTV channels from ok-sources.json lives list.

Fetches upstream live sources (M3U & TXT), normalizes channel names and
categories, prioritizes television channels, validates stream availability with
async low-latency range requests, deduplicates and ranks streams by latency,
and generates:
  - live/live.m3u: Standard M3U playlist with EPG and logos
  - live/live.txt: TVBox / DIYP genre-grouped text playlist
  - live/stats.json: Aggregation summary and statistics
"""

import asyncio
import json
import logging
import os
import re
import ssl
import sys
import time
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("live_aggregator")

ROOT_DIR = Path(__file__).resolve().parent
LIVE_DIR = ROOT_DIR / "live"
UPSTREAM_CONFIG = LIVE_DIR / "upstream.json"
SOURCES_CONFIG = ROOT_DIR / "ok-sources.json"

DEFAULT_EPG = "http://epg.51zmt.top:8000/api/diyp/?ch={name}&date={date}"
DEFAULT_LOGO_BASE = "https://live.zbds.top/logo"

# Maximum valid streams to retain per channel (sorted by latency)
MAX_STREAMS_PER_CHANNEL = 3
# Maximum stream candidates to test per channel
MAX_CANDIDATES_PER_CHANNEL = 6
# Maximum channels to keep for web streams (Bilibili/Huya/Douyu/YY)
MAX_WEB_STREAM_CHANNELS = 80
# Concurrent validation limit
CONCURRENT_CHECK_LIMIT = 80

# Category display order
CATEGORY_ORDER = [
    "央视频道",
    "卫视频道",
    "超清4K",
    "港澳台",
    "数字影视",
    "体育游戏",
    "少儿动漫",
    "地方综合",
    "网络轮播",
    "综合频道",
]

# CCTV channel mappings: (standard_name, tvg_id, logo_url)
CCTV_NAMES = {
    "1": ("CCTV-1 综合", "CCTV1", f"{DEFAULT_LOGO_BASE}/CCTV1.png"),
    "2": ("CCTV-2 财经", "CCTV2", f"{DEFAULT_LOGO_BASE}/CCTV2.png"),
    "3": ("CCTV-3 综艺", "CCTV3", f"{DEFAULT_LOGO_BASE}/CCTV3.png"),
    "4": ("CCTV-4 中文国际", "CCTV4", f"{DEFAULT_LOGO_BASE}/CCTV4.png"),
    "5": ("CCTV-5 体育", "CCTV5", f"{DEFAULT_LOGO_BASE}/CCTV5.png"),
    "5+": ("CCTV-5+ 体育赛事", "CCTV-5+", f"{DEFAULT_LOGO_BASE}/CCTV5+.png"),
    "6": ("CCTV-6 电影", "CCTV6", f"{DEFAULT_LOGO_BASE}/CCTV6.png"),
    "7": ("CCTV-7 国防军事", "CCTV7", f"{DEFAULT_LOGO_BASE}/CCTV7.png"),
    "8": ("CCTV-8 电视剧", "CCTV8", f"{DEFAULT_LOGO_BASE}/CCTV8.png"),
    "9": ("CCTV-9 纪录", "CCTV9", f"{DEFAULT_LOGO_BASE}/CCTV9.png"),
    "10": ("CCTV-10 科教", "CCTV10", f"{DEFAULT_LOGO_BASE}/CCTV10.png"),
    "11": ("CCTV-11 戏曲", "CCTV11", f"{DEFAULT_LOGO_BASE}/CCTV11.png"),
    "12": ("CCTV-12 社会与法", "CCTV12", f"{DEFAULT_LOGO_BASE}/CCTV12.png"),
    "13": ("CCTV-13 新闻", "CCTV13", f"{DEFAULT_LOGO_BASE}/CCTV13.png"),
    "14": ("CCTV-14 少儿", "CCTV14", f"{DEFAULT_LOGO_BASE}/CCTV14.png"),
    "15": ("CCTV-15 音乐", "CCTV15", f"{DEFAULT_LOGO_BASE}/CCTV15.png"),
    "16": ("CCTV-16 奥林匹克", "CCTV16", f"{DEFAULT_LOGO_BASE}/CCTV16.png"),
    "17": ("CCTV-17 农业农村", "CCTV17", f"{DEFAULT_LOGO_BASE}/CCTV17.png"),
    "4K": ("CCTV-4K 超高清", "CCTV4K", f"{DEFAULT_LOGO_BASE}/CCTV4K.png"),
    "8K": ("CCTV-8K 超高清", "CCTV8K", f"{DEFAULT_LOGO_BASE}/CCTV8K.png"),
}

# Major provincial satellite TV stations
PROVINCIAL_SATELLITES = {
    "北京卫视", "江苏卫视", "东方卫视", "浙江卫视", "湖南卫视", "广东卫视",
    "深圳卫视", "山东卫视", "安徽卫视", "河南卫视", "湖北卫视", "江西卫视",
    "四川卫视", "重庆卫视", "天津卫视", "河北卫视", "辽宁卫视", "黑龙江卫视",
    "吉林卫视", "内蒙古卫视", "山西卫视", "陕西卫视", "甘肃卫视", "宁夏卫视",
    "青海卫视", "新疆卫视", "西藏卫视", "贵州卫视", "云南卫视", "广西卫视",
    "海南卫视", "东南卫视", "厦门卫视", "兵团卫视", "三沙卫视", "大湾区卫视"
}

# Block keywords in channel titles
BLOCK_KEYWORDS = [
    "微信", "qq群", "测试", "失效", "更新", "关注", "防走丢", "公告",
    "购买", "招租", "淘客", "优惠券", "禁止", "备用", "请勿", "成人",
    "激情", "伦理", "福利", "自用", "打赏", "支持作者", "赞助", "捐赠",
    "扫码", "客服", "官网", "下载app", "app下载", "广告", "推广"
]


def clean_raw_name(name: str) -> str:
    """Strip noise tags, resolution suffixes and brackets."""
    name = re.sub(r"\[(?:IPV[46]|1080P|720P|HD|4K|8K|超清|高清|蓝光|普清|流畅|备用|移动|联通|电信)\]", "", name, flags=re.I)
    name = re.sub(r"\((?:1080P|720P|HD|4K|8K|超清|高清|蓝光|普清|流畅|备用|移动|联通|电信)\)", "", name, flags=re.I)
    name = re.sub(r"(?:1080P|720P|FHD|HD|4K|8K|HEVC|50FPS|60FPS|超清|高清|蓝光)$", "", name, flags=re.I)
    name = re.sub(r"[\s_]+", " ", name).strip()
    return name


def normalize_channel(raw_name: str) -> Tuple[str, str, str, str]:
    """Normalize channel to (std_name, category, tvg_id, tvg_logo)."""
    name = clean_raw_name(raw_name)

    # 1. 4K Special Check
    is_4k = bool(re.search(r"(?:4K|8K|超高清)", raw_name, flags=re.I))

    # 2. CCTV pattern
    m_cctv = re.match(r"^CCTV[-_\s]*(\d+|4K|8K)([\+]?)(.*)$", name, flags=re.I)
    if m_cctv:
        num = m_cctv.group(1).upper()
        plus = m_cctv.group(2)
        key = f"{num}{plus}"
        if key in CCTV_NAMES:
            std_name, tvg_id, logo = CCTV_NAMES[key]
            cat = "超清4K" if ("4K" in key or "8K" in key) else "央视频道"
            return std_name, cat, tvg_id, logo
        else:
            std_name = f"CCTV-{key}"
            return std_name, "央视频道", f"CCTV{key}", f"{DEFAULT_LOGO_BASE}/CCTV{key}.png"

    # Other CCTV digital channels
    if any(k in name for k in ["CGTN", "兵器科技", "风云足球", "风云剧场", "风云音乐", "怀旧剧场", "第一剧场", "世界地理", "女性时尚", "央视文化精品", "早期教育"]):
        cat = "央视频道"
        std_name = name
        if "CCTV" in raw_name and not name.startswith("CCTV"):
            std_name = f"CCTV-{name}"
        return std_name, cat, std_name, f"{DEFAULT_LOGO_BASE}/{std_name}.png"

    # 3. Provincial Satellites
    for sat in PROVINCIAL_SATELLITES:
        if sat in name:
            return sat, "卫视频道", sat, f"{DEFAULT_LOGO_BASE}/{sat}.png"
        prov = sat.replace("卫视", "")
        if name.startswith(prov) and "卫视" in name:
            return sat, "卫视频道", sat, f"{DEFAULT_LOGO_BASE}/{sat}.png"

    # 4. 4K Category
    if is_4k:
        return name, "超清4K", name, ""

    # 5. Hong Kong, Macao, Taiwan & Overseas
    hkt_keywords = [
        "凤凰中文", "凤凰资讯", "凤凰香港", "翡翠台", "明珠台", "无线新闻", "J2",
        "星空卫视", "中天", "东森", "TVBS", "民视", "华视", "台视", "澳视", "莲花卫视",
        "三立", "纬来", "Now新闻", "HBO", "CNN", "BBC", "Discovery", "国家地理"
    ]
    for k in hkt_keywords:
        if k in name:
            return name, "港澳台", name, ""

    # 6. Movies & Cinema
    movie_keywords = [
        "CHC", "淘剧场", "淘电影", "NewTV", "iHOT", "经典电影", "动作电影",
        "家庭影院", "动作影院", "喜剧影院", "大片", "影院", "剧场"
    ]
    for k in movie_keywords:
        if k in name:
            return name, "数字影视", name, ""

    # 7. Sports
    sports_keywords = ["体育", "足球", "篮球", "网球", "五星体育", "广东体育", "劲爆体育", "极速汽车"]
    for k in sports_keywords:
        if k in name:
            return name, "体育游戏", name, ""

    # 8. Children & Animation
    child_keywords = ["少儿", "卡通", "动漫", "金鹰卡通", "卡酷", "嘉佳", "优漫", "炫动"]
    for k in child_keywords:
        if k in name:
            return name, "少儿动漫", name, ""

    # 9. Local Stations
    local_keywords = ["新闻综合", "都市", "经济", "公共", "科教", "生活", "地面", "珠江"]
    for k in local_keywords:
        if k in name:
            return name, "地方综合", name, ""

    # 10. Web Streams & Others
    if any(k in name for k in ["一起看", "轮播", "直播", "哔哩", "虎牙", "斗鱼", "YY", "电影", "电视剧"]):
        return name, "网络轮播", name, ""

    return name, "综合频道", name, ""


def should_skip_channel(name: str, url: str) -> bool:
    """Filter out spam, advertisements, and invalid URLs."""
    if not url or not url.startswith(("http://", "https://")):
        return True
    name_lower = name.lower()
    for block in BLOCK_KEYWORDS:
        if block in name_lower:
            return True
    return False


async def fetch_source_content(session: aiohttp.ClientSession, source_info: dict) -> Tuple[str, str]:
    """Fetch live playlist from single source, with proxy & mirror fallback."""
    name = source_info.get("name", "Unknown")
    url = source_info.get("url", "")
    ua = source_info.get("ua", "okhttp/3.15")

    # Generate fallback candidate URLs
    candidate_urls = [url]

    # jsDelivr -> raw.githubusercontent.com fallback
    m_jsd = re.match(r"https?://cdn\.jsdelivr\.net/gh/([^/]+)/([^/@]+)@([^/]+)/(.*)", url)
    if m_jsd:
        user, repo, branch, file_path = m_jsd.groups()
        candidate_urls.append(f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{file_path}")
        candidate_urls.append(f"https://gh-proxy.org/https://raw.githubusercontent.com/{user}/{repo}/{branch}/{file_path}")

    # GitHub proxy / raw fallback
    if "raw.githubusercontent.com" in url:
        if "gh-proxy" in url or "ghproxy" in url or "down.nigx.cn" in url:
            direct_url = re.sub(r"https?://[^/]+/(?:https://)?raw\.githubusercontent\.com", "https://raw.githubusercontent.com", url)
            candidate_urls.insert(0, direct_url)
        else:
            proxy_url = f"https://gh-proxy.org/{url}"
            candidate_urls.append(proxy_url)

    for target_url in candidate_urls:
        try:
            headers = {"User-Agent": ua}
            timeout = aiohttp.ClientTimeout(connect=5, total=12)
            async with session.get(target_url, headers=headers, timeout=timeout, ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    text = None
                    for enc in ("utf-8", "gb18030", "gbk"):
                        try:
                            text = data.decode(enc)
                            break
                        except Exception:
                            pass
                    if text and "<!DOCTYPE html" not in text and "<html" not in text:
                        line_count = len(text.splitlines())
                        logger.info(f"Successfully fetched {name} from {target_url[:45]}... ({line_count} lines)")
                        return name, text
        except Exception as e:
            logger.debug(f"Attempt failed for {name} via {target_url[:40]}: {e}")
            continue

    logger.warning(f"Failed to fetch source: {name} ({url})")
    return name, ""


def parse_playlist(source_name: str, text: str) -> List[Tuple[str, str, str, str, str]]:
    """Parse M3U or TXT content into (raw_name, url, group_title, tvg_id, tvg_logo)."""
    results = []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return results

    # Determine if M3U or TXT
    is_m3u = any("#EXTINF" in line for line in lines[:20])

    if is_m3u:
        cur_name = ""
        cur_group = ""
        cur_id = ""
        cur_logo = ""
        for line in lines:
            if line.startswith("#EXTINF"):
                m_group = re.search(r'group-title="([^"]*)"', line)
                cur_group = m_group.group(1).strip() if m_group else ""

                m_id = re.search(r'tvg-id="([^"]*)"', line)
                cur_id = m_id.group(1).strip() if m_id else ""

                m_logo = re.search(r'tvg-logo="([^"]*)"', line)
                cur_logo = m_logo.group(1).strip() if m_logo else ""

                comma_idx = line.rfind(",")
                if comma_idx != -1:
                    cur_name = line[comma_idx + 1:].strip()
            elif line.startswith(("http://", "https://")):
                if cur_name:
                    results.append((cur_name, line, cur_group, cur_id, cur_logo))
                    cur_name = ""
    else:
        # TXT format
        cur_group = ""
        for line in lines:
            if "#genre#" in line:
                cur_group = line.split(",")[0].strip()
            elif "," in line and not line.startswith("#"):
                parts = line.split(",", 1)
                ch_name = parts[0].strip()
                ch_urls = parts[1].strip()
                for u in ch_urls.split("#"):
                    u = u.strip()
                    if u.startswith(("http://", "https://")):
                        results.append((ch_name, u, cur_group, "", ""))

    return results


async def validate_stream(session: aiohttp.ClientSession, url: str) -> Tuple[bool, int]:
    """Test stream availability and measure response latency (TTFB)."""
    start = time.monotonic()
    headers = {
        "User-Agent": "okhttp/3.15",
        "Range": "bytes=0-2048",
    }
    timeout = aiohttp.ClientTimeout(connect=3.0, total=4.0)
    try:
        async with session.get(url, headers=headers, timeout=timeout, ssl=False) as resp:
            elapsed = int((time.monotonic() - start) * 1000)
            if resp.status in (200, 206):
                chunk = await resp.content.read(512)
                if not chunk:
                    return False, elapsed

                # Discard HTML error responses
                if b"<html" in chunk.lower() or b"<!doctype" in chunk.lower():
                    return False, elapsed

                content_type = resp.headers.get("Content-Type", "").lower()
                is_m3u8_hint = ".m3u8" in url or "mpegurl" in content_type
                if is_m3u8_hint:
                    text_head = chunk.decode("utf-8", errors="ignore")
                    if "#EXTM3U" in text_head or "#EXT-X" in text_head or ".ts" in text_head or ".m3u8" in text_head:
                        return True, elapsed
                    return False, elapsed

                # Binary stream (TS, FLV, MP4)
                if chunk.startswith(b"FLV") or chunk[0] == 0x47 or len(chunk) >= 64:
                    return True, elapsed

                return True, elapsed
            return False, elapsed
    except Exception:
        elapsed = int((time.monotonic() - start) * 1000)
        return False, elapsed


async def validate_channels_pool(
    channel_candidates: Dict[str, List[dict]],
    max_concurrent: int = CONCURRENT_CHECK_LIMIT
) -> Dict[str, List[dict]]:
    """Validate candidate streams with concurrency control and early stopping."""
    semaphore = asyncio.Semaphore(max_concurrent)
    valid_results = defaultdict(list)
    tested_count = 0
    success_count = 0

    connector = aiohttp.TCPConnector(limit=max_concurrent + 15, ssl=False, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        async def check_single_candidate(ch_key: str, candidate: dict):
            nonlocal tested_count, success_count
            async with semaphore:
                if len(valid_results[ch_key]) >= MAX_STREAMS_PER_CHANNEL:
                    return

                tested_count += 1
                ok, latency = await validate_stream(session, candidate["url"])
                if ok:
                    success_count += 1
                    candidate["latency"] = latency
                    valid_results[ch_key].append(candidate)

        tasks = []
        for ch_key, candidates in channel_candidates.items():
            for cand in candidates[:MAX_CANDIDATES_PER_CHANNEL]:
                tasks.append(check_single_candidate(ch_key, cand))

        logger.info(f"Validating {len(tasks)} stream candidates across {len(channel_candidates)} channels (concurrency={max_concurrent})...")
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(f"Stream validation finished: tested {tested_count}, available {success_count} streams.")

    # Sort valid streams by latency and retain Top N
    final_grouped = {}
    for ch_key, streams in valid_results.items():
        if streams:
            streams.sort(key=lambda s: s["latency"])
            final_grouped[ch_key] = streams[:MAX_STREAMS_PER_CHANNEL]

    return final_grouped


def channel_sort_key(category: str, ch_name: str) -> tuple:
    """Sort key for channels within each category."""
    if category == "央视频道":
        m = re.match(r"^CCTV[-_\s]*(\d+|4K|8K)([\+]?)", ch_name, re.I)
        if m:
            num_str = m.group(1).upper()
            plus = 1 if m.group(2) else 0
            if num_str.isdigit():
                return (0, int(num_str), plus, ch_name)
            elif num_str == "4K":
                return (0, 98, plus, ch_name)
            elif num_str == "8K":
                return (0, 99, plus, ch_name)
        return (1, 0, 0, ch_name)
    elif category == "卫视频道":
        priority_satellites = [
            "北京卫视", "江苏卫视", "东方卫视", "浙江卫视", "湖南卫视", "广东卫视",
            "深圳卫视", "山东卫视", "安徽卫视", "河南卫视", "湖北卫视", "江西卫视",
            "四川卫视", "重庆卫视", "天津卫视", "河北卫视", "辽宁卫视", "黑龙江卫视",
            "吉林卫视", "内蒙古卫视", "山西卫视", "陕西卫视", "甘肃卫视", "宁夏卫视",
            "青海卫视", "新疆卫视", "西藏卫视", "贵州卫视", "云南卫视", "广西卫视",
            "海南卫视", "东南卫视", "厦门卫视", "兵团卫视", "三沙卫视", "大湾区卫视"
        ]
        if ch_name in priority_satellites:
            return (0, priority_satellites.index(ch_name), 0, ch_name)
        return (1, 0, 0, ch_name)
    return (0, 0, 0, ch_name)


def generate_playlists(grouped_channels: Dict[str, List[dict]]) -> Tuple[str, str, dict, str]:
    """Generate live.m3u, live.txt, summary stats, and markdown summary."""
    categories = defaultdict(lambda: defaultdict(list))
    for ch_key, streams in grouped_channels.items():
        first = streams[0]
        cat = first["category"]
        std_name = first["name"]
        categories[cat][std_name].extend(streams)

    # 1. Generate live.txt (TVBox / DIYP format)
    txt_lines = []
    for cat in CATEGORY_ORDER:
        ch_dict = categories.get(cat)
        if not ch_dict:
            continue
        txt_lines.append(f"{cat},#genre#")
        # Sort channels nicely
        sorted_names = sorted(ch_dict.keys(), key=lambda n: channel_sort_key(cat, n))
        for ch_name in sorted_names:
            for s in ch_dict[ch_name]:
                txt_lines.append(f"{ch_name},{s['url']}")
        txt_lines.append("")

    for cat, ch_dict in categories.items():
        if cat not in CATEGORY_ORDER and ch_dict:
            txt_lines.append(f"{cat},#genre#")
            sorted_names = sorted(ch_dict.keys(), key=lambda n: channel_sort_key(cat, n))
            for ch_name in sorted_names:
                for s in ch_dict[ch_name]:
                    txt_lines.append(f"{ch_name},{s['url']}")
            txt_lines.append("")

    live_txt_content = "\n".join(txt_lines).strip() + "\n"

    # 2. Generate live.m3u (Standard M3U format with EPG and logos)
    m3u_lines = [
        f'#EXTM3U x-tvg-url="{DEFAULT_EPG}"',
    ]

    channel_count = 0
    stream_count = 0
    category_stats = {}

    for cat in CATEGORY_ORDER:
        ch_dict = categories.get(cat)
        if not ch_dict:
            continue
        sorted_names = sorted(ch_dict.keys(), key=lambda n: channel_sort_key(cat, n))
        cat_ch_count = len(sorted_names)
        cat_stream_count = 0
        for ch_name in sorted_names:
            streams = ch_dict[ch_name]
            channel_count += 1
            for idx, s in enumerate(streams, 1):
                stream_count += 1
                cat_stream_count += 1
                display_title = ch_name if len(streams) == 1 else f"{ch_name} (线路{idx})"
                tvg_id = s.get("tvg_id") or ch_name
                tvg_logo = s.get("tvg_logo") or ""
                m3u_lines.append(
                    f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch_name}" tvg-logo="{tvg_logo}" group-title="{cat}",{display_title}'
                )
                m3u_lines.append(s["url"])

        category_stats[cat] = {
            "channels": cat_ch_count,
            "streams": cat_stream_count,
        }

    for cat, ch_dict in categories.items():
        if cat not in CATEGORY_ORDER and ch_dict:
            sorted_names = sorted(ch_dict.keys(), key=lambda n: channel_sort_key(cat, n))
            cat_ch_count = len(sorted_names)
            cat_stream_count = 0
            for ch_name in sorted_names:
                streams = ch_dict[ch_name]
                channel_count += 1
                for idx, s in enumerate(streams, 1):
                    stream_count += 1
                    cat_stream_count += 1
                    display_title = ch_name if len(streams) == 1 else f"{ch_name} (线路{idx})"
                    tvg_id = s.get("tvg_id") or ch_name
                    tvg_logo = s.get("tvg_logo") or ""
                    m3u_lines.append(
                        f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{ch_name}" tvg-logo="{tvg_logo}" group-title="{cat}",{display_title}'
                    )
                    m3u_lines.append(s["url"])
            category_stats[cat] = {
                "channels": cat_ch_count,
                "streams": cat_stream_count,
            }

    live_m3u_content = "\n".join(m3u_lines) + "\n"

    stats = {
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_channels": channel_count,
        "total_streams": stream_count,
        "categories": category_stats,
    }

    # 3. Generate Markdown Summary Report for GitHub Actions Step Summary
    summary_md = [
        "## 📺 IPTV 直播源自动整合与可用性检测报告",
        "",
        f"- **更新时间**: `{stats['updated_at']}`",
        f"- **有效频道总数**: **{channel_count}** 个",
        f"- **有效流线路总数**: **{stream_count}** 条",
        "",
        "| 分组类别 | 有效频道数 | 可用线路数 | 状态 |",
        "| :--- | :---: | :---: | :---: |",
    ]
    for cat, info in category_stats.items():
        summary_md.append(f"| **{cat}** | {info['channels']} | {info['streams']} | ✅ 正常 |")

    summary_md.extend([
        "",
        "### 🔗 订阅地址",
        "- **加速代理订阅 (推荐国内使用)**:",
        "  - M3U 订阅 (带EPG与Logo): `https://gh-proxy.org/https://raw.githubusercontent.com/ZHDeveloper/tv-sources/main/live/live.m3u`",
        "  - TVBox / DIYP 订阅 (TXT): `https://gh-proxy.org/https://raw.githubusercontent.com/ZHDeveloper/tv-sources/main/live/live.txt`",
        "- **原生 GitHub 订阅**:",
        "  - M3U: `https://raw.githubusercontent.com/ZHDeveloper/tv-sources/main/live/live.m3u`",
        "  - TXT: `https://raw.githubusercontent.com/ZHDeveloper/tv-sources/main/live/live.txt`",
        "- **jsDelivr CDN 镜像**:",
        "  - M3U: `https://cdn.jsdelivr.net/gh/ZHDeveloper/tv-sources@main/live/live.m3u`",
        "  - TXT: `https://cdn.jsdelivr.net/gh/ZHDeveloper/tv-sources@main/live/live.txt`",
    ])
    summary_md_str = "\n".join(summary_md) + "\n"

    return live_txt_content, live_m3u_content, stats, summary_md_str


async def main():
    start_time = time.monotonic()
    logger.info("IPTV Live Aggregator starting...")

    # Load upstream sources: prefer live/upstream.json if present
    live_sources = []
    if UPSTREAM_CONFIG.exists():
        with open(UPSTREAM_CONFIG, "r", encoding="utf-8") as f:
            live_sources = json.load(f)
        logger.info(f"Loaded {len(live_sources)} upstream live sources from {UPSTREAM_CONFIG.name}")
    elif SOURCES_CONFIG.exists():
        with open(SOURCES_CONFIG, "r", encoding="utf-8") as f:
            config_data = json.load(f)
        all_live_sources = config_data.get("lives", [])
        live_sources = [
            s for s in all_live_sources
            if "live.txt" not in s.get("url", "") and "live.m3u" not in s.get("url", "") and "聚合精选" not in s.get("name", "")
        ]
        logger.info(f"Loaded {len(live_sources)} live sources from {SOURCES_CONFIG.name}")
    else:
        logger.error("No upstream sources file found!")
        sys.exit(1)

    # Step 1: Concurrently fetch all source playlists
    connector = aiohttp.TCPConnector(limit=15, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as fetch_session:
        fetch_tasks = [fetch_source_content(fetch_session, s) for s in live_sources]
        raw_sources = await asyncio.gather(*fetch_tasks)

    # Step 2: Parse and categorize candidates
    raw_channel_count = 0
    seen_urls: Set[str] = set()
    tv_candidates = defaultdict(list)
    web_candidates = defaultdict(list)

    for src_name, text in raw_sources:
        if not text:
            continue
        parsed_entries = parse_playlist(src_name, text)
        for raw_name, url, grp, tvg_id, tvg_logo in parsed_entries:
            raw_channel_count += 1
            if should_skip_channel(raw_name, url):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)

            std_name, cat, calc_id, calc_logo = normalize_channel(raw_name)
            final_id = tvg_id or calc_id
            final_logo = tvg_logo or calc_logo

            item = {
                "name": std_name,
                "category": cat,
                "url": url,
                "tvg_id": final_id,
                "tvg_logo": final_logo,
                "source": src_name,
            }

            key = f"{cat}::{std_name}"
            if cat in ("网络轮播", "综合频道"):
                web_candidates[key].append(item)
            else:
                tv_candidates[key].append(item)

    # Cap web stream channels to avoid thousands of transient room streams
    selected_candidates = dict(tv_candidates)
    web_count = 0
    for key, items in web_candidates.items():
        if web_count >= MAX_WEB_STREAM_CHANNELS:
            break
        selected_candidates[key] = items
        web_count += 1

    logger.info(
        f"Parsed {raw_channel_count} raw items -> {len(seen_urls)} unique URLs. "
        f"Selected {len(selected_candidates)} channels ({len(tv_candidates)} TV, {web_count} Web/Other) for validation."
    )

    # Step 3: Validate stream availability asynchronously
    valid_grouped = await validate_channels_pool(selected_candidates, max_concurrent=CONCURRENT_CHECK_LIMIT)

    # Step 4: Generate output playlists & statistics
    LIVE_DIR.mkdir(parents=True, exist_ok=True)
    live_txt_str, live_m3u_str, stats, summary_md_str = generate_playlists(valid_grouped)

    txt_file = LIVE_DIR / "live.txt"
    m3u_file = LIVE_DIR / "live.m3u"
    stats_file = LIVE_DIR / "stats.json"
    summary_file = LIVE_DIR / "summary.md"

    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(live_txt_str)

    with open(m3u_file, "w", encoding="utf-8") as f:
        f.write(live_m3u_str)

    stats["elapsed_seconds"] = round(time.monotonic() - start_time, 2)
    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(summary_md_str)

    logger.info(
        f"Aggregation complete in {stats['elapsed_seconds']}s! "
        f"Generated {stats['total_channels']} valid channels, {stats['total_streams']} streams."
    )
    for cat, info in stats["categories"].items():
        logger.info(f"  - {cat:10}: {info['channels']} 频道 / {info['streams']} 线路")


if __name__ == "__main__":
    asyncio.run(main())
