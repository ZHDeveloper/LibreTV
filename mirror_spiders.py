import hashlib
import io
import json
import re
import urllib.request
import zipfile
from pathlib import Path

REPO = "https://cdn.jsdelivr.net/gh/ZHDeveloper/LibreTV@main/spiders"
OUT_DIR = Path("spiders")

NAME_HINTS = [
    ("xiuxiu-pro-new.meitudata.com", "fantaiying"),
    ("cac.opple.com", "xiaomi"),
    ("img2.gelonghui.com", "feimao"),
    ("oss4liview.moji.com", "wangerxiao"),
    ("qiniu.gongxueyun.com", "moyuer"),
    ("aly2.tuchuangyun.top", "xiaosa"),
    ("0286b8d7", "aowu"),
    ("35012678", "aowu-alt"),
    ("gitee.com/xhuban", "xiaohuban"),
    ("fty1016", "cainisi"),
    ("xyq0728", "cainisi-alt"),
    ("lubin.php", "eryuehong-lubin"),
    ("tan.php", "eryuehong-tan"),
    ("jihulab.com/yoursmile77", "nanfeng"),
    ("z.qiqiv.cn", "xiqu"),
]


def clean_url(value):
    url = value.split(";md5;")[0]
    return url.split("#")[0]


def jar_name(url, used):
    for needle, name in NAME_HINTS:
        if needle in url:
            candidate = f"{name}.jar"
            break
    else:
        candidate = f"spider-{hashlib.md5(url.encode()).hexdigest()[:8]}.jar"
    if candidate in used:
        candidate = candidate.replace(".jar", f"-{hashlib.md5(url.encode()).hexdigest()[:6]}.jar")
    used.add(candidate)
    return candidate


def download(url):
    req = urllib.request.Request(url, headers={"User-Agent": "okhttp/4.12.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def main():
    config = json.loads(Path("ok-sources.json").read_text(encoding="utf-8"))
    urls = []
    if config.get("spider"):
        urls.append(clean_url(config["spider"]))
    for site in config.get("sites", []):
        if site.get("jar"):
            urls.append(clean_url(site["jar"]))
    urls = sorted(set(urls))

    OUT_DIR.mkdir(exist_ok=True)
    mapping = {}
    used = set()
    total = 0

    for url in urls:
        blob = download(url)
        digest = hashlib.md5(blob).hexdigest()
        if blob[:2] != b"PK":
            raise ValueError(f"Not a JAR/ZIP: {url}")
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            entries = zf.namelist()
            if not any(n.endswith((".dex", ".class")) for n in entries):
                raise ValueError(f"No dex/class inside: {url}")
        name = jar_name(url, used)
        (OUT_DIR / name).write_bytes(blob)
        mapping[url] = f"{REPO}/{name};md5;{digest}"
        total += len(blob)
        print(f"{name:24} {len(blob):>9} bytes  md5={digest}")

    if config.get("spider"):
        config["spider"] = mapping[clean_url(config["spider"])]
    for site in config.get("sites", []):
        if site.get("jar"):
            site["jar"] = mapping[clean_url(site["jar"])]

    Path("ok-sources.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"\nMirrored {len(mapping)} jars, total {total / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
