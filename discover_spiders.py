"""Report whether spiders/upstream.json still matches the upstream index.

The index at https://12586.kstore.space/123.txt lists every known source
config. Plain-JSON configs expose the spider JAR URL plus an MD5, which is how
a new snapshot (e.g. a fresh dated image-host upload) can be found without
guessing. Read-only: it prints drift and never edits the manifest.
"""

import json
import re
from concurrent.futures import ThreadPoolExecutor

from mirror_spiders import MANIFEST, fetch

INDEX = "https://12586.kstore.space/123.txt"
MD5_SUFFIX = re.compile(r"(?:;md5;|#\.\w+;)")


def host(url):
    match = re.match(r"https?://([^/]+)", url)
    return match.group(1) if match else ""


def strip_comments(text):
    return "\n".join(line for line in text.splitlines() if not line.strip().startswith("//"))


def parse_config(blob):
    text = strip_comments(blob.decode("utf-8", errors="ignore"))
    if not text.lstrip().startswith("{"):
        raise ValueError("非明文配置（加密或多仓格式）")
    return json.loads(text)


def collect(config):
    values = []
    if isinstance(config.get("spider"), str):
        values.append(config["spider"])
    for site in config.get("sites") or []:
        jar = site.get("jar")
        if isinstance(jar, str) and jar:
            values.append(jar)
    return values


def split_md5(value):
    parts = MD5_SUFFIX.split(value, maxsplit=1)
    return parts[0], (parts[1].strip() if len(parts) > 1 else "")


def main():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    by_host = {}
    for key, entry in manifest.items():
        by_host.setdefault(host(entry["upstream"]), []).append(key)

    index = json.loads(strip_comments(fetch(INDEX).decode("gbk", errors="ignore")))
    found = {}
    new = set()

    def load(source):
        name = source.get("name", "?")
        try:
            return name, collect(parse_config(fetch(source["url"])))
        except Exception as exc:
            return name, exc

    with ThreadPoolExecutor(max_workers=len(index.get("urls", [])) or 1) as pool:
        loaded = list(pool.map(load, index.get("urls", [])))

    for name, values in loaded:
        if isinstance(values, Exception):
            print(f"SKIP    {name:10} {str(values)[:50]}")
            continue
        for value in values:
            url, md5 = split_md5(value)
            key = host(url)
            if key in by_host:
                found.setdefault(key, []).append((url, md5))
            elif key:
                new.add((name, url, md5))

    stale = 0
    for key, entries in sorted(by_host.items()):
        if key not in found:
            for entry in entries:
                print(f"NOMATCH {entry:16} 索引中未找到 {key}")
            continue
        candidates = found[key]
        md5s = {md5 for _, md5 in candidates if md5}
        for entry in entries:
            if manifest[entry]["md5"] in md5s or not md5s:
                print(f"SAME    {entry:16} {manifest[entry]['md5'][:8]}")
            elif len(md5s) == 1:
                md5 = md5s.pop()
                stale += 1
                print(f"UPDATE  {entry:16} {manifest[entry]['md5'][:8]} -> {md5[:8]}")
                url = next(u for u, m in candidates if m == md5)
                print(f"        {url}")
            else:
                print(f"AMBIG   {entry:16} 候选: {sorted(md5s)}")

    for name, url, md5 in sorted(new):
        print(f"NEW     {name:10} {md5[:8] or '-':9} {url[:100]}")

    print(f"entries={len(manifest)} stale={stale}")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
