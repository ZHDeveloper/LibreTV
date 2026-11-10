"""Refresh mirrored Spider JARs from spiders/upstream.json.

First discovers the latest spider URLs from https://12586.kstore.space/123.txt
(supporting plain JSON, image-appended "**" base64, and "2423...2324" AES-CBC
configs), updates spiders/upstream.json when a source switches to a new URL,
and then downloads/validates any changed JARs.

Each upstream jar is stored as <base>-<md5[:8]>.bin so every update gets a
new jsDelivr URL. That avoids serving a stale cached JAR together with a
new MD5, which would make TVBox reject the file.
"""

import base64
import binascii
import hashlib
import io
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SPIDERS = Path("spiders")
MANIFEST = SPIDERS / "upstream.json"
CONFIG = Path("ok-sources.json")
INDEX_URL = "https://12586.kstore.space/123.txt"
MD5_SUFFIX = re.compile(r"(?:;md5;|#\.\w+;)")
SOURCE_MAP = {
    "\u996d\u592a\u786c": "fantaiying",
    "\u996d\u592a\u786c\uff08\u5907\u7528\uff09": "fantaiying",
    "\u5c0f\u7c73": "xiaomi",
    "\u80a5\u732b": "feimao",
    "\u738b\u4e8c\u5c0f": "wangerxiao",
    "\u6478\u9c7c\u513f": "moyuer",
    "\u6f47\u6d12": "xiaosa",
    "\u55f7\u545c": "aowu",
    "\u5c0f\u864e\u6591": "xiaohuban",
    "\u83dc\u59ae\u4e1d": "cainisi",
    "\u4e8c\u6708\u7ea2": "eryuehong-tan",
    "\u5357\u98ce": "nanfeng",
    "\u620f\u66f2\u97f3\u4e50": "xiqu",
}


def normalize_url(url):
    sp = urllib.parse.urlsplit(url.strip())
    host = sp.hostname.encode("idna").decode("ascii") if sp.hostname else ""
    netloc = f"{host}:{sp.port}" if sp.port else host
    path = urllib.parse.quote(sp.path, safe="/:@!$&'()*+,;=-._~%")
    return urllib.parse.urlunsplit((sp.scheme, netloc, path, sp.query, sp.fragment))


def fetch(url, attempts=2):
    url = normalize_url(url)
    curl = shutil.which("curl")
    last = None
    for _ in range(attempts):
        if curl:
            try:
                proc = subprocess.run(
                    [
                        curl, "-fsSL", "--connect-timeout", "10", "--max-time", "25",
                        "-A", "okhttp/4.12.0", url,
                    ],
                    capture_output=True,
                    timeout=35,
                )
                if proc.returncode == 0 and proc.stdout:
                    return proc.stdout
                last = proc.stderr.decode("utf-8", errors="ignore").strip()[:200]
            except Exception as exc:
                last = exc
            time.sleep(2)
            continue
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "okhttp/4.12.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read()
        except Exception as exc:
            last = exc
            time.sleep(2)
    if not isinstance(last, BaseException):
        last = RuntimeError(str(last))
    raise last


def strip_comments(text):
    return "\n".join(
        line for line in text.splitlines()
        if not line.strip().startswith(("//", "/*"))
    )


def decrypt_2423(hex_str):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    hex_str = hex_str.strip()
    idx = hex_str.find("2324")
    key = binascii.unhexlify(hex_str[4:idx]).decode("utf-8", "ignore").ljust(16, "0")[:16].encode("utf-8")
    iv = binascii.unhexlify(hex_str[-26:]).decode("utf-8", "ignore").ljust(16, "0")[:16].encode("utf-8")
    cipher_bytes = binascii.unhexlify(hex_str[idx + 4:-26])
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    plain = decryptor.update(cipher_bytes) + decryptor.finalize()
    pad = plain[-1]
    if 1 <= pad <= 16:
        plain = plain[:-pad]
    return plain.decode("utf-8", "ignore")


def decode_source_config(blob):
    if b"**" in blob:
        b64 = blob.rsplit(b"**", 1)[-1].strip()
        text = base64.b64decode(b64).decode("utf-8", "ignore")
    else:
        text = blob.decode("utf-8", "ignore").strip()
        if text.startswith("2423") and "2324" in text:
            text = decrypt_2423(text)
    return json.loads(strip_comments(text))


def clean_jar_url(value):
    return normalize_url(MD5_SUFFIX.split(value.strip(), maxsplit=1)[0])


def discover_upstreams(manifest):
    try:
        index = json.loads(strip_comments(fetch(INDEX_URL).decode("utf-8", "ignore")))
    except Exception as exc:
        print(f"WARN index discovery failed: {exc}", file=sys.stderr)
        return []

    def load_source(item):
        name = item.get("name", "")
        if name not in SOURCE_MAP:
            return name, None
        try:
            return name, decode_source_config(fetch(item["url"]))
        except Exception as exc:
            return name, exc

    urls = index.get("urls", [])
    with ThreadPoolExecutor(max_workers=min(len(urls) or 1, 8)) as pool:
        loaded = list(pool.map(load_source, urls))

    updated = []
    for name, cfg in loaded:
        if cfg is None or isinstance(cfg, Exception):
            continue
        base = SOURCE_MAP[name]
        spider = cfg.get("spider")
        if isinstance(spider, str) and spider.strip():
            new_url = clean_jar_url(spider)
            if new_url and manifest.get(base, {}).get("upstream") != new_url:
                updated.append(f"{base}: {manifest[base]['upstream']} -> {new_url}")
                manifest[base]["upstream"] = new_url

        site_jars = [
            clean_jar_url(s["jar"])
            for s in (cfg.get("sites") or [])
            if isinstance(s.get("jar"), str) and s.get("jar").strip()
        ]
        if base == "aowu":
            alt = next((u for u in site_jars if u != manifest["aowu"]["upstream"]), None)
            if alt and manifest.get("aowu-alt", {}).get("upstream") != alt:
                updated.append(f"aowu-alt: {manifest['aowu-alt']['upstream']} -> {alt}")
                manifest["aowu-alt"]["upstream"] = alt
        elif base == "cainisi":
            alt = next((u for u in site_jars if "xyq" in u), None)
            if alt and manifest.get("cainisi-alt", {}).get("upstream") != alt:
                updated.append(f"cainisi-alt: {manifest['cainisi-alt']['upstream']} -> {alt}")
                manifest["cainisi-alt"]["upstream"] = alt
        elif base == "eryuehong-tan":
            alt = next((u for u in site_jars if "lubin" in u), None)
            if alt and manifest.get("eryuehong-lubin", {}).get("upstream") != alt:
                updated.append(f"eryuehong-lubin: {manifest['eryuehong-lubin']['upstream']} -> {alt}")
                manifest["eryuehong-lubin"]["upstream"] = alt
    return updated


def validate(blob, url):
    if blob[:2] != b"PK":
        raise ValueError(f"not a JAR/ZIP: {url}")
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        if not any(name.endswith((".dex", ".class")) for name in zf.namelist()):
            raise ValueError(f"no dex/class inside: {url}")


def url_name(value):
    return value.rsplit("/", 1)[-1]


def remap(value, refs):
    if not value or ";md5;" not in value:
        return value
    url, old_md5 = value.rsplit(";md5;", 1)
    name = url_name(url)
    if name not in refs:
        return value
    new_name, new_md5 = refs[name]
    if name == new_name and old_md5 == new_md5:
        return value
    return f"{url.rsplit('/', 1)[0]}/{new_name};md5;{new_md5}"


def refresh(base, entry):
    current = SPIDERS / entry["file"]
    try:
        blob = fetch(entry["upstream"])
        validate(blob, entry["upstream"])
        md5 = hashlib.md5(blob).hexdigest()

        if md5 == entry["md5"] and current.exists():
            return base, entry["md5"], md5, entry["file"], entry["file"], None

        new_file = f"{base}-{md5[:8]}.bin"
        (SPIDERS / new_file).write_bytes(blob)
        if new_file != entry["file"] and current.exists():
            current.unlink()
        return base, entry["md5"], md5, entry["file"], new_file, None
    except Exception as exc:
        return base, entry["md5"], entry["md5"], entry["file"], entry["file"], str(exc)


def main():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    discovered = discover_upstreams(manifest)
    refs = {}
    changed = []
    errors = []

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda kv: refresh(*kv), manifest.items()))

    for base, old_md5, md5, old_file, new_file, error in results:
        if error:
            errors.append(f"{base}: {error}")
            continue
        refs[old_file] = (new_file, md5)
        # A config may already point at this version from an earlier run.
        refs[new_file] = (new_file, md5)
        if md5 != old_md5 or new_file != old_file:
            changed.append(f"{base}: {old_md5} -> {md5} ({new_file})")
            manifest[base]["file"] = new_file
            manifest[base]["md5"] = md5

    config_text = CONFIG.read_text(encoding="utf-8")
    config = json.loads(config_text)
    if config.get("spider"):
        config["spider"] = remap(config["spider"], refs)
    for site in config.get("sites", []):
        if site.get("jar"):
            site["jar"] = remap(site["jar"], refs)

    new_config = json.dumps(config, ensure_ascii=False, indent=2) + "\n"
    new_manifest = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"

    if new_config != config_text:
        CONFIG.write_text(new_config, encoding="utf-8")
    if new_manifest != MANIFEST.read_text(encoding="utf-8"):
        MANIFEST.write_text(new_manifest, encoding="utf-8")

    for line in discovered:
        print("DISCOVERED", line)
    for line in changed:
        print("CHANGED", line)
    for line in errors:
        print("WARN", line, file=sys.stderr)
    print(f"checked={len(manifest)} discovered={len(discovered)} changed={len(changed)} failed={len(errors)}")

    if errors and not changed:
        sys.exit(1)


if __name__ == "__main__":
    main()
