"""Refresh mirrored Spider JARs from spiders/upstream.json.

Each upstream jar is stored as <base>-<md5[:8]>.png so every update gets a
new jsDelivr URL. That avoids serving a stale cached JAR together with a
new MD5, which would make TVBox reject the file.
"""

import hashlib
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

SPIDERS = Path("spiders")
MANIFEST = SPIDERS / "upstream.json"
CONFIG = Path("ok-sources.json")


def fetch(url, attempts=3):
    last = None
    for _ in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "okhttp/4.12.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        except Exception as exc:
            last = exc
            time.sleep(5)
    raise last


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


def main():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    refs = {}
    changed = []
    errors = []

    for base, entry in manifest.items():
        try:
            blob = fetch(entry["upstream"])
            validate(blob, entry["upstream"])
            md5 = hashlib.md5(blob).hexdigest()
            current = SPIDERS / entry["file"]
            new_file = f"{base}-{md5[:8]}.png"

            if md5 == entry["md5"] and current.exists():
                refs[entry["file"]] = (entry["file"], md5)
                continue

            (SPIDERS / new_file).write_bytes(blob)
            if new_file != entry["file"] and current.exists():
                current.unlink()

            refs[entry["file"]] = (new_file, md5)
            # A config may already point at this version from an earlier run.
            refs[new_file] = (new_file, md5)
            changed.append(f"{base}: {entry['md5']} -> {md5} ({new_file})")
            entry["file"] = new_file
            entry["md5"] = md5
        except Exception as exc:
            errors.append(f"{base}: {exc}")

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

    for line in changed:
        print("CHANGED", line)
    for line in errors:
        print("WARN", line, file=sys.stderr)
    print(f"checked={len(manifest)} changed={len(changed)} failed={len(errors)}")

    if errors and not changed:
        sys.exit(1)


if __name__ == "__main__":
    main()
