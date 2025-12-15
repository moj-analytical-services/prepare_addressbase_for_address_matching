#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "requests",
#   "markitdown",
# ]
# ///

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import sys
import tempfile
import time
from pathlib import Path
from zipfile import ZipFile

import requests
from markitdown import MarkItDown
from requests.adapters import HTTPAdapter

try:
    # requests vendors/depends on urllib3
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    Retry = None  # type: ignore


URLS: list[str] = [
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/data-formats",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/supply-and-update",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/addressbase-premium-structure",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/feature-types",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/feature-types/street-type-11-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/feature-types/blpu-type-21-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/feature-types/addressbase-supply-set",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/feature-types/feature-with-lifecycle",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/street-descriptor-type-15-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/application-cross-reference-type-23-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/lpi-type-24-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/delivery-point-address-type-28-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/successor-cross-reference-type-30-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/organisation-type-31-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/classification-type-32-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/structured-data-types/entity-with-lifecycle",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/dataset-metadata/metadata-type-29-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/dataset-metadata/header-type-10-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/dataset-metadata/trailer-type-99-record",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/code-lists-and-enumerations",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-premium/addressbase-premium-technical-specification/example-records/csv",
    "https://docs.os.uk/os-downloads/addressing-and-location/addressbase-fundamentals/classification-scheme",
]


def _dedupe_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s or "section"


def _demote_headings(md_text: str, by: int = 1) -> str:
    def repl(m: re.Match[str]) -> str:
        hashes = m.group(1)
        rest = m.group(2)
        new_level = min(6, len(hashes) + by)
        return ("#" * new_level) + " " + rest

    return re.sub(r"^(#{1,6})\s+(.+)$", repl, md_text, flags=re.MULTILINE)


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "os-docs-scraper/1.0 (+https://docs.os.uk)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    if Retry is not None:
        try:
            retry = Retry(
                total=5,
                backoff_factor=0.6,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]),
                raise_on_status=False,
            )
        except TypeError:  # older urllib3
            retry = Retry(  # type: ignore
                total=5,
                backoff_factor=0.6,
                status_forcelist=(429, 500, 502, 503, 504),
                method_whitelist=frozenset(["GET"]),
                raise_on_status=False,
            )

        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)

    return s


def _convert_page(
    md: MarkItDown, url: str, session: requests.Session
) -> tuple[str, str]:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    # Prefer converting the HTTP response so we control headers/retries.
    if hasattr(md, "convert_response"):
        result = md.convert_response(resp)
    elif hasattr(md, "convert_url"):
        result = md.convert_url(url)  # older API
    elif hasattr(md, "convert_uri"):
        result = md.convert_uri(url)
    else:
        result = md.convert(url)

    title = getattr(result, "title", None) or url
    text = getattr(result, "text_content", "") or ""
    return str(title).strip(), text.strip()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Scrape OS docs pages into a single Markdown file."
    )
    ap.add_argument(
        "-o",
        "--output",
        default="os_docs.md",
        help="Output markdown file (default: os_docs.md)",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)",
    )
    args = ap.parse_args()

    urls = _dedupe_preserve_order(URLS)
    out_path = Path(args.output).resolve()

    md = MarkItDown(enable_plugins=False)
    session = _build_session()

    generated_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    # Pre-collect titles for a simple TOC (best-effort; if a page fails, it still proceeds).
    sections: list[tuple[str, str, str]] = []  # (url, title, body_md)
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Fetching + converting: {url}")
        try:
            title, body = _convert_page(md, url, session)
            sections.append((url, title, body))
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            sections.append(
                (
                    url,
                    f"(FAILED) {url}",
                    f"> Failed to convert this page.\n>\n> Error: `{e}`\n",
                )
            )
        time.sleep(max(0.0, args.delay))

    # Build unique anchors
    used: set[str] = set()
    anchors: list[str] = []
    for _, title, _ in sections:
        a = _slug(title)
        base = a
        n = 2
        while a in used:
            a = f"{base}-{n}"
            n += 1
        used.add(a)
        anchors.append(a)

    with out_path.open("w", encoding="utf-8") as f:
        f.write("# OS docs scrape\n\n")
        f.write(f"_Generated: {generated_at}_\n\n")
        f.write("## Table of contents\n\n")
        for (url, title, _), a in zip(sections, anchors, strict=False):
            f.write(f"- [{title}](#{a})  \n  <{url}>\n")
        f.write("\n---\n")

        for (url, title, body), a in zip(sections, anchors, strict=False):
            f.write(f"\n## {title}\n\n")
            f.write(f"Source: <{url}>\n\n")

            # Demote headings so the file stays well-structured under our per-page H2.
            body2 = _demote_headings(body, by=1).strip()

            # If the converted content starts with the same title as our section, drop it.
            first_nonempty = next((ln for ln in body2.splitlines() if ln.strip()), "")
            if re.match(r"^#{2,6}\s+", first_nonempty):
                t = re.sub(r"^#{2,6}\s+", "", first_nonempty).strip()
                if t.lower() == title.lower():
                    body2 = "\n".join(body2.splitlines()[1:]).lstrip()

            # Add an explicit anchor line for markdown renderers that don’t auto-anchor consistently.
            f.write(f'<a id="{a}"></a>\n\n')
            f.write(body2)
            f.write("\n\n---\n")

    # Download and append CSV headers from the zip file
    print("\nDownloading CSV header files...")
    zip_url = "https://1897589978-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2FcNpJpLP8RROUaWVQo5ea%2Fuploads%2FGSJPfervI4tEnUThO9Hd%2Faddressbase-premium-header-files.zip?alt=media&token=682afd54-9862-4caf-a46d-92c9ab8733dc"

    try:
        resp = session.get(zip_url, timeout=30)
        resp.raise_for_status()

        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "headers.zip"
            zip_path.write_bytes(resp.content)

            with out_path.open("a", encoding="utf-8") as f:
                f.write("\n## CSV Header Files\n\n")
                f.write(
                    "Headers extracted from addressbase-premium-header-files.zip\n\n"
                )

                with ZipFile(zip_path, "r") as zipf:
                    csv_files = [
                        name for name in zipf.namelist() if name.endswith(".csv")
                    ]

                    for csv_file in sorted(csv_files):
                        print(f"  Processing: {csv_file}")
                        with zipf.open(csv_file) as csvf:
                            # Read the first line (headers)
                            content = csvf.read().decode("utf-8")
                            reader = csv.reader(content.splitlines())
                            headers = next(reader, [])

                            if headers:
                                f.write(f"**{csv_file}**\n\n")
                                f.write(f"`{','.join(headers)}`\n\n")

        print("CSV headers appended successfully")

    except Exception as e:
        print(
            f"  WARNING: Failed to download/process CSV headers: {e}", file=sys.stderr
        )

    print(f"\nDone! Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
