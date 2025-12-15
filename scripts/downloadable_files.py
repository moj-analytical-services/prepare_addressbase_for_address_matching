#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "requests",
#   "python-dotenv",
# ]
# ///

from __future__ import annotations

import os
import sys
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from dotenv import load_dotenv

# Data package details from:
# https://osdatahub.os.uk/api/dataPackages/0040204651/6758807/download?fileName=AB76GB_CSV.zip
PACKAGE_ID = "0040204651"
VERSION_ID = "6758807"


def format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def add_key_param(url: str, api_key: str) -> str:
    """Add API key as query parameter to URL for browser-pasteable downloads."""
    parts = urlparse(url)
    # Keep existing params, drop any existing key param, then add ours
    params = [
        (k, v)
        for (k, v) in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() != "key"
    ]
    params.append(("key", api_key))
    return urlunparse(parts._replace(query=urlencode(params)))


def main() -> int:
    # Load environment variables from .env file
    load_dotenv()

    api_key = os.environ.get("OS_PROJECT_API_KEY")
    if not api_key:
        print("Error: OS_PROJECT_API_KEY not found in environment.", file=sys.stderr)
        print(
            "Make sure you have a .env file with OS_PROJECT_API_KEY=<your-key>",
            file=sys.stderr,
        )
        return 1

    # Query the OS Downloads API
    url = f"https://api.os.uk/downloads/v1/dataPackages/{PACKAGE_ID}/versions/{VERSION_ID}"
    headers = {"key": api_key}

    print(f"Querying OS Downloads API...")
    print(f"Package ID: {PACKAGE_ID}, Version: {VERSION_ID}\n")

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        package_data = response.json()

        # Print package information
        print(f"{'=' * 80}")
        print(f"Data Package: {package_data.get('dataPackage', {}).get('name', 'N/A')}")
        print(f"Version ID: {package_data.get('id', 'N/A')}")
        print(f"Created: {package_data.get('createdOn', 'N/A')}")
        print(f"Supply Type: {package_data.get('supplyType', 'N/A')}")
        print(f"Format: {package_data.get('format', 'N/A')}")
        print(f"{'=' * 80}\n")

        # List downloadable files
        downloads = package_data.get("downloads", [])
        if not downloads:
            print("No downloadable files found.")
            return 0

        print(f"Available Files ({len(downloads)}):\n")

        total_size = 0
        for i, file_info in enumerate(downloads, 1):
            filename = file_info.get("fileName", "N/A")
            size = file_info.get("size", 0)
            md5 = file_info.get("md5", "N/A")
            download_url = file_info.get("url", "N/A")
            download_url_with_key = (
                add_key_param(download_url, api_key) if download_url != "N/A" else "N/A"
            )

            total_size += size

            print(f"{i}. {filename}")
            print(f"   Size: {format_size(size)} ({size:,} bytes)")
            print(f"   MD5:  {md5}")
            print(f"   URL:  {download_url_with_key}")
            print()

        print(f"{'=' * 80}")
        print(f"Total Size: {format_size(total_size)} ({total_size:,} bytes)")
        print(f"{'=' * 80}")

        return 0

    except requests.HTTPError as e:
        print(f"HTTP Error: {e}", file=sys.stderr)
        if e.response is not None:
            print(f"Response: {e.response.text}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
