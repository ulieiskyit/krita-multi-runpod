#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import urllib.request

DEFAULT_MANIFEST = "manifest.json"
DEFAULT_BASE_DIR = "models"

def download_file(url, filepath):
    if os.path.exists(filepath):
        print(f"[skip] {filepath} already exists")
        return
    print(f"[download] {url} -> {filepath}")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with urllib.request.urlopen(url) as resp, open(filepath, "wb") as out:
        shutil.copyfileobj(resp, out)

def parse_args():
    parser = argparse.ArgumentParser(description="Download model assets defined in a manifest JSON file.")
    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST,
        help="Path to the manifest JSON file (default: %(default)s)",
    )
    parser.add_argument(
        "--base-dir",
        default=DEFAULT_BASE_DIR,
        help="Directory to place downloaded files (default: %(default)s)",
    )
    return parser.parse_args()

def main():
    args = parse_args()

    with open(args.manifest, "r") as f:
        manifest = json.load(f)

    base_dir = os.path.abspath(args.base_dir)

    for category, items in manifest.items():
        category_dir = os.path.join(base_dir, category)
        os.makedirs(category_dir, exist_ok=True)

        for entry in items:
            url = entry["url"]
            filename = entry["filename"]
            filepath = os.path.join(category_dir, filename)
            download_file(url, filepath)

if __name__ == "__main__":
    main()
