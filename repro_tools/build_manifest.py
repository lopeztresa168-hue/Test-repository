#!/usr/bin/env python3
"""
repro_tools/build_manifest.py

آخرین مرحله‌ی هر job: manifest کامل از هر فایل خروجی (مسیر، حجم، sha256،
تعداد رکورد برای jsonl/csv) + یک خلاصه‌ی وضعیت هر ترکیب (done/failed) و
زمان اجرا می‌سازد. این فایل به artifact نهایی اضافه می‌شود.

Usage:
    python3 build_manifest.py <results_dir> <done_items.json> <failed_items.json> \
        <chunk_index> <elapsed_seconds> <out_manifest.json>
"""
import hashlib
import json
import os
import sys


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def record_count(path):
    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    if path.endswith(".csv"):
        with open(path, "r", encoding="utf-8-sig") as f:
            return max(sum(1 for _ in f) - 1, 0)  # منهای هدر
    return None


def main(results_dir, done_path, failed_path, chunk_index, elapsed_seconds, out_path):
    files_manifest = []
    for root, _, files in os.walk(results_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, results_dir)
            files_manifest.append({
                "path": rel,
                "size_bytes": os.path.getsize(full),
                "sha256": sha256_of(full),
                "record_count": record_count(full),
            })

    done = json.load(open(done_path)) if os.path.exists(done_path) else []
    failed = json.load(open(failed_path)) if os.path.exists(failed_path) else []

    manifest = {
        "chunk_index": chunk_index,
        "elapsed_seconds": float(elapsed_seconds),
        "done_count": len(done),
        "failed_count": len(failed),
        "done_items": done,
        "failed_items": failed,
        "files": files_manifest,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"✅ manifest ساخته شد: {out_path} ({len(files_manifest)} فایل، "
          f"done={len(done)}, failed={len(failed)})")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("usage: build_manifest.py <results_dir> <done.json> <failed.json> "
              "<chunk_index> <elapsed_seconds> <out.json>")
        sys.exit(2)
    sys.exit(main(*sys.argv[1:]))
