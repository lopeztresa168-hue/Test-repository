#!/usr/bin/env python3
"""
repro_tools/compare_outputs.py

قاعده سخت اعتبارسنجی #3: خروجی‌های job شاهد (ماژول اصلیِ دست‌نخورده) را با
خروجی‌های job verbose به‌صورت بایت‌به‌بایت مقایسه می‌کند (فایل‌های .csv و
.jsonl زیر دو پوشه‌ی results). هر اختلاف در گزارش نهایی (diff_report.json)
ثبت می‌شود؛ خروجی این اسکریپت هیچ‌وقت باعث fail شدن job نمی‌شود — فقط ثبت
می‌کند (تصمیم درباره‌ی اهمیت اختلاف با کاربر است، نه با CI).

Usage:
    python3 compare_outputs.py <witness_results_dir> <verbose_results_dir> <out_report.json>
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


def collect(base_dir, exts=(".csv", ".jsonl")):
    out = {}
    for root, _, files in os.walk(base_dir):
        for fn in files:
            if fn.endswith(exts):
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, base_dir)
                out[rel] = full
    return out


def main(witness_dir, verbose_dir, out_report):
    witness_files = collect(witness_dir)
    verbose_files = collect(verbose_dir)

    all_rel = sorted(set(witness_files) | set(verbose_files))
    report = {"identical": [], "different": [], "only_in_witness": [], "only_in_verbose": []}

    for rel in all_rel:
        in_w = rel in witness_files
        in_v = rel in verbose_files
        if in_w and not in_v:
            report["only_in_witness"].append(rel)
            print(f"⚠️ فقط در witness هست: {rel}")
            continue
        if in_v and not in_w:
            report["only_in_verbose"].append(rel)
            print(f"⚠️ فقط در verbose هست: {rel}")
            continue

        w_path, v_path = witness_files[rel], verbose_files[rel]
        w_sha, v_sha = sha256_of(w_path), sha256_of(v_path)
        if w_sha == v_sha:
            report["identical"].append(rel)
        else:
            report["different"].append({
                "file": rel,
                "witness_sha256": w_sha,
                "verbose_sha256": v_sha,
                "witness_size": os.path.getsize(w_path),
                "verbose_size": os.path.getsize(v_path),
            })
            print(f"‼️ اختلاف بایت‌به‌بایت: {rel}  (witness={w_sha[:12]}…  verbose={v_sha[:12]}…)")

    print(f"\n=== خلاصه مقایسه بایت‌به‌بایت witness vs verbose ===")
    print(f"  یکسان   : {len(report['identical'])}")
    print(f"  متفاوت  : {len(report['different'])}")
    print(f"  فقط witness: {len(report['only_in_witness'])}")
    print(f"  فقط verbose: {len(report['only_in_verbose'])}")

    with open(out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"گزارش کامل ذخیره شد: {out_report}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("usage: compare_outputs.py <witness_dir> <verbose_dir> <out_report.json>")
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3]))
