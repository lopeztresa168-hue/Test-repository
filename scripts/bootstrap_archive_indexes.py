#!/usr/bin/env python3
# 
#
# مسئله: signature_archive_index.json و pattern_archive_index.json در مخزن
# سوم اصلاً وجود ندارند (هیچ‌وقت bootstrap نشده‌اند)، چون آرشیوهای موجود در
# signature_archives/ و pattern_archives/ قبل از اضافه‌شدن مکانیزم ایندکس
# پردازش شده و در processed_archives.json/processed_pattern_archives.json
# کش شده‌اند — یعنی build_all_queues.py دیگر هیچ‌وقت دوباره اسکنشان نمی‌کند
# و نگاشت signature_path/pattern_path -> archive_path‌شان برای همیشه گم
# مانده. build_all_queues.py یک مسیر bootstrap دارد (BOOTSTRAP_SOURCE_INDEX)
# ولی فقط برای signature است، معادلی برای pattern ندارد، و طبق قرارداد این
# پروژه اجازه‌ی تغییر build_all_queues.py وجود ندارد (نگاه کنید به کامنت
# مشابه در purge_session_mismatches.py).
#
# این اسکریپت به‌جای BOOTSTRAP_SOURCE_INDEX، هر دو ایندکس (signature و
# pattern) را از صفر و مستقل از build_all_queues.py می‌سازد — با یک طراحی
# سه‌مرحله‌ای متناسب با اجرای موازی (matrix) در GitHub Actions:
#
#   ۱. prepare  — فقط اسم پوشه‌های module/strategy زیر signature_archives/
#                 و pattern_archives/ را لیست می‌کند (سبک، بدون دانلود آرشیو)
#                 و بین N شارد (بر اساس --shards) تقسیم می‌کند.
#   ۲. shard    — هر job فقط آرشیوهای شارد خودش را دانلود/رمزگشایی/لیست
#                 می‌کند (read-only — هیچ‌چیز روی مخزن سوم نوشته نمی‌شود) و
#                 نتیجه را در یک فایل JSON محلی می‌ریزد تا به‌صورت artifact
#                 آپلود شود. چون shardها فقط می‌خوانند و چیزی روی مخزن
#                 نمی‌نویسند، هیچ race condition ای بین جاب‌های موازی وجود
#                 ندارد.
#   ۳. merge    — بعد از پایان همه‌ی shardها (نه موازی)، تمام artifactهای
#                 shard را می‌خواند، دو دیکشنری نهایی را می‌سازد، و فقط
#                 یک‌بار signature_archive_index.json و
#                 pattern_archive_index.json را آپلود می‌کند (تک‌نویسنده،
#                 بدون تداخل).
#
# نیازمندی‌های محیطی: THIRD_REPO, GH_TOKEN, RESULTS_PASSWORD (یا
# DATA_PASSWORD) — دقیقاً مثل build_all_queues.py/purge_session_mismatches.py

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path

import build_all_queues as baq  # فقط برای استفاده‌ی مجدد از توابع کمکی موجود
                                 # (gh_api_get_binary, _list_jsonl/csv_inside_archive,
                                 # get_file_sha, upload_file_with_curl, log) —
                                 # هیچ تابعی داخل build_all_queues.py تغییر/اضافه نمی‌شود.

log = baq.log

STANDARD_MODULES = {"combo_10day", "combo_monthly"}
KINDS = [
    # (kind_dir,        suffix,   list_fn,                      strip_prefix)
    ("signature_archives", ".jsonl", baq._list_jsonl_inside_archive, "signatures/"),
    ("pattern_archives",   ".csv",   baq._list_csv_inside_archive,   "patterns/"),
]


def _gh_list(repo, path):
    """لیست اسامی زیرمسیرهای یک پوشه در مخزن (فقط لیست، بدون دانلود)."""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.[].name' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        return []
    return [n.strip() for n in result.stdout.strip().split("\n") if n.strip()]


# ---------------------------------------------------------------------------
# مرحله ۱: prepare
# ---------------------------------------------------------------------------

def cmd_prepare(args):
    repo = os.environ["THIRD_REPO"]
    tasks = []  # هر آیتم: {"kind": ..., "module": ..., "strategy": ...}

    for kind_dir, _suffix, _fn, _strip in KINDS:
        top = _gh_list(repo, kind_dir)
        modules = [m for m in top if m in STANDARD_MODULES]
        log(f"  [PREPARE] {kind_dir}: ماژول‌ها = {modules}")
        for module in modules:
            strategies = _gh_list(repo, f"{kind_dir}/{module}")
            log(f"  [PREPARE] {kind_dir}/{module}: {len(strategies)} استراتژی")
            for strat in strategies:
                tasks.append({"kind": kind_dir, "module": module, "strategy": strat})

    log(f"  [PREPARE] مجموع (kind,module,strategy): {len(tasks)}")

    n_shards = args.shards
    shards = [[] for _ in range(n_shards)]
    for i, task in enumerate(tasks):
        shards[i % n_shards].append(task)

    non_empty_indices = [i for i, s in enumerate(shards) if s]
    log(f"  [PREPARE] {len(non_empty_indices)}/{n_shards} شارد غیرخالی خواهد بود")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"shards": shards}, f, ensure_ascii=False)

    # خروجی جدا برای matrix خود ورکفلو (فقط اندیس‌های غیرخالی)
    with open(args.matrix_out, "w", encoding="utf-8") as f:
        json.dump(non_empty_indices, f)

    log(f"  [PREPARE] فایل شاردها: {args.out}  |  ماتریکس اندیس‌ها: {args.matrix_out}")


# ---------------------------------------------------------------------------
# مرحله ۲: shard (read-only — فقط دانلود/رمزگشایی/لیست، هیچ آپلودی به مخزن ندارد)
# ---------------------------------------------------------------------------

def cmd_shard(args):
    repo = os.environ["THIRD_REPO"]
    password = os.environ.get("RESULTS_PASSWORD") or os.environ.get("DATA_PASSWORD")
    if not password:
        log("RESULTS_PASSWORD/DATA_PASSWORD تنظیم نشده.", "ERROR")
        sys.exit(1)

    with open(args.in_file, "r", encoding="utf-8") as f:
        all_shards = json.load(f)["shards"]

    if args.shard_index < 0 or args.shard_index >= len(all_shards):
        log(f"  [SHARD {args.shard_index}] اندیس خارج از محدوده — کاری نیست.")
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({"signature": {}, "pattern": {}}, f)
        return

    tasks = all_shards[args.shard_index]
    log(f"  [SHARD {args.shard_index}] {len(tasks)} (kind,module,strategy) به این شارد اختصاص یافت")

    kind_lookup = {k[0]: k for k in KINDS}
    sig_entries = {}
    pat_entries = {}

    for task in tasks:
        kind_dir, module, strat = task["kind"], task["module"], task["strategy"]
        _kind_dir, suffix, list_fn, strip_prefix = kind_lookup[kind_dir]

        path = f"{kind_dir}/{module}/{strat}"
        files = _gh_list(repo, path)
        archive_files = [fn for fn in files if fn.endswith(".tar.gz.enc")]
        if not archive_files:
            continue
        log(f"  [SHARD {args.shard_index}] {path}: {len(archive_files)} آرشیو")

        for fname in archive_files:
            archive_path = f"{path}/{fname}"
            entries = list_fn(repo, archive_path, password)
            if not entries:
                log(f"    ⚠️ {archive_path}: خالی/خراب — رد شد", "WARNING")
                continue
            for entry in entries:
                clean = entry[len(strip_prefix):] if entry.startswith(strip_prefix) else entry
                if kind_dir == "signature_archives":
                    sig_entries[clean] = archive_path
                else:
                    pat_entries[clean] = archive_path

    log(f"  [SHARD {args.shard_index}] نتیجه: {len(sig_entries)} signature، {len(pat_entries)} pattern")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"signature": sig_entries, "pattern": pat_entries}, f, ensure_ascii=False)


# ---------------------------------------------------------------------------
# مرحله ۳: merge — تک‌نویسنده، بعد از پایان همه‌ی shardها
# ---------------------------------------------------------------------------

def cmd_merge(args):
    repo = os.environ["THIRD_REPO"]

    merged_sig = {}
    merged_pat = {}

    shard_files = sorted(Path(args.in_dir).rglob("shard_*.json"))
    log(f"  [MERGE] {len(shard_files)} فایل shard پیدا شد")
    for sf in shard_files:
        try:
            with open(sf, "r", encoding="utf-8") as f:
                data = json.load(f)
            merged_sig.update(data.get("signature", {}))
            merged_pat.update(data.get("pattern", {}))
        except Exception as e:
            log(f"  ⚠️ خواندن {sf} ناموفق بود: {e}", "WARNING")

    log(f"  [MERGE] مجموع نهایی: {len(merged_sig)} signature، {len(merged_pat)} pattern")

    # ادغام با ایندکس فعلی (اگر از قبل چیزی وجود داشته باشد) — تا هیچ نگاشت
    # قدیمی‌ای که این bootstrap ندیده (مثلاً به‌خاطر خطای موقت) از دست نرود.
    existing_sig = baq._load_source_index(repo)
    existing_pat = baq._load_pattern_index(repo)
    existing_sig.update(merged_sig)
    existing_pat.update(merged_pat)

    tmp_sig = "/tmp/_bootstrap_signature_archive_index.json"
    tmp_pat = "/tmp/_bootstrap_pattern_archive_index.json"
    with open(tmp_sig, "w", encoding="utf-8") as f:
        json.dump(existing_sig, f, indent=2, ensure_ascii=False)
    with open(tmp_pat, "w", encoding="utf-8") as f:
        json.dump(existing_pat, f, indent=2, ensure_ascii=False)

    sha_sig = baq.get_file_sha(repo, baq.SOURCE_INDEX_PATH)
    sha_pat = baq.get_file_sha(repo, baq.PATTERN_INDEX_PATH)

    ok_sig = baq.upload_file_with_curl(
        repo, baq.SOURCE_INDEX_PATH, tmp_sig, sha_sig,
        "bootstrap: rebuild signature_archive_index.json via matrix scan"
    )
    ok_pat = baq.upload_file_with_curl(
        repo, baq.PATTERN_INDEX_PATH, tmp_pat, sha_pat,
        "bootstrap: rebuild pattern_archive_index.json via matrix scan"
    )

    if not ok_sig:
        log("❌ آپلود signature_archive_index.json ناموفق بود", "ERROR")
    if not ok_pat:
        log("❌ آپلود pattern_archive_index.json ناموفق بود", "ERROR")
    if not (ok_sig and ok_pat):
        sys.exit(1)

    log(f"✅ signature_archive_index.json: {len(existing_sig)} نگاشت")
    log(f"✅ pattern_archive_index.json: {len(existing_pat)} نگاشت")


def main():
    parser = argparse.ArgumentParser(description="Bootstrap موازی signature_archive_index.json و pattern_archive_index.json")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_prep = sub.add_parser("prepare")
    p_prep.add_argument("--shards", type=int, default=20)
    p_prep.add_argument("--out", default="/tmp/bootstrap_shards.json")
    p_prep.add_argument("--matrix-out", default="/tmp/bootstrap_matrix.json")
    p_prep.set_defaults(func=cmd_prepare)

    p_shard = sub.add_parser("shard")
    p_shard.add_argument("--in-file", required=True)
    p_shard.add_argument("--shard-index", type=int, required=True)
    p_shard.add_argument("--out", required=True)
    p_shard.set_defaults(func=cmd_shard)

    p_merge = sub.add_parser("merge")
    p_merge.add_argument("--in-dir", required=True, help="پوشه‌ای که همه‌ی artifactهای shard_*.json در آن دانلود شده‌اند")
    p_merge.set_defaults(func=cmd_merge)

    args = parser.parse_args()

    if not os.environ.get("THIRD_REPO") or not os.environ.get("GH_TOKEN"):
        log("THIRD_REPO / GH_TOKEN تنظیم نشده. خروج.", "ERROR")
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
