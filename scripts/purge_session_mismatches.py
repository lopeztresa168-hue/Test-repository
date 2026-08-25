#!/usr/bin/env python3
# 
#
# مسئله: قبل از فیکس granularity (اضافه‌شدن --session به combo_10day.py و
# combo_monthly.py)، وقتی یک (استراتژی, کوین) فقط از طریق یک رکورد سشن خاص
# (مثلاً "BTCUSDT__session_london") هفت شرط را پاس می‌کرد — نه از طریق رکورد
# کل‌روزه‌ی همان کوین — سیستم قدیم به‌هرحال یک صف بدون --session می‌ساخت،
# یعنی خروجی fixed_5d/10d/.../monthly که الان روی دیسک/در batch_outputs
# ذخیره شده، از معاملات کل ۲۴ ساعته محاسبه شده، نه فقط بازه‌ی ساعتی سشنی که
# واقعاً قبول شده بود. این اسکریپت دقیقاً همین موارد را:
#   ۱. با اسکن مستقیم _internal_all_results.json (نه یک لیست ایستا) پیدا می‌کند
#   ۲. فایل‌های خروجی نادرست را از batch_outputs/*/ (مخزن اصلی) حذف می‌کند
#   ۳. همان ترکیب‌ها را با --session درست، دوباره به all_combinations.json
#      (مخزن سوم) اضافه می‌کند تا analysis_fixed_batch.yml این بار درست
#      پردازش‌شان کند.
#
# نکته‌ی مهم: (استراتژی, کوین)ی که رکورد کل‌روزه‌اش هم مستقل قبول شده بود،
# دست‌نخورده می‌ماند — چون خروجی کل‌روزه‌اش از اول درست بوده.
#
# استفاده:
#   python purge_session_mismatches.py --scan-only        فقط گزارش می‌دهد، چیزی حذف/ری‌کیو نمی‌کند
#   python purge_session_mismatches.py --apply             واقعاً حذف و ری‌کیو می‌کند
#
# نیازمندی‌های محیطی (env vars) — دقیقاً همان‌هایی که build_all_queues.py لازم دارد:
#   THIRD_REPO, GH_TOKEN, RESULTS_PASSWORD (یا DATA_PASSWORD)
#   BATCH_OUTPUTS_ROOT (اختیاری، پیش‌فرض "batch_outputs" — مسیر لوکال در مخزن اصلیِ چک‌اوت‌شده)

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict

import build_all_queues as baq  # از همان دایرکتوری اجرا می‌شود؛ توابع کمکی را دوباره‌نویسی نمی‌کنیم

log = baq.log


def compute_mismatches(records):
    """برای هر (period_name, coin)، تعیین می‌کند که آیا:
       - رکورد کل‌روزه (session=None) هم مستقل قبول شده (fullday_accepted)
       - و/یا کدام سشن‌ها قبول شده‌اند (session_accepted)
    خروجی: لیستی از دیکشنری‌های {"strat":..., "coin":..., "sessions":[...]}
    فقط برای مواردی که فقط از طریق سشن قبول شده‌اند (نه کل‌روز)."""
    fullday_accepted = set()
    session_accepted = defaultdict(set)

    skipped = 0
    for item in records:
        if not isinstance(item, dict):
            skipped += 1
            continue
        folder_name = item.get("period_name")
        symbol = item.get("symbol") or ""
        if not folder_name or not symbol:
            skipped += 1
            continue
        if not baq._passes_seven_conditions(item):
            continue
        coin, session = baq._parse_symbol_to_coin_session(symbol)
        key = (folder_name, coin)
        if session is None:
            fullday_accepted.add(key)
        else:
            session_accepted[key].add(session)

    if skipped:
        log(f"  ⚠️ {skipped} رکورد بدون period_name/symbol معتبر رد شد", 'WARNING')

    mismatches = []
    for key, sessions in session_accepted.items():
        if key in fullday_accepted:
            continue
        strat, coin = key
        mismatches.append({"strat": strat, "coin": coin, "sessions": sorted(sessions)})

    log(f"  تعداد (استراتژی,کوین) با حداقل یک سشن قبول‌شده: {len(session_accepted)}")
    log(f"  از این‌ها، رکورد کل‌روزه هم قبول شده (خروجی درست، دست‌نخورده می‌ماند): {len(session_accepted) - len(mismatches)}")
    log(f"  از این‌ها، فقط سشن قبول شده (خروجی نادرست — باید پاک/ری‌کیو شود): {len(mismatches)}")
    return mismatches


# ---------------------------------------------------------------------------
# بخش ۱: پیداکردن و حذف فایل‌های خروجی نادرست از مخزن اصلی (batch_outputs/)
# ---------------------------------------------------------------------------

def _safe_coin(coin):
    return coin.replace('+', '_')


def find_stale_output_files(batch_outputs_root, mismatches):
    """زیر batch_outputs_root (همه‌ی chunk_* ها) دنبال فایل‌هایی می‌گردد که
    مربوط به (strat, coin) های mismatch هستند — در هر دو مسیر ممکن:
      batch_outputs/chunk_*/signatures/{module}/{strat}/{safe_coin}/*.jsonl
      batch_outputs/chunk_*/{module}_patterns/{strat}/{safe_coin}/*.csv
    توجه: فایل‌هایی که خودشان suffix سشن دارند (مثلاً ..._london.csv) از قبل
    درست بودند (اگر با این فیکس بعد از استقرار تولید شده باشند) و اینجا هم
    حذف می‌شوند تا اگر تکراری/قدیمی بودند، دوباره‌سازی تمیز شوند — بدون خطر،
    چون ری‌کیو در بخش ۲ آن‌ها را با --session درست از نو می‌سازد.
    """
    root = Path(batch_outputs_root)
    if not root.exists():
        log(f"  ⚠️ مسیر {batch_outputs_root} پیدا نشد — هیچ فایلی برای حذف بررسی نمی‌شود", 'WARNING')
        return []

    targets = {(m["strat"], m["coin"]) for m in mismatches}
    modules = ["combo_10day", "combo_monthly"]
    stale_files = []

    for chunk_dir in sorted(root.glob("chunk_*")):
        for module in modules:
            for subdir_name in (f"signatures/{module}", f"{module}_patterns"):
                base = chunk_dir / subdir_name
                if not base.exists():
                    continue
                for strat, coin in targets:
                    coin_dir = base / strat / _safe_coin(coin)
                    if coin_dir.exists() and coin_dir.is_dir():
                        for f in coin_dir.rglob("*"):
                            if f.is_file():
                                stale_files.append(f)

    return stale_files


def delete_files_and_commit(stale_files, repo_root="."):
    """فایل‌های stale را با git rm حذف و commit/push می‌کند. اگر پوشه‌ای بعد از
    حذف فایل‌هایش خالی بماند، خودش را هم حذف می‌کند (برای تمیزی)."""
    if not stale_files:
        log("  هیچ فایل stale ای برای حذف پیدا نشد.")
        return 0

    log(f"  {len(stale_files)} فایل stale برای حذف پیدا شد.")
    rel_paths = []
    for f in stale_files:
        try:
            rel = str(f.relative_to(repo_root))
        except ValueError:
            rel = str(f)
        rel_paths.append(rel)

    # حذف با git rm (به‌جای os.remove) تا مستقیماً stage شود
    CHUNK = 200
    for i in range(0, len(rel_paths), CHUNK):
        batch = rel_paths[i:i + CHUNK]
        cmd = ["git", "rm", "-f", "--quiet"] + batch
        result = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
        if result.returncode != 0:
            log(f"  ⚠️ git rm روی یک batch با خطا مواجه شد: {result.stderr[:300]}", 'WARNING')

    # پاک‌سازی پوشه‌های خالی باقی‌مانده (git خودش پوشه‌ی خالی track نمی‌کند، ولی
    # روی دیسک ممکنه باقی بمونه؛ برای تمیزی فایل‌سیستم لوکال حذفشون می‌کنیم)
    for f in stale_files:
        d = f.parent
        while d.exists() and d != Path(repo_root) and not any(d.iterdir()):
            try:
                d.rmdir()
            except Exception:
                break
            d = d.parent

    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_root)
    if diff.returncode == 0:
        log("  ℹ️ چیزی برای commit نبود (فایل‌ها از قبل حذف شده بودند؟).")
        return 0

    subprocess.run(["git", "commit", "-m", f"purge {len(rel_paths)} session-mismatched fixed outputs"],
                    cwd=repo_root, check=True)

    push_ok = False
    import random
    for attempt in range(5):
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
        r = subprocess.run(["git", "push", "origin", "main"], cwd=repo_root, capture_output=True, text=True)
        if r.returncode == 0:
            push_ok = True
            break
        import time
        time.sleep(random.randint(3, 12))
    if not push_ok:
        log("  ❌ push حذف فایل‌ها به مخزن اصلی ناموفق بود.", 'ERROR')
        raise RuntimeError("push حذف فایل‌های stale ناموفق بود")

    log(f"  ✅ {len(rel_paths)} فایل حذف و push شد.")
    return len(rel_paths)


# ---------------------------------------------------------------------------
# بخش ۲: افزودن دوباره‌ی ترکیب‌های درست (با session) به all_combinations.json
# ---------------------------------------------------------------------------

def build_requeue_items(mismatches):
    items = []
    for m in mismatches:
        strat, coin = m["strat"], m["coin"]
        for session in m["sessions"]:
            for module, intervals in baq.MODULE_INTERVALS.items():
                for interval in intervals:
                    for model in baq.MODELS:
                        items.append({
                            "module": module,
                            "strat": strat,
                            "coin": coin,
                            "interval": interval,
                            "model": model,
                            "session": session,
                        })
    return items


def requeue_to_third_repo(repo, token, new_items):
    if not new_items:
        log("  چیزی برای ری‌کیو نیست.")
        return 0

    log(f"  دانلود all_combinations.json فعلی از {repo}...")
    sha = baq.get_file_sha(repo, "all_combinations.json")
    content = baq.gh_api_get(repo, "all_combinations.json")
    try:
        current = json.loads(content) if content else []
        if not isinstance(current, list):
            current = []
    except Exception:
        current = []

    existing_keys = {
        (i.get("module"), i.get("strat"), i.get("coin"), i.get("interval"), i.get("model"), i.get("session"))
        for i in current if isinstance(i, dict)
    }
    added = 0
    for item in new_items:
        key = (item["module"], item["strat"], item["coin"], item["interval"], item["model"], item["session"])
        if key in existing_keys:
            continue
        current.append(item)
        existing_keys.add(key)
        added += 1

    if added == 0:
        log("  همه‌ی آیتم‌های ری‌کیو از قبل در صف بودند — چیزی اضافه نشد.")
        return 0

    tmp_path = "/tmp/all_combinations_requeue.json"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(current, f, ensure_ascii=False)

    ok = baq.upload_file_with_curl(
        repo, "all_combinations.json", tmp_path, sha=sha,
        commit_msg=f"requeue {added} session-mismatched combos with correct --session"
    )
    if not ok:
        raise RuntimeError("آپلود all_combinations.json ری‌کیوشده ناموفق بود")

    log(f"  ✅ {added} ترکیب جدید (با session درست) به all_combinations.json اضافه شد.")
    return added


def main():
    parser = argparse.ArgumentParser(description="پاک‌سازی و ری‌کیو خروجی‌های fixed که سشن اشتباه داشتند")
    parser.add_argument("--apply", action="store_true", help="واقعاً حذف/ری‌کیو کن (پیش‌فرض: فقط گزارش)")
    parser.add_argument("--scan-only", action="store_true", help="فقط اسکن و گزارش، هیچ تغییری اعمال نشود (پیش‌فرض)")
    parser.add_argument("--batch-outputs-root", default=os.environ.get("BATCH_OUTPUTS_ROOT", "batch_outputs"))
    args = parser.parse_args()

    apply_changes = args.apply and not args.scan_only

    third_repo = os.environ.get('THIRD_REPO')
    gh_token = os.environ.get('GH_TOKEN')
    results_password = os.environ.get('RESULTS_PASSWORD') or os.environ.get('DATA_PASSWORD')

    if not third_repo or not gh_token or not results_password:
        log("THIRD_REPO / GH_TOKEN / RESULTS_PASSWORD تنظیم نشده. خروج.", 'ERROR')
        sys.exit(1)

    log("=" * 60)
    log(f"🔍 اسکن _internal_all_results.json برای یافتن خروجی‌های سشن-نادرست (mode={'APPLY' if apply_changes else 'SCAN-ONLY'})")
    log("=" * 60)

    records = baq.fetch_internal_all_results(third_repo, results_password)
    mismatches = compute_mismatches(records)

    if not mismatches:
        log("✅ هیچ (استراتژی, کوین) نادرستی پیدا نشد — چیزی برای پاک‌سازی وجود ندارد.")
        sys.exit(0)

    log("")
    log(f"📋 لیست کامل موارد نادرست ({len(mismatches)} مورد):")
    for m in mismatches:
        log(f"   - {m['strat']} / {m['coin']} / سشن‌ها: {', '.join(m['sessions'])}")

    with open("/tmp/session_mismatches.json", "w", encoding="utf-8") as f:
        json.dump(mismatches, f, ensure_ascii=False, indent=2)
    log("📄 لیست کامل در /tmp/session_mismatches.json ذخیره شد.")

    stale_files = find_stale_output_files(args.batch_outputs_root, mismatches)
    log(f"🗑️ تعداد فایل خروجی نادرست پیداشده در {args.batch_outputs_root}: {len(stale_files)}")

    requeue_items = build_requeue_items(mismatches)
    log(f"🔁 تعداد ترکیب برای ری‌کیو (با --session درست): {len(requeue_items)}")

    if not apply_changes:
        log("")
        log("ℹ️ حالت scan-only — هیچ فایلی حذف و هیچ صفی تغییر نکرد. برای اعمال واقعی، با --apply اجرا کن.")
        sys.exit(0)

    log("")
    log("🗑️ در حال حذف فایل‌های نادرست از مخزن اصلی...")
    deleted_count = delete_files_and_commit(stale_files, repo_root=".")

    log("")
    log("🔁 در حال ری‌کیو کردن ترکیب‌ها با session درست در مخزن سوم...")
    added_count = requeue_to_third_repo(third_repo, gh_token, requeue_items)

    log("=" * 60)
    log("📊 خلاصه:")
    log(f"   (استراتژی,کوین) نادرست شناسایی‌شده: {len(mismatches)}")
    log(f"   فایل حذف‌شده از batch_outputs: {deleted_count}")
    log(f"   ترکیب جدید اضافه‌شده به all_combinations.json: {added_count}")
    log("✅ پاک‌سازی و ری‌کیو تمام شد.")
    log("=" * 60)


if __name__ == "__main__":
    main()
