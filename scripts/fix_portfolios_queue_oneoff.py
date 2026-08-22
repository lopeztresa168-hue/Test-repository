"""
اسکریپت one-off برای اصلاح فوری all_combinations_portfolios.json روی مخزن سوم.

چرا لازم است:
  با پچ build_all_queues.py، از این به بعد signatureهای جدید هم archive_path
  می‌گیرند. اما آیتم‌هایی که همین الان (قبل از پچ) بدون archive_path در
  all_combinations_portfolios.json نوشته شده‌اند، فقط وقتی migrate می‌شوند که
  build_portfolios_queue() دوباره اجرا شود *و* signature جدیدی از
  golden_scores.parquet پیدا کند (چون تابع در صورت نبود signature جدید زودتر
  return می‌کند و اصلاً به بخش migration نمی‌رسد). اگر فعلاً signature جدیدی
  در صف نیست، صف خراب می‌ماند و هر اجرای analysis_portfolios.yml باز هم با
  FileNotFoundError شکست می‌خورد.

این اسکریپت مستقل از وجود signature جدید، همین الان صف موجود را می‌خواند،
نگاشت signature_path -> archive_path را از روی آرشیوهای واقعی می‌سازد، و
صف را روی مخزن سوم بازنویسی می‌کند. کاملاً غیرمخرب است — هیچ آیتمی حذف یا
duplicate نمی‌شود، فقط کلید archive_path به آیتم‌های فاقد آن اضافه می‌شود.

استفاده:
  export THIRD_REPO="owner/repo"
  export GH_TOKEN="..."
  export RESULTS_PASSWORD="..."   # یا DATA_PASSWORD
  python fix_portfolios_queue_oneoff.py

نکته: این فایل باید کنار build_all_queues.py (یا در مسیری که قابل import
باشد) قرار بگیرد، چون توابع کمکی را از آن import می‌کند.
"""

import os
import sys
import json

# فرض بر این است که build_all_queues.py در همان دایرکتوری یا در PYTHONPATH است
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from build_all_queues import (
    log,
    gh_api_get,
    get_all_signatures_from_archives,
    _migrate_legacy_queue_format,
)

REMOTE_PATH = "all_combinations_portfolios.json"


def main():
    third_repo = os.environ.get("THIRD_REPO")
    gh_token = os.environ.get("GH_TOKEN")
    results_password = os.environ.get("RESULTS_PASSWORD") or os.environ.get("DATA_PASSWORD")

    if not third_repo or not gh_token or not results_password:
        log("THIRD_REPO / GH_TOKEN / RESULTS_PASSWORD (یا DATA_PASSWORD) باید تنظیم شده باشند.", "ERROR")
        sys.exit(1)

    log("=" * 60)
    log(f"🔧 اصلاح one-off صف portfolios روی {third_repo}")
    log("=" * 60)

    log(f"  دانلود {REMOTE_PATH}...")
    content = gh_api_get(third_repo, REMOTE_PATH)
    if content is None:
        log(f"  {REMOTE_PATH} روی مخزن پیدا نشد — کاری برای انجام نیست.", "WARNING")
        return

    try:
        existing_combos = json.loads(content)
        if not isinstance(existing_combos, list):
            log(f"  {REMOTE_PATH} یک آرایه JSON نیست — خروج بدون تغییر.", "ERROR")
            sys.exit(1)
    except Exception as e:
        log(f"  خواندن {REMOTE_PATH} ناموفق بود: {e}", "ERROR")
        sys.exit(1)

    total = len(existing_combos)
    missing_before = sum(
        1 for item in existing_combos
        if isinstance(item, dict) and "archive_path" not in item
    )
    log(f"  تعداد کل آیتم‌های صف: {total} — بدون archive_path: {missing_before}")

    if missing_before == 0:
        log("  همه‌ی آیتم‌ها از قبل archive_path دارند — نیازی به migrate نیست ✅")
        return

    log("  ساخت نگاشت signature_path -> archive_path از روی آرشیوهای واقعی...")
    archive_signatures_for_map = get_all_signatures_from_archives(
        third_repo, results_password, key_name="signature_path"
    )
    sig_to_archive = {
        s["signature_path"]: s.get("archive_path")
        for s in archive_signatures_for_map
        if isinstance(s, dict) and "signature_path" in s
    }
    log(f"  تعداد signature در نگاشت آرشیوها: {len(sig_to_archive)}")

    migrated, changed = _migrate_legacy_queue_format(
        third_repo, REMOTE_PATH, existing_combos, sig_to_archive,
        "one-off fix: add missing archive_path to portfolios queue"
    )

    if not changed:
        log("  چیزی برای migrate کردن پیدا نشد (غیرمنتظره).", "WARNING")
        return

    missing_after = sum(
        1 for item in migrated
        if isinstance(item, dict) and not item.get("archive_path")
    )
    log(f"  ✅ migrate تمام شد. آیتم‌های همچنان بدون archive_path (آرشیو یافت نشد): {missing_after}/{total}")
    if missing_after:
        log(
            "  ⚠️ این تعداد آیتم در هیچ آرشیوی پیدا نشدند و توسط فیلتر jq در "
            "analysis_portfolios.yml رد خواهند شد — یعنی هرگز پردازش نمی‌شوند. "
            "لازم است بررسی شود که archive این signatureها هنوز در signature_archives/ هست یا نه.",
            "WARNING"
        )


if __name__ == "__main__":
    main()
