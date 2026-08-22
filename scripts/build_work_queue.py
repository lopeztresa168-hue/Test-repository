#!/usr/bin/env python3
# build_work_queue.py - تولید و به‌روزرسانی all_combinations.json و completed_strategies.json
# اصلاح شده با استفاده از curl و فایل موقت برای جلوگیری از Argument list too long

import os
import sys
import json
import argparse
import subprocess
import base64
from pathlib import Path

# ================================ توابع کمکی برای API (با curl) ================================
def gh_api_get(repo, path):
    """دریافت محتوای یک فایل از مخزن سوم با gh api (برای خواندن)"""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.content' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return base64.b64decode(result.stdout.strip()).decode('utf-8')

def upload_file_with_curl(repo, remote_path, local_file_path, sha=None):
    """
    آپلود یا به‌روزرسانی فایل در مخزن سوم با استفاده از curl و فایل موقت.
    این روش از Argument list too long جلوگیری می‌کند.
    """
    # خواندن محتوای فایل محلی و تبدیل به base64
    with open(local_file_path, 'rb') as f:
        content_bytes = f.read()
    encoded = base64.b64encode(content_bytes).decode()

    # ساخت فایل موقت برای payload
    temp_payload = "/tmp/payload.json"
    with open(temp_payload, 'w') as f:
        f.write('{"message":"upload work queue","content":"')
    with open(temp_payload, 'ab') as f:
        f.write(encoded.encode())
    with open(temp_payload, 'a') as f:
        f.write('","branch":"main"')
        if sha:
            f.write(f',"sha":"{sha}"')
        f.write('}')

    # ارسال درخواست با curl
    token = os.environ.get('GH_TOKEN')
    if not token:
        print("❌ GH_TOKEN تنظیم نشده است.")
        return False

    cmd = [
        "curl", "-s", "-X", "PUT",
        "-H", f"Authorization: token {token}",
        "-H", "Content-Type: application/json",
        "-d", f"@{temp_payload}",
        f"https://api.github.com/repos/{repo}/contents/{remote_path}"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # پاک کردن فایل موقت
    os.unlink(temp_payload)

    if result.returncode != 0:
        print(f"⚠️ خطا در curl: {result.stderr}")
        return False

    # بررسی پاسخ
    try:
        resp = json.loads(result.stdout)
        if 'content' in resp:
            return True
        else:
            print(f"⚠️ پاسخ ناموفق: {resp.get('message', 'خطای ناشناخته')}")
            return False
    except:
        print(f"⚠️ خطا در parse پاسخ: {result.stdout[:200]}")
        return False

def get_file_sha(repo, path):
    """دریافت sha یک فایل از مخزن سوم (با gh api)"""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.sha' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return None

# ================================ توابع تولید ترکیب ================================
def generate_all_combinations_for_strategy(strategy_name, coins, intervals, models, modules_fixed):
    """
    تولید تمام پرانتزهای ممکن برای یک استراتژی مشخص
    هر پرانتز: (module, strat, coin, interval, model)
    بازگشت: لیستی از دیکشنری‌ها
    """
    combos = []
    for mod in modules_fixed:
        for coin in coins:
            for interval in intervals:
                for model in models:
                    combos.append({
                        "module": mod,
                        "strat": strategy_name,
                        "coin": coin,
                        "interval": interval,
                        "model": model
                    })
    return combos

# ================================ تابع اصلی ================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--progress", required=True, help="مسیر analysis_progress.json (دیگر استفاده نمی‌شود، فقط برای سازگاری)")
    parser.add_argument("--strategies", required=True, help="مسیر strategies.txt (لیست استراتژی‌ها از aggregated)")
    parser.add_argument("--output-dir", required=True, help="مسیر موقت خروجی (استفاده نمی‌شود، فقط برای سازگاری)")
    parser.add_argument("--batch-size", type=int, default=100, help="سایز بسته (برای نمایش)")
    args = parser.parse_args()

    # خواندن لیست استراتژی‌ها
    with open(args.strategies, 'r') as f:
        strategies = [line.strip() for line in f if line.strip()]

    if not strategies:
        print("❌ هیچ استراتژی یافت نشد.")
        sys.exit(0)

    # تنظیمات ثابت (از کد قبلی)
    fixed_modules = ["combo_10day", "combo_monthly"]
    coins = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT",
             "BTCUSDT+ETHUSDT", "BTCUSDT+XRPUSDT", "BTCUSDT+SOLUSDT",
             "ETHUSDT+XRPUSDT", "ETHUSDT+SOLUSDT", "XRPUSDT+SOLUSDT",
             "BTCUSDT+ETHUSDT+XRPUSDT", "BTCUSDT+ETHUSDT+SOLUSDT",
             "BTCUSDT+XRPUSDT+SOLUSDT", "ETHUSDT+XRPUSDT+SOLUSDT",
             "BTCUSDT+ETHUSDT+XRPUSDT+SOLUSDT"]
    intervals = [
        "fixed_5d","fixed_10d","fixed_15d","fixed_30d",
        "CPI_pre_1d","CPI_pre_2d","CPI_pre_3d","CPI_pre_5d",
        "CPI_post_1d","CPI_post_2d","CPI_post_3d","CPI_post_5d","CPI_post_7d","CPI_post_10d","CPI_post_15d",
        "CoreCPI_pre_1d","CoreCPI_pre_2d","CoreCPI_pre_3d","CoreCPI_pre_5d",
        "CoreCPI_post_1d","CoreCPI_post_2d","CoreCPI_post_3d","CoreCPI_post_5d","CoreCPI_post_7d","CoreCPI_post_10d","CoreCPI_post_15d",
        "PPI_pre_1d","PPI_pre_2d","PPI_pre_3d","PPI_pre_5d",
        "PPI_post_1d","PPI_post_2d","PPI_post_3d","PPI_post_5d","PPI_post_7d","PPI_post_10d","PPI_post_15d",
        "CorePPI_pre_1d","CorePPI_pre_2d","CorePPI_pre_3d","CorePPI_pre_5d",
        "CorePPI_post_1d","CorePPI_post_2d","CorePPI_post_3d","CorePPI_post_5d","CorePPI_post_7d","CorePPI_post_10d","CorePPI_post_15d",
        "FOMC_pre_1d","FOMC_pre_2d","FOMC_pre_3d","FOMC_pre_5d",
        "FOMC_post_1d","FOMC_post_2d","FOMC_post_3d","FOMC_post_5d","FOMC_post_7d","FOMC_post_10d","FOMC_post_15d",
        "CPI_y_y_pre_1d","CPI_y_y_pre_2d","CPI_y_y_pre_3d","CPI_y_y_pre_5d",
        "CPI_y_y_post_1d","CPI_y_y_post_2d","CPI_y_y_post_3d","CPI_y_y_post_5d","CPI_y_y_post_7d","CPI_y_y_post_10d","CPI_y_y_post_15d"
    ]
    models = ["simple_hybrid", "fibonacci_full", "fibonacci_hybrid"]

    third_repo = os.environ.get('THIRD_REPO')
    gh_token = os.environ.get('GH_TOKEN')
    if not third_repo or not gh_token:
        print("❌ THIRD_REPO یا GH_TOKEN تنظیم نشده است.")
        sys.exit(1)

    # ========== 1. دانلود فایل‌های موجود از مخزن سوم ==========
    print("📥 دانلود all_combinations.json از مخزن سوم...")
    all_combos_content = gh_api_get(third_repo, "all_combinations.json")
    if all_combos_content is not None:
        try:
            existing_combos = json.loads(all_combos_content)
            if not isinstance(existing_combos, list):
                existing_combos = []
        except:
            existing_combos = []
    else:
        existing_combos = []

    print("📥 دانلود completed_strategies.json از مخزن سوم...")
    completed_content = gh_api_get(third_repo, "completed_strategies.json")
    if completed_content is not None:
        try:
            completed_strategies = json.loads(completed_content)
            if not isinstance(completed_strategies, list):
                completed_strategies = []
        except:
            completed_strategies = []
    else:
        completed_strategies = []

    # ========== 2. استخراج استراتژی‌هایی که نیاز به ترکیب جدید دارند ==========
    strategies_to_process = [s for s in strategies if s not in completed_strategies]
    if not strategies_to_process:
        print("✅ هیچ استراتژی جدیدی برای ترکیب‌سازی وجود ندارد.")
        sys.exit(0)

    print(f"🔄 پردازش استراتژی‌های جدید: {strategies_to_process}")

    # ========== 3. تولید ترکیب‌های جدید ==========
    new_combos = []
    for strat in strategies_to_process:
        combos = generate_all_combinations_for_strategy(strat, coins, intervals, models, fixed_modules)
        new_combos.extend(combos)
    print(f"📊 تعداد ترکیب‌های جدید تولید شده: {len(new_combos)}")

    # ========== 4. اضافه کردن ترکیب‌های جدید به انتهای لیست موجود ==========
    updated_combos = existing_combos + new_combos
    print(f"📊 تعداد کل ترکیب‌ها پس از اضافه شدن: {len(updated_combos)}")

    # ========== 5. نوشتن فایل موقت محلی ==========
    tmp_file = "/tmp/all_combinations.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(updated_combos, f, indent=2, ensure_ascii=False)

    # ========== 6. آپلود با curl (بدون Argument list too long) ==========
    print("📤 آپلود all_combinations.json...")
    sha_all = get_file_sha(third_repo, "all_combinations.json")
    if upload_file_with_curl(third_repo, "all_combinations.json", tmp_file, sha_all):
        print("✅ all_combinations.json با موفقیت آپلود شد.")
    else:
        print("❌ خطا در آپلود all_combinations.json")
        sys.exit(1)

    # ========== 7. آپدیت completed_strategies.json با استراتژی‌های تازه‌پردازش‌شده ==========
    # نکته مهم: این کار باید همین‌جا و بلافاصله بعد از آپلود موفق all_combinations.json
    # انجام شود، چون یک اسکریپت دیگر ترکیب‌ها را یکی‌یکی از all_combinations.json حذف می‌کند.
    # یعنی نمی‌توان بر اساس وجود/عدم‌وجود یک ترکیب در آن فایل تشخیص داد که استراتژی قبلاً
    # پردازش شده یا نه. تنها معیار قابل‌اعتماد، ثبت اسم استراتژی در completed_strategies.json
    # است، در همان لحظه‌ای که ترکیب‌هایش نوشته می‌شوند.
    updated_completed = completed_strategies + strategies_to_process
    print(f"📤 آپدیت completed_strategies.json با {len(strategies_to_process)} استراتژی جدید...")
    tmp_completed = "/tmp/completed_strategies.json"
    with open(tmp_completed, "w", encoding="utf-8") as f:
        json.dump(updated_completed, f, indent=2, ensure_ascii=False)
    sha_completed = get_file_sha(third_repo, "completed_strategies.json")
    if upload_file_with_curl(third_repo, "completed_strategies.json", tmp_completed, sha_completed):
        print("✅ completed_strategies.json با موفقیت آپدیت شد.")
    else:
        print("❌ خطا در آپدیت completed_strategies.json — توجه: all_combinations.json آپلود شد")
        print("   ولی completed_strategies آپدیت نشد. در اجرای بعدی همین استراتژی‌ها دوباره پردازش می‌شوند.")
        sys.exit(1)

    print("✅ عملیات ساخت و به‌روزرسانی صف با موفقیت پایان یافت.")

if __name__ == "__main__":
    main()
