#!/usr/bin/env python3
# build_golden_queue.py - تولید و به‌روزرسانی all_combinations_golden.json
# منطق: signatureهای جدید را از signature_archives/ پیدا کن و به صف اضافه کن
# تنها مکانیزم dedup: completed_golden.json (مثل completed_strategies.json)

import os
import sys
import json
import argparse
import subprocess
import base64

# ================================ توابع کمکی (مشابه build_work_queue.py) ================================

def gh_api_get(repo, path):
    """دریافت محتوای یک فایل از مخزن سوم با gh api"""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.content' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return base64.b64decode(result.stdout.strip()).decode('utf-8')

def upload_file_with_curl(repo, remote_path, local_file_path, sha=None):
    """آپلود یا به‌روزرسانی فایل در مخزن سوم با curl"""
    with open(local_file_path, 'rb') as f:
        content_bytes = f.read()
    encoded = base64.b64encode(content_bytes).decode()

    temp_payload = "/tmp/payload_golden.json"
    with open(temp_payload, 'w') as f:
        f.write('{"message":"update golden queue","content":"')
    with open(temp_payload, 'ab') as f:
        f.write(encoded.encode())
    with open(temp_payload, 'a') as f:
        f.write('","branch":"main"')
        if sha:
            f.write(f',"sha":"{sha}"')
        f.write('}')

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
    os.unlink(temp_payload)

    if result.returncode != 0:
        print(f"⚠️ خطا در curl: {result.stderr}")
        return False

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
    """دریافت sha یک فایل از مخزن سوم"""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.sha' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return None

# ================================ تابع دریافت signatures از آرشیوها ================================

def get_all_signatures_from_archives(repo):
    """
    لیست تمام signatureهای موجود در signature_archives/ را دریافت کن.
    از GitHub API برای لیست کردن فایل‌های .tar.gz.enc استفاده می‌کنیم
    — بدون نیاز به دانلود و رمزگشایی.
    هر فایل در مسیر: signature_archives/<module>/<strategy>/<coin>/<filename>.jsonl.tar.gz.enc
    ما نام فایل‌ها را (بدون پسوند .tar.gz.enc) به‌عنوان signature در نظر می‌گیریم.
    ساختار آیتم صف: {"signature_path": "combo_10day/MyStrategy/BTCUSDT/file.jsonl"}
    """
    print("📥 دریافت لیست signatureها از signature_archives/...")

    # دریافت زیرشاخه‌های اول (modules مثل combo_10day, combo_monthly)
    cmd = f"gh api repos/{repo}/contents/signature_archives --jq '.[].name' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        print("⚠️ هیچ محتوایی در signature_archives/ یافت نشد.")
        return []

    modules = [m.strip() for m in result.stdout.strip().split('\n') if m.strip()]
    print(f"   ماژول‌های یافت‌شده: {modules}")

    all_signatures = []

    for module in modules:
        # دریافت استراتژی‌های زیر هر ماژول
        cmd = f"gh api repos/{repo}/contents/signature_archives/{module} --jq '.[].name' 2>/dev/null"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0 or not result.stdout.strip():
            continue
        strategies = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]

        for strategy in strategies:
            # دریافت کوین‌ها زیر هر استراتژی
            cmd = f"gh api repos/{repo}/contents/signature_archives/{module}/{strategy} --jq '.[].name' 2>/dev/null"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0 or not result.stdout.strip():
                continue
            coins = [c.strip() for c in result.stdout.strip().split('\n') if c.strip()]

            for coin in coins:
                # دریافت فایل‌های .tar.gz.enc
                path = f"signature_archives/{module}/{strategy}/{coin}"
                cmd = f"gh api repos/{repo}/contents/{path} --jq '.[].name' 2>/dev/null"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode != 0 or not result.stdout.strip():
                    continue
                files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]

                for fname in files:
                    if fname.endswith('.tar.gz.enc'):
                        # حذف پسوند .tar.gz.enc برای گرفتن نام اصلی jsonl
                        base_name = fname[:-len('.tar.gz.enc')]
                        sig_path = f"{module}/{strategy}/{coin}/{base_name}"
                        all_signatures.append({"signature_path": sig_path})

    print(f"   📊 تعداد کل signatureهای یافت‌شده: {len(all_signatures)}")
    return all_signatures

# ================================ تابع اصلی ================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--progress", default="/tmp/dummy", help="برای سازگاری با build_work_queue.py")
    parser.add_argument("--strategies", default=None, help="برای سازگاری (استفاده نمی‌شود)")
    parser.add_argument("--output-dir", default="/tmp/dummy", help="برای سازگاری")
    parser.add_argument("--batch-size", type=int, default=100, help="سایز بسته")
    args = parser.parse_args()

    third_repo = os.environ.get('THIRD_REPO')
    gh_token = os.environ.get('GH_TOKEN')
    if not third_repo or not gh_token:
        print("❌ THIRD_REPO یا GH_TOKEN تنظیم نشده است.")
        sys.exit(1)

    # ========== 1. دانلود completed_golden.json (تنها معیار dedup) ==========
    print("📥 دانلود completed_golden.json از مخزن سوم...")
    completed_content = gh_api_get(third_repo, "completed_golden.json")
    if completed_content is not None:
        try:
            completed_golden = json.loads(completed_content)
            if not isinstance(completed_golden, list):
                completed_golden = []
        except:
            completed_golden = []
    else:
        completed_golden = []

    completed_set = set(completed_golden)
    print(f"   تعداد signatureهای قبلاً ثبت‌شده: {len(completed_set)}")

    # ========== 2. دریافت تمام signatureها از آرشیوها ==========
    all_signatures = get_all_signatures_from_archives(third_repo)
    if not all_signatures:
        print("✅ هیچ signature‌ای در آرشیوها یافت نشد. کاری انجام نمی‌شود.")
        sys.exit(0)

    # ========== 3. فیلتر: فقط signatureهایی که در completed_golden نیستند ==========
    new_signatures = [
        s for s in all_signatures
        if s["signature_path"] not in completed_set
    ]
    print(f"   signatureهای جدید (نه در completed_golden): {len(new_signatures)}")

    if not new_signatures:
        print("✅ هیچ signature جدیدی برای اضافه کردن وجود ندارد.")
        sys.exit(0)

    # ========== 4. دانلود all_combinations_golden.json موجود ==========
    print("📥 دانلود all_combinations_golden.json از مخزن سوم...")
    combos_content = gh_api_get(third_repo, "all_combinations_golden.json")
    if combos_content is not None:
        try:
            existing_combos = json.loads(combos_content)
            if not isinstance(existing_combos, list):
                existing_combos = []
        except:
            existing_combos = []
    else:
        existing_combos = []
    print(f"   ترکیب‌های موجود در صف: {len(existing_combos)}")

    # ========== 5. اضافه کردن signatureهای جدید به انتهای صف ==========
    updated_combos = existing_combos + new_signatures
    print(f"   📊 تعداد کل ترکیب‌ها پس از اضافه شدن: {len(updated_combos)}")

    # ========== 6. آپلود all_combinations_golden.json ==========
    tmp_file = "/tmp/all_combinations_golden.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(updated_combos, f, indent=2, ensure_ascii=False)

    print("📤 آپلود all_combinations_golden.json...")
    sha_combos = get_file_sha(third_repo, "all_combinations_golden.json")
    if upload_file_with_curl(third_repo, "all_combinations_golden.json", tmp_file, sha_combos):
        print("✅ all_combinations_golden.json با موفقیت آپلود شد.")
    else:
        print("❌ خطا در آپلود all_combinations_golden.json")
        sys.exit(1)

    # ========== 7. آپدیت completed_golden.json ==========
    # باید بلافاصله بعد از آپلود موفق all_combinations_golden.json انجام شود
    # چون analysis.yml آیتم‌ها را یکی‌یکی از صف حذف می‌کند و نمی‌توان به وجود آن‌ها اتکا کرد
    new_sig_paths = [s["signature_path"] for s in new_signatures]
    updated_completed = completed_golden + new_sig_paths

    print(f"📤 آپدیت completed_golden.json با {len(new_sig_paths)} signature جدید...")
    tmp_completed = "/tmp/completed_golden.json"
    with open(tmp_completed, "w", encoding="utf-8") as f:
        json.dump(updated_completed, f, indent=2, ensure_ascii=False)

    sha_completed = get_file_sha(third_repo, "completed_golden.json")
    if upload_file_with_curl(third_repo, "completed_golden.json", tmp_completed, sha_completed):
        print("✅ completed_golden.json با موفقیت آپدیت شد.")
    else:
        print("❌ خطا در آپدیت completed_golden.json — all_combinations_golden.json آپلود شد")
        print("   اما completed_golden آپدیت نشد. در اجرای بعدی همین signatureها دوباره پردازش می‌شوند.")
        sys.exit(1)

    print("✅ عملیات ساخت صف golden با موفقیت پایان یافت.")

if __name__ == "__main__":
    main()
