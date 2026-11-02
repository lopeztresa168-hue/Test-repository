#!/usr/bin/env python3
#  - تولید و به‌روزرسانی all_combinations_correlation.json
# منطق: دقیقاً مثل golden — از همان signatureهای signature_archives/ استفاده می‌کند
# تنها مکانیزم dedup: completed_correlation.json

import os
import sys
import json
import argparse
import subprocess
import base64

# ================================ توابع کمکی ================================

def gh_api_get(repo, path):
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.content' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return base64.b64decode(result.stdout.strip()).decode('utf-8')

def upload_file_with_curl(repo, remote_path, local_file_path, sha=None):
    with open(local_file_path, 'rb') as f:
        content_bytes = f.read()
    encoded = base64.b64encode(content_bytes).decode()

    temp_payload = "/tmp/payload_correlation.json"
    with open(temp_payload, 'w') as f:
        f.write('{"message":"update correlation queue","content":"')
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
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.sha' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return None

# ================================ دریافت signatures ================================

def get_all_signatures_from_archives(repo):
    """
    لیست signatureها از signature_archives/ — دقیقاً مثل golden.
    از GitHub API برای لیست کردن استفاده می‌کنیم (بدون دانلود).
    """
    print("📥 دریافت لیست signatureها از signature_archives/...")

    cmd = f"gh api repos/{repo}/contents/signature_archives --jq '.[].name' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        print("⚠️ هیچ محتوایی در signature_archives/ یافت نشد.")
        return []

    modules = [m.strip() for m in result.stdout.strip().split('\n') if m.strip()]
    print(f"   ماژول‌های یافت‌شده: {modules}")

    all_signatures = []

    for module in modules:
        cmd = f"gh api repos/{repo}/contents/signature_archives/{module} --jq '.[].name' 2>/dev/null"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0 or not result.stdout.strip():
            continue
        strategies = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]

        for strategy in strategies:
            cmd = f"gh api repos/{repo}/contents/signature_archives/{module}/{strategy} --jq '.[].name' 2>/dev/null"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0 or not result.stdout.strip():
                continue
            coins = [c.strip() for c in result.stdout.strip().split('\n') if c.strip()]

            for coin in coins:
                path = f"signature_archives/{module}/{strategy}/{coin}"
                cmd = f"gh api repos/{repo}/contents/{path} --jq '.[].name' 2>/dev/null"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode != 0 or not result.stdout.strip():
                    continue
                files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]

                for fname in files:
                    if fname.endswith('.tar.gz.enc'):
                        base_name = fname[:-len('.tar.gz.enc')]
                        sig_path = f"{module}/{strategy}/{coin}/{base_name}"
                        # ✅ تغییر کلید از signature_path به path
                        all_signatures.append({"path": sig_path})

    print(f"   📊 تعداد کل signatureهای یافت‌شده: {len(all_signatures)}")
    return all_signatures

# ================================ تابع اصلی ================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--progress", default="/tmp/dummy", help="برای سازگاری")
    parser.add_argument("--strategies", default=None, help="برای سازگاری (استفاده نمی‌شود)")
    parser.add_argument("--output-dir", default="/tmp/dummy", help="برای سازگاری")
    parser.add_argument("--batch-size", type=int, default=100, help="سایز بسته")
    args = parser.parse_args()

    third_repo = os.environ.get('THIRD_REPO')
    gh_token = os.environ.get('GH_TOKEN')
    if not third_repo or not gh_token:
        print("❌ THIRD_REPO یا GH_TOKEN تنظیم نشده است.")
        sys.exit(1)

    # ========== 1. دانلود completed_correlation.json ==========
    print("📥 دانلود completed_correlation.json از مخزن سوم...")
    completed_content = gh_api_get(third_repo, "completed_correlation.json")
    if completed_content is not None:
        try:
            completed_correlation = json.loads(completed_content)
            if not isinstance(completed_correlation, list):
                completed_correlation = []
        except:
            completed_correlation = []
    else:
        completed_correlation = []

    completed_set = set(completed_correlation)
    print(f"   تعداد signatureهای قبلاً ثبت‌شده: {len(completed_set)}")

    # ========== 2. دریافت signatureها از آرشیوها ==========
    all_signatures = get_all_signatures_from_archives(third_repo)
    if not all_signatures:
        print("✅ هیچ signature‌ای در آرشیوها یافت نشد. کاری انجام نمی‌شود.")
        sys.exit(0)

    # ========== 3. فیلتر: فقط signatureهای جدید ==========
    # ✅ استفاده از کلید path
    new_signatures = [
        s for s in all_signatures
        if s["path"] not in completed_set
    ]
    print(f"   signatureهای جدید (نه در completed_correlation): {len(new_signatures)}")

    if not new_signatures:
        print("✅ هیچ signature جدیدی برای اضافه کردن وجود ندارد.")
        sys.exit(0)

    # ========== 4. دانلود all_combinations_correlation.json موجود ==========
    print("📥 دانلود all_combinations_correlation.json از مخزن سوم...")
    combos_content = gh_api_get(third_repo, "all_combinations_correlation.json")
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

    # ========== 5. اضافه کردن signatureهای جدید ==========
    updated_combos = existing_combos + new_signatures
    print(f"   📊 تعداد کل ترکیب‌ها پس از اضافه شدن: {len(updated_combos)}")

    # ========== 6. آپلود all_combinations_correlation.json ==========
    tmp_file = "/tmp/all_combinations_correlation.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(updated_combos, f, indent=2, ensure_ascii=False)

    print("📤 آپلود all_combinations_correlation.json...")
    sha_combos = get_file_sha(third_repo, "all_combinations_correlation.json")
    if upload_file_with_curl(third_repo, "all_combinations_correlation.json", tmp_file, sha_combos):
        print("✅ all_combinations_correlation.json با موفقیت آپلود شد.")
    else:
        print("❌ خطا در آپلود all_combinations_correlation.json")
        sys.exit(1)

    # ========== 7. آپدیت completed_correlation.json ==========
    # ✅ استفاده از کلید path
    new_sig_paths = [s["path"] for s in new_signatures]
    updated_completed = completed_correlation + new_sig_paths

    print(f"📤 آپدیت completed_correlation.json با {len(new_sig_paths)} signature جدید...")
    tmp_completed = "/tmp/completed_correlation.json"
    with open(tmp_completed, "w", encoding="utf-8") as f:
        json.dump(updated_completed, f, indent=2, ensure_ascii=False)

    sha_completed = get_file_sha(third_repo, "completed_correlation.json")
    if upload_file_with_curl(third_repo, "completed_correlation.json", tmp_completed, sha_completed):
        print("✅ completed_correlation.json با موفقیت آپدیت شد.")
    else:
        print("❌ خطا در آپدیت completed_correlation.json — all_combinations_correlation.json آپلود شد")
        print("   اما completed_correlation آپدیت نشد. در اجرای بعدی همین signatureها دوباره پردازش می‌شوند.")
        sys.exit(1)

    print("✅ عملیات ساخت صف correlation با موفقیت پایان یافت.")

if __name__ == "__main__":
    main()
