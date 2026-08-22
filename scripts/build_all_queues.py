#!/usr/bin/env python3
# build_all_queues.py - ترکیب سه اسکریپت صف‌ساز با لاگ‌گیری دقیق
# مراحل: work → golden → portfolios
# (ماژول correlation به‌طور کامل حذف شده است)

import os
import sys
import json
import subprocess
import base64
import shutil
from datetime import datetime
from pathlib import Path

# ================================ لاگ‌گیری ================================

LOG_FILE = None  # در main مقداردهی می‌شود

def log(message, level='INFO'):
    """چاپ پیام با timestamp و سطح لاگ — هم stdout هم فایل"""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] [{level}] {message}"
    print(line, flush=True)
    if LOG_FILE:
        try:
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
        except Exception:
            pass

# ================================ توابع مشترک ================================

def _normalize_completed_list(items, source_name=""):
    """تضمین می‌کند که items یک لیست از رشته‌های hashable باشد.

    فایل‌های completed_*.json باید فقط شامل رشته (signature_path) باشند،
    اما اگر به هر دلیلی (نسخه قدیمی اسکریپت، دستکاری دستی، و ...) عناصری
    از نوع list/dict در آن‌ها نفوذ کرده باشد، set(items) با خطای
    "unhashable type" کرش می‌کند. این تابع چنین عناصری را نرمالایز یا حذف
    می‌کند تا خط لوله متوقف نشود.
    """
    normalized = []
    bad_count = 0
    for item in items:
        if isinstance(item, str):
            normalized.append(item)
        elif isinstance(item, dict) and "signature_path" in item:
            normalized.append(str(item["signature_path"]))
            bad_count += 1
        elif isinstance(item, (list, tuple)) and item:
            candidate = next((x for x in item if isinstance(x, str)), None)
            if candidate is not None:
                normalized.append(candidate)
            bad_count += 1
        else:
            bad_count += 1

    if bad_count:
        log(f"  ⚠️ {bad_count} آیتم غیرمعتبر/غیر-رشته‌ای در {source_name} یافت و نرمالایز/حذف شد", 'WARNING')

    return normalized

def gh_api_get(repo, path):
    """دریافت محتوای متنی یک فایل از مخزن.
    برای فایل‌های بزرگ‌تر از ~1MB، GitHub Contents API فیلد .content را خالی
    برمی‌گرداند (truncated=true)، پس باید از download_url استفاده شود — دقیقاً
    مثل gh_api_get_binary. بدون این fallback، فایل‌های بزرگ (مثل
    completed_golden.json بعد از رشد) همیشه «یافت نشد» در نظر گرفته می‌شدند و
    completed_set همیشه خالی می‌ماند."""
    log(f"  [API GET] {repo}/{path}")
    cmd = f"gh api repos/{repo}/contents/{path} 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        log(f"  [API GET] نتیجه: یافت نشد (returncode={result.returncode})", 'WARNING')
        return None

    try:
        meta = json.loads(result.stdout)
    except Exception as e:
        log(f"  [API GET] خطا در parse متادیتا: {e}", 'ERROR')
        return None

    file_size = meta.get('size', 0)
    content_b64 = meta.get('content', '')
    truncated = meta.get('truncated', False)

    if not truncated and content_b64:
        try:
            content = base64.b64decode(content_b64.replace('\n', '')).decode('utf-8')
            log(f"  [API GET] نتیجه: دریافت شد ({len(content)} کاراکتر)")
            return content
        except Exception as e:
            log(f"  [API GET] خطا در decode: {e}", 'ERROR')
            return None

    log(f"  [API GET] فایل بزرگ است ({file_size} bytes, truncated={truncated}) — استفاده از download_url...")
    download_url = meta.get('download_url', '')
    token = os.environ.get('GH_TOKEN')
    if not download_url:
        log("  [API GET] download_url یافت نشد", 'ERROR')
        return None

    dl = subprocess.run(
        ["curl", "-s", "-L", "-H", f"Authorization: token {token}", download_url],
        capture_output=True, text=True
    )
    if dl.returncode != 0 or not dl.stdout:
        log(f"  [API GET] دانلود از download_url ناموفق: {dl.stderr[:200]}", 'ERROR')
        return None

    log(f"  [API GET] نتیجه: دریافت شد از download_url ({len(dl.stdout)} کاراکتر)")
    return dl.stdout

def gh_api_get_binary(repo, path, dest_path):
    """دانلود فایل binary از مخزن (برای فایل‌های بزرگ از download_url استفاده می‌کند)"""
    log(f"  [API GET BINARY] {repo}/{path} → {dest_path}")
    token = os.environ.get('GH_TOKEN')

    cmd = f"gh api repos/{repo}/contents/{path} 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        log(f"  [API GET BINARY] خطا در دریافت metadata (returncode={result.returncode})", 'ERROR')
        return False

    try:
        meta = json.loads(result.stdout)
    except Exception as e:
        log(f"  [API GET BINARY] خطا در parse metadata: {e}", 'ERROR')
        return False

    file_size = meta.get('size', 0)
    download_url = meta.get('download_url', '')
    log(f"  [API GET BINARY] اندازه فایل: {file_size} bytes")

    if file_size > 1_000_000 or not meta.get('content'):
        if not download_url:
            log("  [API GET BINARY] download_url یافت نشد", 'ERROR')
            return False
        log("  [API GET BINARY] دانلود از download_url (فایل بزرگ)...")
        cmd_dl = [
            "curl", "-s", "-L",
            "-H", f"Authorization: token {token}",
            download_url, "-o", dest_path
        ]
        r = subprocess.run(cmd_dl, capture_output=True)
        if r.returncode != 0:
            log(f"  [API GET BINARY] خطا در curl download: {r.stderr}", 'ERROR')
            return False
    else:
        content_b64 = meta.get('content', '').replace('\n', '')
        if not content_b64:
            log("  [API GET BINARY] content خالی در پاسخ API", 'ERROR')
            return False
        decoded = base64.b64decode(content_b64)
        with open(dest_path, 'wb') as f:
            f.write(decoded)

    exists = os.path.exists(dest_path) and os.path.getsize(dest_path) > 0
    if exists:
        log(f"  [API GET BINARY] دانلود موفق ({os.path.getsize(dest_path)} bytes)")
    else:
        log("  [API GET BINARY] فایل دانلود‌شده خالی یا ناموجود است", 'ERROR')
    return exists

def upload_file_with_curl(repo, remote_path, local_file_path, sha=None, commit_msg="update queue"):
    """آپلود فایل با curl (از Argument list too long جلوگیری می‌کند)"""
    log(f"  [UPLOAD] {local_file_path} → {repo}/{remote_path}")
    with open(local_file_path, 'rb') as f:
        content_bytes = f.read()
    encoded = base64.b64encode(content_bytes).decode()

    temp_payload = "/tmp/payload_queue.json"
    with open(temp_payload, 'w') as f:
        f.write(f'{{"message":"{commit_msg}","content":"')
    with open(temp_payload, 'ab') as f:
        f.write(encoded.encode())
    with open(temp_payload, 'a') as f:
        f.write('","branch":"main"')
        if sha:
            f.write(f',"sha":"{sha}"')
        f.write('}')

    token = os.environ.get('GH_TOKEN')
    if not token:
        log("  [UPLOAD] GH_TOKEN تنظیم نشده است", 'ERROR')
        return False

    cmd = [
        "curl", "-s", "-X", "PUT",
        "-H", f"Authorization: token {token}",
        "-H", "Content-Type: application/json",
        "-d", f"@{temp_payload}",
        f"https://api.github.com/repos/{repo}/contents/{remote_path}"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        os.unlink(temp_payload)
    except Exception:
        pass

    if result.returncode != 0:
        log(f"  [UPLOAD] خطا در curl: {result.stderr}", 'ERROR')
        return False

    try:
        resp = json.loads(result.stdout)
        if 'content' in resp:
            log(f"  [UPLOAD] موفق ✅")
            return True
        else:
            log(f"  [UPLOAD] پاسخ ناموفق: {resp.get('message', 'خطای ناشناخته')}", 'ERROR')
            return False
    except Exception:
        log(f"  [UPLOAD] خطا در parse پاسخ: {result.stdout[:200]}", 'ERROR')
        return False

def get_file_sha(repo, path):
    """دریافت SHA یک فایل از مخزن"""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.sha' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip():
        sha = result.stdout.strip()
        log(f"  [SHA] {path}: {sha[:12]}...")
        return sha
    log(f"  [SHA] {path}: یافت نشد (فایل جدید)")
    return None

def decrypt_file(enc_path, dec_path, password):
    """رمزگشایی AES-256-CBC با Node.js"""
    log(f"  [DECRYPT] {enc_path} → {dec_path}")
    pass_file = "/tmp/dec_password.txt"
    with open(pass_file, 'w') as f:
        f.write(password)

    node_code = f"""
const crypto=require('crypto'),fs=require('fs');
const pass=fs.readFileSync('{pass_file}','utf8').trim();
const data=fs.readFileSync('{enc_path}');
const key=crypto.scryptSync(pass,'salt',32);
const iv=data.slice(0,16),enc=data.slice(16);
const decipher=crypto.createDecipheriv('aes-256-cbc',key,iv);
const dec=Buffer.concat([decipher.update(enc),decipher.final()]);
fs.writeFileSync('{dec_path}',dec);
"""
    result = subprocess.run(["node", "-e", node_code], capture_output=True, text=True)
    try:
        os.unlink(pass_file)
    except Exception:
        pass
    if result.returncode != 0:
        log(f"  [DECRYPT] خطای Node.js: {result.stderr[:300]}", 'ERROR')
        return False
    log("  [DECRYPT] رمزگشایی موفق ✅")
    return True

def check_file_exists(repo, path):
    """بررسی وجود فایل در مخزن"""
    cmd = f"gh api repos/{repo}/contents/{path} --jq '.sha' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    exists = result.returncode == 0 and bool(result.stdout.strip())
    log(f"  [CHECK] {path}: {'موجود ✅' if exists else 'ناموجود ❌'}")
    return exists

# ================================ تابع مشترک دریافت signatures از آرشیوها ================================

def _list_jsonl_inside_archive(repo, archive_path, password):
    """
    آرشیو tar.gz.enc را دانلود، رمزگشایی و فهرست فایل‌های .jsonl داخل آن را برمی‌گرداند.

    خروجی:
    - لیستی از مسیرهای نسبی .jsonl همان‌طور که داخل tar هستند (مثلاً
      "signatures/combo_10day/Best_15m/BTCUSDT/fixed_5d_simple_hybrid.jsonl")
    - در صورت بروز هرگونه خطا (دانلود، رمزگشایی یا tar)، None برمی‌گرداند تا
      caller این آرشیو را کاملاً نادیده بگیرد (هیچ‌گاه نام آرشیو را به عنوان
      signature استفاده نکند).
    """
    return _list_files_inside_archive(repo, archive_path, password, suffix='.jsonl')


def _list_csv_inside_archive(repo, archive_path, password):
    """مشابه _list_jsonl_inside_archive، برای فایل‌های .csv داخل آرشیوهای pattern_archives/."""
    return _list_files_inside_archive(repo, archive_path, password, suffix='.csv')


def _list_files_inside_archive(repo, archive_path, password, suffix):
    enc_path = "/tmp/_archive_list_tmp.tar.gz.enc"
    dec_path = "/tmp/_archive_list_tmp.tar.gz"

    for p in (enc_path, dec_path):
        try:
            os.unlink(p)
        except Exception:
            pass

    if not gh_api_get_binary(repo, archive_path, enc_path):
        log(f"  [ARCHIVES] دانلود ناموفق: {archive_path}", 'ERROR')
        return None

    if not decrypt_file(enc_path, dec_path, password):
        log(f"  [ARCHIVES] رمزگشایی ناموفق: {archive_path}", 'ERROR')
        for p in (enc_path, dec_path):
            try:
                os.unlink(p)
            except Exception:
                pass
        return None

    result = subprocess.run(["tar", "-tzf", dec_path], capture_output=True, text=True)
    for p in (enc_path, dec_path):
        try:
            os.unlink(p)
        except Exception:
            pass

    if result.returncode != 0:
        log(f"  [ARCHIVES] tar -tzf ناموفق برای {archive_path}: {result.stderr[:200]}", 'ERROR')
        return None

    entries = [ln.strip() for ln in result.stdout.split('\n') if ln.strip()]
    matched = [e for e in entries if e.endswith(suffix)]

    if not matched:
        log(f"  [ARCHIVES] هیچ فایل {suffix} داخل {archive_path} یافت نشد", 'WARNING')

    return matched


def _load_processed_archives_cache(repo):
    """دانلود processed_archives.json از مخزن سوم. اگر وجود نداشت، لیست خالی برمی‌گرداند."""
    log("  [ARCHIVES] دانلود کش processed_archives.json...")
    cache_content = gh_api_get(repo, "processed_archives.json")
    if cache_content is not None:
        try:
            processed = json.loads(cache_content)
            if not isinstance(processed, list):
                processed = []
        except Exception:
            processed = []
    else:
        processed = []
    processed = _normalize_completed_list(processed, "processed_archives.json")
    log(f"  [ARCHIVES] تعداد آرشیوهای قبلاً پردازش‌شده در کش: {len(processed)}")
    return processed


def _upload_processed_archives_cache(repo, updated_archive_paths, max_retries=3):
    """آپلود اتمیک processed_archives.json.

    برای مدیریت race condition (مثلاً اگر همزمان دو اجرای دیگر هم در حال
    آپدیت این فایل باشند)، هر بار sha تازه گرفته می‌شود و در صورت شکست
    آپلود (مثلاً به دلیل sha منقضی‌شده)، با sha جدید دوباره تلاش می‌شود.
    """
    tmp_file = "/tmp/processed_archives.json"
    dedup_sorted = sorted(set(updated_archive_paths))
    for attempt in range(1, max_retries + 1):
        sha = get_file_sha(repo, "processed_archives.json")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(dedup_sorted, f, indent=2, ensure_ascii=False)
        if upload_file_with_curl(repo, "processed_archives.json", tmp_file, sha, "update processed archives cache"):
            log(f"  [ARCHIVES] کش processed_archives.json به‌روزرسانی شد ✅ ({len(dedup_sorted)} آرشیو)")
            return True
        log(f"  [ARCHIVES] تلاش {attempt}/{max_retries} برای آپلود کش ناموفق بود — تلاش مجدد با sha تازه...", 'WARNING')
    log("  [ARCHIVES] آپلود کش processed_archives.json پس از چند تلاش ناموفق بود", 'ERROR')
    return False


# ================================ ایندکس دائمی signature_path -> archive_path ================================
#
# مشکل قبلی: sig_to_archive در build_portfolios_queue با فراخوانی دوباره‌ی
# get_all_signatures_from_archives ساخته می‌شد، اما آن تابع آرشیوهایی را که
# قبلاً در processed_archives.json کش شده‌اند (یعنی اکثر آرشیوهای قدیمی) به
# طور کامل رد می‌کند (continue) و هیچ signature‌ای از آن‌ها برنمی‌گرداند.
# نتیجه: sig_to_archive فقط شامل آرشیوهای «تازه دیده‌شده در همین اجرا» بود
# و archive_path برای اکثر signatureهای قدیمی = None می‌شد.
#
# راه‌حل: یک فایل ایندکس دائمی (signature_archive_index.json) در مخزن سوم
# نگه می‌داریم که نگاشت signature_path -> archive_path را به صورت تجمعی
# (append-only) ذخیره می‌کند. هر بار که get_all_signatures_from_archives
# آرشیوهای جدیدی پیدا و پردازش می‌کند، نگاشت آن‌ها بلافاصله در این ایندکس
# ذخیره می‌شود — حتی اگر بعداً آیتم مربوطه از all_combinations_golden.json
# حذف شود (چون آن فایل صف است، نه آرشیو تاریخچه). به این ترتیب دیگر هیچ‌وقت
# نیازی به rescan کامل آرشیوها نیست: کافیست همین یک فایل کوچک دانلود شود.

SOURCE_INDEX_PATH = "signature_archive_index.json"

def _load_source_index(repo):
    """دانلود نگاشت دائمی signature_path -> archive_path از مخزن سوم."""
    log(f"  [INDEX] دانلود {SOURCE_INDEX_PATH}...")
    content = gh_api_get(repo, SOURCE_INDEX_PATH)
    if content is None:
        log(f"  [INDEX] {SOURCE_INDEX_PATH} یافت نشد — ایندکس خالی در نظر گرفته می‌شود", 'WARNING')
        return {}
    try:
        index = json.loads(content)
        if not isinstance(index, dict):
            log(f"  [INDEX] محتوای {SOURCE_INDEX_PATH} از نوع dict نیست — نادیده گرفته می‌شود", 'WARNING')
            return {}
    except Exception as e:
        log(f"  [INDEX] خطا در parse {SOURCE_INDEX_PATH}: {e}", 'ERROR')
        return {}
    log(f"  [INDEX] {len(index)} نگاشت signature_path -> archive_path بارگذاری شد")
    return index


def _upload_source_index(repo, new_entries, max_retries=3):
    """ادغام اتمیک new_entries در signature_archive_index.json موجود در مخزن و آپلود آن.

    new_entries: dict {signature_path: archive_path} که باید اضافه/به‌روزرسانی شود.
    برای جلوگیری از race condition، هر تلاش دوباره نسخه‌ی فعلی فایل را از مخزن
    می‌خواند، new_entries را روی آن ادغام می‌کند و با sha تازه آپلود می‌کند.
    """
    if not new_entries:
        return True

    tmp_file = "/tmp/signature_archive_index.json"
    for attempt in range(1, max_retries + 1):
        current = _load_source_index(repo)
        current.update(new_entries)
        sha = get_file_sha(repo, SOURCE_INDEX_PATH)
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=False)
        if upload_file_with_curl(repo, SOURCE_INDEX_PATH, tmp_file, sha, "update signature->archive index"):
            log(f"  [INDEX] {SOURCE_INDEX_PATH} به‌روزرسانی شد ✅ (اکنون {len(current)} نگاشت، {len(new_entries)} مورد جدید/تغییریافته)")
            return True
        log(f"  [INDEX] تلاش {attempt}/{max_retries} برای آپلود ایندکس ناموفق بود — تلاش مجدد با sha تازه...", 'WARNING')
    log(f"  [INDEX] آپلود {SOURCE_INDEX_PATH} پس از چند تلاش ناموفق بود — نگاشت‌های جدید فقط در حافظه‌ی همین اجرا باقی ماندند", 'ERROR')
    return False


def get_all_signatures_from_archives(repo, password, key_name="signature_path", force_refresh=False):
    """
    تمام آرشیوهای signature_archives/{module}/{strategy}/*.tar.gz.enc را پیدا کرده،
    هرکدام را دانلود/رمزگشایی/استخراج‌لیست می‌کند و برای هر فایل .jsonl داخل آن
    یک آیتم {key_name: <مسیر کامل jsonl شامل 'signatures/'>, "archive_path": <مسیر آرشیو>}
    می‌سازد.

    آرشیوهای خراب یا بدون .jsonl به‌طور کامل نادیده گرفته می‌شوند — هیچ‌گاه
    نام آرشیو به عنوان signature بازگردانده نمی‌شود.

    بهینه‌سازی: آرشیوهایی که در کش processed_archives.json (در ریشه‌ی مخزن
    سوم) ثبت شده‌اند، اصلاً دانلود نمی‌شوند. فقط آرشیوهای جدید پردازش شده و
    بلافاصله به کش اضافه می‌شوند تا اجراهای بعدی مجبور به دانلود دوباره‌ی
    آرشیوهای قبلاً پردازش‌شده نباشند.

    اگر force_refresh=True باشد، کش نادیده گرفته می‌شود و همه‌ی آرشیوها دوباره
    پردازش می‌شوند (تازه‌سازی کامل). این حالت برای زمانی استفاده می‌شود که
    نیاز به بازسازی کامل نگاشت signature_path -> archive_path داریم.
    """
    processed_archives = _load_processed_archives_cache(repo) if not force_refresh else []
    processed_set = set(processed_archives)

    if force_refresh:
        log("  [ARCHIVES] force_refresh=True — کش نادیده گرفته می‌شود و همه‌ی آرشیوها دوباره پردازش می‌شوند.")

    log("  [ARCHIVES] دریافت لیست محتوای signature_archives/...")
    cmd = f"gh api repos/{repo}/contents/signature_archives --jq '.[].name' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        log("  [ARCHIVES] هیچ محتوایی در signature_archives/ یافت نشد", 'WARNING')
        return []

    top_level_names = [m.strip() for m in result.stdout.strip().split('\n') if m.strip()]
    log(f"  [ARCHIVES] محتوای سطح اول: {top_level_names}")

    standard_modules = {"combo_10day", "combo_monthly"}
    found_modules = [n for n in top_level_names if n in standard_modules]

    if not found_modules:
        log("  [ARCHIVES] هیچ ماژول استانداردی (combo_10day/combo_monthly) یافت نشد", 'ERROR')
        return []

    archive_paths = []
    for module in found_modules:
        cmd = f"gh api repos/{repo}/contents/signature_archives/{module} --jq '.[].name' 2>/dev/null"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0 or not result.stdout.strip():
            log(f"  [ARCHIVES] ماژول {module}: استراتژی‌ای یافت نشد", 'WARNING')
            continue
        strategies = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]
        log(f"  [ARCHIVES] {module}: {len(strategies)} استراتژی")

        for strategy in strategies:
            path = f"signature_archives/{module}/{strategy}"
            cmd = f"gh api repos/{repo}/contents/{path} --jq '.[].name' 2>/dev/null"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0 or not result.stdout.strip():
                log(f"  [ARCHIVES] {module}/{strategy}: فایلی یافت نشد", 'WARNING')
                continue
            files = [fn.strip() for fn in result.stdout.strip().split('\n') if fn.strip()]
            for fname in files:
                if fname.endswith('.tar.gz.enc'):
                    archive_paths.append(f"signature_archives/{module}/{strategy}/{fname}")

    log(f"  [ARCHIVES] تعداد کل آرشیوهای .tar.gz.enc یافت‌شده: {len(archive_paths)}")

    all_signatures = []
    skipped = 0
    cached_skip = 0
    newly_processed = []
    for archive_path in archive_paths:
        if archive_path in processed_set and not force_refresh:
            cached_skip += 1
            continue

        jsonl_entries = _list_jsonl_inside_archive(repo, archive_path, password)
        if not jsonl_entries:
            skipped += 1
            continue

        for entry in jsonl_entries:
            # حذف پیشوند "signatures/" از entry تا با ستون signature_path در golden_scores.csv مطابقت داشته باشد
            clean_entry = entry
            if clean_entry.startswith("signatures/"):
                clean_entry = clean_entry[11:]  # len("signatures/") == 11
            all_signatures.append({key_name: clean_entry, "archive_path": archive_path})

        if not force_refresh:
            processed_set.add(archive_path)
            newly_processed.append(archive_path)

    log(f"  [ARCHIVES] آرشیوهای رد شده به دلیل وجود در کش (بدون دانلود): {cached_skip}")
    log(f"  [ARCHIVES] آرشیوهای نادیده‌گرفته‌شده (خراب/خالی): {skipped}")
    log(f"  [ARCHIVES] آرشیوهای جدید پردازش‌شده در این اجرا: {len(newly_processed)}")
    log(f"  [ARCHIVES] تعداد کل signatureهای یافت‌شده: {len(all_signatures)}")

    if newly_processed and not force_refresh:
        _upload_processed_archives_cache(repo, processed_archives + newly_processed)

    # نگاشت signature_path -> archive_path هر آرشیو تازه‌پردازش‌شده را به صورت
    # دائمی در signature_archive_index.json ذخیره می‌کنیم — چون processed_set
    # باعث می‌شود این آرشیوها در فراخوانی‌های بعدی دیگر هرگز اسکن نشوند،
    # این تنها فرصت است که نگاشت آن‌ها ثبت شود؛ در غیر این صورت archive_path
    # این signatureها برای همیشه گم می‌شود (باگ اصلی archive_path=null).
    if key_name == "signature_path":
        new_index_entries = {
            s["signature_path"]: s.get("archive_path")
            for s in all_signatures
            if isinstance(s, dict) and s.get("signature_path") and s.get("archive_path")
        }
        if new_index_entries:
            _upload_source_index(repo, new_index_entries)

    return all_signatures


# ================================ نسخه‌ی مجزا برای pattern_archives (CSV آستانه/نسبت_شانس) ================================
# عمداً کاملاً جدا از signature_archive_index.json/processed_archives.json نگه
# داشته شده — هیچ فایل/کش مشترکی با منطق signatures ندارد تا صفر ریسک برای
# سیستم موجود (که قبلاً به‌دقت باگ‌یابی شده) ایجاد شود.

PATTERN_INDEX_PATH = "pattern_archive_index.json"
PATTERN_PROCESSED_CACHE_PATH = "processed_pattern_archives.json"


def _load_pattern_index(repo):
    """دانلود نگاشت دائمی pattern_path -> archive_path از مخزن سوم."""
    log(f"  [PATTERNS] دانلود {PATTERN_INDEX_PATH}...")
    content = gh_api_get(repo, PATTERN_INDEX_PATH)
    if content is None:
        log(f"  [PATTERNS] {PATTERN_INDEX_PATH} یافت نشد — ایندکس خالی در نظر گرفته می‌شود", 'WARNING')
        return {}
    try:
        index = json.loads(content)
        if not isinstance(index, dict):
            log(f"  [PATTERNS] محتوای {PATTERN_INDEX_PATH} از نوع dict نیست — نادیده گرفته می‌شود", 'WARNING')
            return {}
    except Exception as e:
        log(f"  [PATTERNS] خطا در parse {PATTERN_INDEX_PATH}: {e}", 'ERROR')
        return {}
    log(f"  [PATTERNS] {len(index)} نگاشت pattern_path -> archive_path بارگذاری شد")
    return index


def _upload_pattern_index(repo, new_entries, max_retries=3):
    """ادغام اتمیک new_entries در pattern_archive_index.json موجود و آپلود آن."""
    if not new_entries:
        return True

    tmp_file = "/tmp/pattern_archive_index.json"
    for attempt in range(1, max_retries + 1):
        current = _load_pattern_index(repo)
        current.update(new_entries)
        sha = get_file_sha(repo, PATTERN_INDEX_PATH)
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=False)
        if upload_file_with_curl(repo, PATTERN_INDEX_PATH, tmp_file, sha, "update pattern->archive index"):
            log(f"  [PATTERNS] {PATTERN_INDEX_PATH} به‌روزرسانی شد ✅ (اکنون {len(current)} نگاشت، {len(new_entries)} مورد جدید/تغییریافته)")
            return True
        log(f"  [PATTERNS] تلاش {attempt}/{max_retries} برای آپلود ایندکس ناموفق بود — تلاش مجدد با sha تازه...", 'WARNING')
    log(f"  [PATTERNS] آپلود {PATTERN_INDEX_PATH} پس از چند تلاش ناموفق بود — نگاشت‌های جدید فقط در حافظه‌ی همین اجرا باقی ماندند", 'ERROR')
    return False


def _load_processed_pattern_archives_cache(repo):
    content = gh_api_get(repo, PATTERN_PROCESSED_CACHE_PATH)
    if content is not None:
        try:
            processed = json.loads(content)
            if not isinstance(processed, list):
                processed = []
        except Exception:
            processed = []
    else:
        processed = []
    return _normalize_completed_list(processed, PATTERN_PROCESSED_CACHE_PATH)


def _upload_processed_pattern_archives_cache(repo, updated_archive_paths, max_retries=3):
    tmp_file = "/tmp/processed_pattern_archives.json"
    dedup_sorted = sorted(set(updated_archive_paths))
    for attempt in range(1, max_retries + 1):
        sha = get_file_sha(repo, PATTERN_PROCESSED_CACHE_PATH)
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(dedup_sorted, f, indent=2, ensure_ascii=False)
        if upload_file_with_curl(repo, PATTERN_PROCESSED_CACHE_PATH, tmp_file, sha, "update processed pattern archives cache"):
            log(f"  [PATTERNS] کش {PATTERN_PROCESSED_CACHE_PATH} به‌روزرسانی شد ✅ ({len(dedup_sorted)} آرشیو)")
            return True
        log(f"  [PATTERNS] تلاش {attempt}/{max_retries} برای آپلود کش ناموفق بود — تلاش مجدد با sha تازه...", 'WARNING')
    log(f"  [PATTERNS] آپلود {PATTERN_PROCESSED_CACHE_PATH} پس از چند تلاش ناموفق بود", 'ERROR')
    return False


def get_all_patterns_from_archives(repo, password, force_refresh=False):
    """
    معادل get_all_signatures_from_archives، برای pattern_archives/{module}/{strategy}/*.tar.gz.enc
    (CSVهای آستانه/نسبت_شانس/الگوی_وضعیت combo_10day/combo_monthly).

    خروجی: [{"pattern_path": "<مسیر csv شامل 'patterns/'>", "archive_path": ...}, ...]
    اگر pattern_archives/ اصلاً وجود نداشته باشد (مثلاً چون هنوز هیچ اجرایی
    این داده را تولید نکرده)، لیست خالی برمی‌گرداند — هیچ خطایی ایجاد
    نمی‌شود، چون این قابلیت اختیاری و مکمل صف اصلی signatures است.
    """
    processed_archives = _load_processed_pattern_archives_cache(repo) if not force_refresh else []
    processed_set = set(processed_archives)

    log("  [PATTERNS] دریافت لیست محتوای pattern_archives/...")
    cmd = f"gh api repos/{repo}/contents/pattern_archives --jq '.[].name' 2>/dev/null"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        log("  [PATTERNS] هیچ محتوایی در pattern_archives/ یافت نشد (اختیاری — رد می‌شود)", 'WARNING')
        return []

    top_level_names = [m.strip() for m in result.stdout.strip().split('\n') if m.strip()]
    standard_modules = {"combo_10day", "combo_monthly"}
    found_modules = [n for n in top_level_names if n in standard_modules]
    if not found_modules:
        log("  [PATTERNS] هیچ ماژول استانداردی در pattern_archives/ یافت نشد", 'WARNING')
        return []

    archive_paths = []
    for module in found_modules:
        cmd = f"gh api repos/{repo}/contents/pattern_archives/{module} --jq '.[].name' 2>/dev/null"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0 or not result.stdout.strip():
            continue
        strategies = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]
        for strategy in strategies:
            path = f"pattern_archives/{module}/{strategy}"
            cmd = f"gh api repos/{repo}/contents/{path} --jq '.[].name' 2>/dev/null"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0 or not result.stdout.strip():
                continue
            files = [fn.strip() for fn in result.stdout.strip().split('\n') if fn.strip()]
            for fname in files:
                if fname.endswith('.tar.gz.enc'):
                    archive_paths.append(f"pattern_archives/{module}/{strategy}/{fname}")

    log(f"  [PATTERNS] تعداد کل آرشیوهای .tar.gz.enc یافت‌شده: {len(archive_paths)}")

    all_patterns = []
    newly_processed = []
    for archive_path in archive_paths:
        if archive_path in processed_set and not force_refresh:
            continue
        csv_entries = _list_csv_inside_archive(repo, archive_path, password)
        if not csv_entries:
            continue
        for entry in csv_entries:
            clean_entry = entry
            if clean_entry.startswith("patterns/"):
                clean_entry = clean_entry[len("patterns/"):]
            all_patterns.append({"pattern_path": clean_entry, "archive_path": archive_path})
        if not force_refresh:
            processed_set.add(archive_path)
            newly_processed.append(archive_path)

    log(f"  [PATTERNS] تعداد کل pattern CSVهای یافت‌شده: {len(all_patterns)}")

    if newly_processed and not force_refresh:
        _upload_processed_pattern_archives_cache(repo, processed_archives + newly_processed)

    new_index_entries = {
        p["pattern_path"]: p.get("archive_path")
        for p in all_patterns
        if isinstance(p, dict) and p.get("pattern_path") and p.get("archive_path")
    }
    if new_index_entries:
        _upload_pattern_index(repo, new_index_entries)

    return all_patterns


def _migrate_legacy_queue_format(repo, remote_path, existing_combos, sig_to_archive, commit_msg):
    """
    سازگاری با صف‌های قدیمی: اگر صف موجود (existing_combos) شامل حداقل یک
    آیتم فاقد کلید archive_path باشد، یعنی صف با فرمت قدیمی ساخته شده است.

    این تابع archive_path را (در صورت وجود در نگاشت sig_to_archive) به چنین
    آیتم‌هایی اضافه می‌کند و صف را در مخزن بازنویسی می‌کند. عملیات کاملاً
    غیرمخرب است:
    - هیچ آیتمی حذف نمی‌شود.
    - هیچ آیتمی تکراری اضافه نمی‌شود (فقط archive_path به آیتم‌های موجود
      افزوده می‌شود، نه اینکه صف از نو با لیست signatureهای جدید ساخته شود).
    - اگر signature_path یک آیتم قدیمی در نگاشت پیدا نشود (مثلاً چون آرشیو
      مربوطه دیگر در signature_archives/ وجود ندارد)، آیتم بدون تغییر با
      archive_path=None نگه داشته می‌شود تا از دست نرود.

    خروجی: (لیست به‌روزشده‌ی آیتم‌ها, آیا مهاجرتی انجام شد یا نه)
    """
    needs_migration = any(
        isinstance(item, dict) and "archive_path" not in item
        for item in existing_combos
    )
    if not needs_migration:
        return existing_combos, False

    log(f"  [MIGRATE] فرمت قدیمی صف شناسایی شد ({remote_path}) — افزودن archive_path به آیتم‌های موجود...", 'WARNING')

    migrated = []
    added_count = 0
    missing_count = 0
    for item in existing_combos:
        if isinstance(item, str):
            sig_path = item
            archive_path = sig_to_archive.get(sig_path)
            migrated.append({"signature_path": sig_path, "archive_path": archive_path})
            if archive_path:
                added_count += 1
            else:
                missing_count += 1
        elif isinstance(item, dict):
            if "archive_path" in item:
                migrated.append(item)
            else:
                sig_path = item.get("signature_path")
                archive_path = sig_to_archive.get(sig_path)
                new_item = dict(item)
                new_item["archive_path"] = archive_path
                migrated.append(new_item)
                if archive_path:
                    added_count += 1
                else:
                    missing_count += 1
        else:
            migrated.append(item)

    log(f"  [MIGRATE] archive_path به {added_count} آیتم اضافه شد؛ "
        f"{missing_count} آیتم بدون archive_path باقی ماند (در آرشیوهای فعلی یافت نشد)")

    tmp_file = f"/tmp/_migrate_{os.path.basename(remote_path)}"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(migrated, f, indent=2, ensure_ascii=False)

    sha = get_file_sha(repo, remote_path)
    if not upload_file_with_curl(repo, remote_path, tmp_file, sha, commit_msg):
        log(f"  [MIGRATE] آپلود صف مهاجرت‌شده ({remote_path}) ناموفق بود — "
            f"نسخه‌ی مهاجرت‌شده فقط در حافظه برای ادامه‌ی این اجرا استفاده می‌شود", 'ERROR')
    else:
        log(f"  [MIGRATE] صف {remote_path} با موفقیت به فرمت جدید بازنویسی شد ✅ ({len(migrated)} آیتم)")

    return migrated, True

def _decrypt_enc_bytes(enc_path, password):
    """رمزگشایی فایل .enc (aes-256-cbc، کلید scrypt با salt='salt'، iv در ۱۶ بایت
    ابتدایی فایل) و بازگرداندن bytes خام. دقیقاً مطابق طرح رمزنگاری استفاده‌شده
    در جاب فشرده‌سازی/رمزنگاری reports (loop_analysis.yml) و decrypt_enc_bytes
    در calculators.py."""
    script = (
        "const crypto=require('crypto'),fs=require('fs');"
        f"const pass={json.dumps(password)};"
        f"const data=fs.readFileSync({json.dumps(enc_path)});"
        "const key=crypto.scryptSync(pass,'salt',32);"
        "const iv=data.slice(0,16),enc=data.slice(16);"
        "const decipher=crypto.createDecipheriv('aes-256-cbc',key,iv);"
        "const dec=Buffer.concat([decipher.update(enc),decipher.final()]);"
        "process.stdout.write(dec);"
    )
    result = subprocess.run(["node", "-e", script], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"رمزگشایی ناموفق: {result.stderr.decode()}")
    return result.stdout

# ================================ مرحله ۱: Work Queue ================================

def filter_strategies_by_conditions(repo, token, password=None):
    """فیلتر استراتژی‌ها بر اساس ۷ شرط، با استفاده از _internal_all_results.json
    (تولیدشده توسط calculators.py report در جاب generate-reports، در ریشه‌ی
    مخزن سوم). مجموعه‌ای از folder_name های قبول‌شده برمی‌گرداند.

    اگر فایل موجود نبود یا خراب بود، هیچ fallback ای وجود ندارد — RuntimeError
    raise می‌شود تا اسکریپت با exit(1) متوقف شود (طبق نیازمندی صریح).

    برای رکوردهایی که یکی از ستون‌های مورد نیاز را ندارند، از مقدار پیش‌فرضی
    استفاده می‌شود که همیشه شرط مربوطه را نقض می‌کند (یعنی رکورد رد می‌شود، نه
    اینکه به‌اشتباه قبول شود).
    """
    log("=" * 60)
    log("🔍 فیلتر استراتژی‌ها بر اساس ۷ شرط (_internal_all_results.json)")
    log("=" * 60)

    log("  دانلود reports.tar.gz.enc از ریشه‌ی مخزن سوم...")
    enc_path = "/tmp/_filter_reports.tar.gz.enc"
    if os.path.exists(enc_path):
        os.remove(enc_path)
    if not gh_api_get_binary(repo, "reports.tar.gz.enc", enc_path):
        log("  ❌ reports.tar.gz.enc در مخزن سوم یافت نشد", 'ERROR')
        raise RuntimeError("reports.tar.gz.enc در مخزن سوم یافت نشد — هیچ fallback ای وجود ندارد")

    if not password:
        log("  ❌ رمز عبور (RESULTS_PASSWORD) برای رمزگشایی reports.tar.gz.enc تنظیم نشده", 'ERROR')
        raise RuntimeError("رمز عبور برای رمزگشایی reports.tar.gz.enc موجود نیست")

    try:
        raw = _decrypt_enc_bytes(enc_path, password)
    except Exception as e:
        log(f"  ❌ خطا در رمزگشایی reports.tar.gz.enc: {e}", 'ERROR')
        raise RuntimeError(f"رمزگشایی reports.tar.gz.enc ناموفق بود: {e}")

    import tarfile
    import tempfile
    try:
        with tempfile.TemporaryDirectory() as td:
            tgz_path = os.path.join(td, "reports.tar.gz")
            with open(tgz_path, "wb") as f:
                f.write(raw)
            with tarfile.open(tgz_path, "r:gz") as tf:
                tf.extractall(td)
            snap_path = os.path.join(td, "reports", "_internal_all_results.json")
            if not os.path.exists(snap_path):
                log("  ❌ _internal_all_results.json درون reports.tar.gz.enc یافت نشد", 'ERROR')
                raise RuntimeError("_internal_all_results.json درون reports.tar.gz.enc یافت نشد — هیچ fallback ای وجود ندارد")
            with open(snap_path, "r", encoding="utf-8") as f:
                content = f.read()
    except RuntimeError:
        raise
    except Exception as e:
        log(f"  ❌ خطا در استخراج reports.tar.gz.enc: {e}", 'ERROR')
        raise RuntimeError(f"استخراج reports.tar.gz.enc ناموفق بود: {e}")
    finally:
        if os.path.exists(enc_path):
            os.remove(enc_path)

    try:
        records = json.loads(content)
    except Exception as e:
        log(f"  ❌ خطا در parse کردن _internal_all_results.json: {e}", 'ERROR')
        raise RuntimeError(f"_internal_all_results.json خراب است و parse نشد: {e}")

    if not isinstance(records, list):
        log("  ❌ محتوای _internal_all_results.json از نوع لیست نیست", 'ERROR')
        raise RuntimeError("_internal_all_results.json فرمت نامعتبر دارد (یک لیست از رکوردها انتظار می‌رفت)")

    log(f"  تعداد کل استراتژی‌های موجود در _internal_all_results.json: {len(records)}")

    accepted = set()
    skipped_no_folder = 0

    for item in records:
        if not isinstance(item, dict):
            skipped_no_folder += 1
            continue

        # period_name نام پایه‌ی استراتژی (بدون پسوند کوین) است و دقیقاً با
        # فرمت strategies.txt مطابقت دارد؛ folder_name شامل پسوند کوین است
        # (مثلاً "..._BTCUSDT-1m_XRPUSDT-1m") و برای تطبیق با strategies.txt
        # مناسب نیست.
        folder_name = item.get("period_name")
        if not folder_name:
            skipped_no_folder += 1
            continue

        # مقادیر پیش‌فرض طوری انتخاب شده‌اند که در صورت غیبت ستون، شرط مربوطه
        # همیشه نقض شود (یعنی رکورد رد شود).
        pct_profitable_months = item.get("pct_profitable_months", -1)
        total_trades = item.get("total_trades", -1)
        win_rate = item.get("win_rate", -1)
        real_return = item.get("real_return", -1)
        avg_profit_in_profitable_months = item.get("avg_profit_in_profitable_months", -1)
        max_consecutive_monthly_loss = item.get("max_consecutive_monthly_loss", float("inf"))
        avg_loss_in_loss_months = item.get("avg_loss_in_loss_months", float("-inf"))
        sharpe_monthly = item.get("sharpe_monthly", float("-inf"))

        cond1 = pct_profitable_months > 10
        cond2 = (total_trades > 100) or (total_trades <= 100 and win_rate >= 75 and real_return > 0)
        cond3 = avg_profit_in_profitable_months > 0.5
        cond4 = win_rate >= 25
        cond5 = max_consecutive_monthly_loss < 12
        cond6 = avg_loss_in_loss_months >= -100
        cond7 = sharpe_monthly > -0.2

        if cond1 and cond2 and cond3 and cond4 and cond5 and cond6 and cond7:
            accepted.add(folder_name)

    if skipped_no_folder:
        log(f"  ⚠️ {skipped_no_folder} رکورد بدون folder_name معتبر رد شد", 'WARNING')

    log(f"  تعداد استراتژی‌های واجد شرایط (پس از فیلتر ۷ شرطی): {len(accepted)}")
    return accepted

def build_work_queue(repo, token, filtered_set=None):
    log("=" * 60)
    log("🔄 مرحله ۱: ساخت صف کاری (work queue)")
    log("=" * 60)

    MAX_COMBOS = 50000

    fixed_modules = ["combo_10day", "combo_monthly"]
    coins = [
        "BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "PAXGUSDT",
        "BTCUSDT+ETHUSDT", "BTCUSDT+XRPUSDT", "BTCUSDT+SOLUSDT", "BTCUSDT+PAXGUSDT",
        "ETHUSDT+XRPUSDT", "ETHUSDT+SOLUSDT", "ETHUSDT+PAXGUSDT",
        "XRPUSDT+SOLUSDT", "XRPUSDT+PAXGUSDT", "SOLUSDT+PAXGUSDT",
        "BTCUSDT+ETHUSDT+XRPUSDT", "BTCUSDT+ETHUSDT+SOLUSDT", "BTCUSDT+ETHUSDT+PAXGUSDT",
        "BTCUSDT+XRPUSDT+SOLUSDT", "BTCUSDT+XRPUSDT+PAXGUSDT",
        "BTCUSDT+SOLUSDT+PAXGUSDT", "ETHUSDT+XRPUSDT+SOLUSDT",
        "ETHUSDT+XRPUSDT+PAXGUSDT", "ETHUSDT+SOLUSDT+PAXGUSDT",
        "XRPUSDT+SOLUSDT+PAXGUSDT",
        "BTCUSDT+ETHUSDT+XRPUSDT+SOLUSDT", "BTCUSDT+ETHUSDT+XRPUSDT+PAXGUSDT",
        "BTCUSDT+ETHUSDT+SOLUSDT+PAXGUSDT", "BTCUSDT+XRPUSDT+SOLUSDT+PAXGUSDT",
        "ETHUSDT+XRPUSDT+SOLUSDT+PAXGUSDT",
        "BTCUSDT+ETHUSDT+XRPUSDT+SOLUSDT+PAXGUSDT",
    ]
    intervals_10day = [
        "fixed_5d", "fixed_10d", "fixed_15d", "fixed_30d",
        "CPI_post_5d", "CPI_post_10d", "CPI_post_15d", "CPI_post_30d",
        "CoreCPI_post_5d", "CoreCPI_post_10d", "CoreCPI_post_15d", "CoreCPI_post_30d",
        "PPI_post_5d", "PPI_post_10d", "PPI_post_15d", "PPI_post_30d",
        "CorePPI_post_5d", "CorePPI_post_10d", "CorePPI_post_15d", "CorePPI_post_30d",
        "FOMC_pre_5d", "FOMC_post_5d", "FOMC_post_10d", "FOMC_post_15d", "FOMC_post_30d",
        "CPI_y_y_post_5d", "CPI_y_y_post_10d", "CPI_y_y_post_15d", "CPI_y_y_post_30d"
    ]
    intervals_monthly = ["monthly"]
    module_intervals = {
        "combo_10day": intervals_10day,
        "combo_monthly": intervals_monthly,
    }
    models = ["simple_hybrid"]

    strategies_file = os.environ.get('STRATEGIES_FILE', '/tmp/strategies.txt')
    log(f"  خواندن استراتژی‌ها از: {strategies_file}")
    if os.path.exists(strategies_file):
        with open(strategies_file, 'r') as f:
            strategies = [line.strip() for line in f if line.strip()]
        log(f"  تعداد استراتژی‌های خوانده‌شده از strategies.txt: {len(strategies)}")
    else:
        log(f"  فایل {strategies_file} یافت نشد — تلاش برای دریافت از API مخزن سوم...", 'WARNING')
        cmd = f"gh api repos/{repo}/contents/aggregated --jq '.[].name' 2>/dev/null"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            strategies = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]
            log(f"  تعداد استراتژی‌های دریافت‌شده از API (aggregated/): {len(strategies)}")
        else:
            log("  دریافت استراتژی‌ها از API هم ناموفق بود", 'ERROR')
            raise RuntimeError("strategies.txt یافت نشد و دریافت از API نیز ناموفق بود")

    if not strategies:
        log("  هیچ استراتژی‌ای یافت نشد", 'WARNING')
        return 0

    if filtered_set is not None:
        if not filtered_set:
            log("  ⚠️ filtered_set خالی است — هیچ استراتژی مجازی برای پردازش وجود ندارد", 'WARNING')
            return 0
        before_count = len(strategies)
        strategies = [s for s in strategies if s in filtered_set]
        log(f"  اعمال filtered_set: از {before_count} استراتژی، {len(strategies)} استراتژی مجاز (فیلترشده) باقی ماند")
        if not strategies:
            log("  هیچ‌کدام از استراتژی‌های خوانده‌شده در filtered_set نبودند", 'WARNING')
            return 0

    log("  دانلود completed_strategies.json...")
    completed_content = gh_api_get(repo, "completed_strategies.json")
    if completed_content is not None:
        try:
            completed_strategies = json.loads(completed_content)
            if not isinstance(completed_strategies, list):
                completed_strategies = []
        except Exception:
            completed_strategies = []
    else:
        completed_strategies = []
    log(f"  تعداد استراتژی‌های قبلاً پردازش‌شده: {len(completed_strategies)}")

    log("  دانلود all_combinations.json...")
    all_combos_content = gh_api_get(repo, "all_combinations.json")
    if all_combos_content is not None:
        try:
            existing_combos = json.loads(all_combos_content)
            if not isinstance(existing_combos, list):
                existing_combos = []
        except Exception:
            existing_combos = []
    else:
        existing_combos = []
    log(f"  تعداد ترکیب‌های موجود در صف: {len(existing_combos)}")

    strategies_to_process = [s for s in strategies if s not in completed_strategies]
    log(f"  تعداد استراتژی‌های جدید (نه در completed): {len(strategies_to_process)}")

    if not strategies_to_process:
        log("  هیچ استراتژی جدیدی برای پردازش وجود ندارد ✅")
        return 0

    log(f"  استراتژی‌های جدید: {strategies_to_process}")

    existing_count = len(existing_combos)
    log(f"  تعداد ترکیب‌های موجود در all_combinations.json: {existing_count}")

    if existing_count >= MAX_COMBOS:
        log("⏸️ سقف ۵۰,۰۰۰ ترکیب پر شده است. منتظر پردازش و پاکسازی توسط analysis_fixed_batch باشید.", 'WARNING')
        return 0

    allowed = MAX_COMBOS - existing_count
    log(f"  ظرفیت باقی‌مانده تا سقف ۵۰,۰۰۰: {allowed}")

    new_combos = []
    fully_added_strategies = []
    deferred_strategies = []
    capped = False

    for strat in strategies_to_process:
        if capped:
            deferred_strategies.append(strat)
            continue

        strat_combos = []
        for mod in fixed_modules:
            mod_intervals = module_intervals[mod]
            for coin in coins:
                for interval in mod_intervals:
                    for model in models:
                        strat_combos.append({
                            "module": mod,
                            "strat": strat,
                            "coin": coin,
                            "interval": interval,
                            "model": model
                        })

        if len(strat_combos) <= allowed:
            new_combos.extend(strat_combos)
            allowed -= len(strat_combos)
            fully_added_strategies.append(strat)
            log(f"  استراتژی '{strat}': {len(strat_combos)} ترکیب اضافه شد (ظرفیت باقی‌مانده: {allowed})")
        else:
            log(f"  ⏸️ استراتژی '{strat}' نیازمند {len(strat_combos)} ترکیب است اما فقط {allowed} ظرفیت باقی مانده — "
                f"این استراتژی و استراتژی‌های بعدی به دفعه بعد موکول می‌شوند", 'WARNING')
            capped = True
            deferred_strategies.append(strat)

    if deferred_strategies:
        log(f"  تعداد استراتژی‌هایی که به دلیل سقف به دفعه بعد موکول شدند: {len(deferred_strategies)}")

    log(f"  تعداد کل ترکیب‌های جدید: {len(new_combos)}")

    if not new_combos:
        log("  هیچ ترکیب جدیدی اضافه نشد (همه‌ی استراتژی‌های باقی‌مانده به دلیل سقف موکول شدند)", 'WARNING')
        return 0

    updated_combos = existing_combos + new_combos
    log(f"  تعداد کل ترکیب‌ها پس از اضافه شدن: {len(updated_combos)}")

    tmp_file = "/tmp/all_combinations.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(updated_combos, f, indent=2, ensure_ascii=False)

    sha_all = get_file_sha(repo, "all_combinations.json")
    if not upload_file_with_curl(repo, "all_combinations.json", tmp_file, sha_all, "update work queue"):
        raise RuntimeError("آپلود all_combinations.json ناموفق بود")

    # فقط استراتژی‌هایی که به‌طور کامل اضافه شده‌اند در completed_strategies.json
    # ثبت می‌شوند؛ استراتژی‌های موکول‌شده به دلیل سقف، در اجرای بعدی دوباره
    # کاندیدای پردازش خواهند بود.
    updated_completed = completed_strategies + fully_added_strategies
    tmp_completed = "/tmp/completed_strategies.json"
    with open(tmp_completed, "w", encoding="utf-8") as f:
        json.dump(updated_completed, f, indent=2, ensure_ascii=False)

    sha_completed = get_file_sha(repo, "completed_strategies.json")
    if not upload_file_with_curl(repo, "completed_strategies.json", tmp_completed, sha_completed, "update completed strategies"):
        raise RuntimeError("آپدیت completed_strategies.json ناموفق بود")

    log(f"✅ تعداد {len(new_combos)} ترکیب جدید به all_combinations.json اضافه شد "
        f"— {len(fully_added_strategies)} استراتژی به‌طور کامل تکمیل شد، {len(deferred_strategies)} استراتژی موکول شد")
    return len(new_combos)

# ================================ مرحله ۲: Golden Queue ================================

def build_golden_queue(repo, token, password):
    log("=" * 60)
    log("🔄 مرحله ۲: ساخت صف golden")
    log("=" * 60)

    log("  دانلود completed_golden.json...")
    completed_content = gh_api_get(repo, "completed_golden.json")
    if completed_content is not None:
        try:
            completed_golden = json.loads(completed_content)
            if not isinstance(completed_golden, list):
                completed_golden = []
        except Exception:
            completed_golden = []
    else:
        completed_golden = []

    completed_golden = _normalize_completed_list(completed_golden, "completed_golden.json")
    completed_set = set(completed_golden)
    log(f"  تعداد signatureهایی که قبلاً در completed_golden.json ثبت شده‌اند: {len(completed_set)}")

    # دریافت signatures در سطح فایل .jsonl (شامل 'signatures/' در ابتدای مسیر)
    all_signatures = get_all_signatures_from_archives(repo, password, key_name="signature_path")
    log(f"  تعداد signatureهای پیدا شده در signature_archives/: {len(all_signatures)}")

    if not all_signatures:
        log("  هیچ signature‌ای در آرشیوها یافت نشد ✅")
        return 0

    new_signatures = [s for s in all_signatures if s["signature_path"] not in completed_set]
    log(f"  تعداد signatureهای جدید (نه در completed_golden): {len(new_signatures)}")

    if not new_signatures:
        log("  هیچ signature جدیدی برای اضافه کردن وجود ندارد ✅")
        return 0

    # غنی‌سازی اختیاری: اگر CSV آستانه/نسبت_شانس متناظر در pattern_archives/
    # موجود باشد، مسیر آرشیوش (pattern_archive_path) به آیتم اضافه می‌شود.
    # این کاملاً اختیاری و افزایشی است — هر خطایی اینجا فقط لاگ می‌شود و صف
    # اصلی (signature_path/archive_path) دست‌نخورده و بدون وقفه ادامه می‌یابد.
    try:
        all_patterns = get_all_patterns_from_archives(repo, password)
        pattern_lookup = {
            p["pattern_path"]: p.get("archive_path")
            for p in all_patterns
            if isinstance(p, dict) and p.get("pattern_path")
        }
        enriched_count = 0
        for s in new_signatures:
            sig_path = s.get("signature_path", "")
            if sig_path.endswith(".jsonl"):
                candidate_pattern_path = sig_path[: -len(".jsonl")] + ".csv"
                if candidate_pattern_path in pattern_lookup:
                    s["pattern_archive_path"] = pattern_lookup[candidate_pattern_path]
                    enriched_count += 1
        log(f"  [PATTERNS] {enriched_count} از {len(new_signatures)} signature جدید با pattern_archive_path غنی شدند")
    except Exception as e:
        log(f"  [PATTERNS] غنی‌سازی pattern_archive_path با خطا مواجه شد (نادیده گرفته می‌شود): {e}", 'WARNING')

    log("  دانلود all_combinations_golden.json...")
    combos_content = gh_api_get(repo, "all_combinations_golden.json")
    if combos_content is not None:
        try:
            existing_combos = json.loads(combos_content)
            if not isinstance(existing_combos, list):
                existing_combos = []
        except Exception:
            existing_combos = []
    else:
        existing_combos = []
    log(f"  ترکیب‌های موجود در صف: {len(existing_combos)}")

    sig_to_archive = {
        s["signature_path"]: s.get("archive_path")
        for s in all_signatures
        if isinstance(s, dict) and "signature_path" in s
    }
    existing_combos, _ = _migrate_legacy_queue_format(
        repo, "all_combinations_golden.json", existing_combos, sig_to_archive,
        "migrate golden queue to include archive_path"
    )

    existing_in_queue = {
        s["signature_path"] if isinstance(s, dict) else s
        for s in existing_combos
    }
    new_signatures = [
        s for s in new_signatures
        if s["signature_path"] not in existing_in_queue
    ]
    log(f"  تعداد signatureهای واقعاً جدید (نه در صف و نه در completed): {len(new_signatures)}")

    if not new_signatures:
        log("  هیچ signature جدیدی برای اضافه کردن وجود ندارد ✅")
        return 0

    updated_combos = existing_combos + new_signatures
    log(f"  تعداد کل ترکیب‌ها پس از اضافه شدن: {len(updated_combos)}")

    tmp_file = "/tmp/all_combinations_golden.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(updated_combos, f, indent=2, ensure_ascii=False)

    sha_combos = get_file_sha(repo, "all_combinations_golden.json")
    if not upload_file_with_curl(repo, "all_combinations_golden.json", tmp_file, sha_combos, "update golden queue"):
        raise RuntimeError("آپلود all_combinations_golden.json ناموفق بود")

    new_sig_paths = [s["signature_path"] for s in new_signatures]
    updated_completed = completed_golden + new_sig_paths
    tmp_completed = "/tmp/completed_golden.json"
    with open(tmp_completed, "w", encoding="utf-8") as f:
        json.dump(updated_completed, f, indent=2, ensure_ascii=False)

    sha_completed = get_file_sha(repo, "completed_golden.json")
    if not upload_file_with_curl(repo, "completed_golden.json", tmp_completed, sha_completed, "update completed golden"):
        raise RuntimeError("آپدیت completed_golden.json ناموفق بود")

    log(f"✅ تعداد {len(new_signatures)} signature جدید به all_combinations_golden.json اضافه شد")
    return len(new_signatures)


def _install_pandas():
    log("  نصب pandas و pyarrow...")
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "pandas", "pyarrow",
         "--quiet", "--break-system-packages"],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        r2 = subprocess.run(
            [sys.executable, "-m", "pip", "install", "pandas", "pyarrow", "--quiet"],
            capture_output=True, text=True
        )
        if r2.returncode != 0:
            log(f"  نصب ناموفق: {r2.stderr[:200]}", 'ERROR')
            return False
    log("  pandas و pyarrow نصب شدند ✅")
    return True

def _get_qualified_signatures(csv_path, min_score=45):
    """
    خواندن golden_scores.csv و فیلتر score >= min_score

    اولویت با ستون signature_path است (معماری جدید). اگر ستون signature_path
    وجود نداشت، از ستون signature استفاده می‌شود (سازگاری با عقب).

    اجباری: archive_path مستقیماً از خودِ سورس گلدن (ستونی داخل همین CSV،
    مثلاً archive_path/source_file/source_archive) خوانده می‌شود — نه با
    اسکن مجدد آرشیوها و نه از ایندکس جداگانه. اگر golden.py چنین ستونی
    ننوشته باشد، archive_path=None می‌ماند و این یعنی مشکل باید در خودِ
    golden.py حل شود، نه اینجا با rescan سنگین.

    دیباگ اجباری: صرف‌نظر از موفقیت یا شکست خواندن/فیلتر کردن، ۲ نمونه‌ی
    اول همین CSV (سورس گلدن) همیشه چاپ می‌شود تا مشخص شود واقعاً چه
    ستون‌هایی و چه مقادیری در سورس گلدن وجود دارد.
    """
    try:
        import pandas as pd
    except ImportError:
        if not _install_pandas():
            return []
        import pandas as pd

    df = None
    try:
        df = pd.read_csv(csv_path)
        log(f"  ستون‌های CSV: {list(df.columns)}")
        log(f"  تعداد کل سطرها: {len(df)}")
    except Exception as e:
        log(f"  خطا در خواندن CSV: {e}", 'ERROR')
        log("  [GOLDEN-SOURCE] ۲ نمونه اول چاپ نشد چون خودِ فایل CSV خوانده نشد", 'ERROR')
        return []

    # ---- چاپ اجباری ۲ نمونه اول سورس گلدن (چه بعداً موفق شویم چه نه) ----
    try:
        sample_records = df.head(2).to_dict(orient="records")
        log(f"  [GOLDEN-SOURCE] ۲ نمونه اول سورس گلدن (CSV): {json.dumps(sample_records, ensure_ascii=False, default=str)}")
    except Exception as e:
        log(f"  [GOLDEN-SOURCE] چاپ نمونه‌ها با خطا مواجه شد: {e}", 'ERROR')

    try:
        score_col = None
        for col in ['score', 'golden_score', 'total_score', 'final_score']:
            if col in df.columns:
                score_col = col
                break
        if score_col is None:
            numeric_cols = df.select_dtypes(include='number').columns.tolist()
            if numeric_cols:
                score_col = numeric_cols[0]
                log(f"  ستون 'score' یافت نشد، از '{score_col}' استفاده می‌شود", 'WARNING')
            else:
                log("  هیچ ستون عددی در CSV یافت نشد", 'ERROR')
                return []

        # ========== تغییر اصلی: اولویت با signature_path ==========
        sig_col = None
        # اولویت ۱: signature_path (ستون جدید در golden.py اصلاح‌شده)
        for col in ['signature_path']:
            if col in df.columns:
                sig_col = col
                break
        # اولویت ۲: signature (ستون قدیمی، سازگاری با عقب)
        if sig_col is None:
            for col in ['signature', 'sig', 'name']:
                if col in df.columns:
                    sig_col = col
                    break

        if sig_col is None:
            str_cols = df.select_dtypes(include='object').columns.tolist()
            if str_cols:
                sig_col = str_cols[0]
                log(f"  ستون 'signature_path'/'signature' یافت نشد، از '{sig_col}' استفاده می‌شود", 'WARNING')
            else:
                log("  هیچ ستون رشته‌ای در CSV یافت نشد", 'ERROR')
                return []

        # اجباری: archive_path مستقیماً از سورس گلدن — بدون هیچ rescan/ایندکس
        archive_candidates = ['archive_path', 'archives_path', 'source_file', 'source_archive', 'archive']
        archive_col = None
        for col in archive_candidates:
            if col in df.columns:
                archive_col = col
                break
        if archive_col:
            log(f"  [GOLDEN-SOURCE] ستون archive_path از سورس گلدن خوانده می‌شود: '{archive_col}'")
        else:
            log(
                f"  [GOLDEN-SOURCE] ⚠️ هیچ‌کدام از ستون‌های {archive_candidates} در CSV گلدن یافت نشد. "
                f"ستون‌های واقعی موجود در فایل: {list(df.columns)} — "
                "golden.py فعلاً چنین ستونی در golden_scores.csv نمی‌نویسد "
                "(خروجی golden.py فقط شامل strategy_id/coin_composition/signature_path/signature/"
                "score/sample_count/version_id/calculated_at است)؛ archive_path=None ثبت می‌شود "
                "و باید از ایندکس fallback (signature_archive_index.json) پر شود.",
                'WARNING'
            )

        filtered = df[df[score_col] >= min_score]
        log(f"  تعداد استراتژی‌های با امتیاز >= {min_score}: {len(filtered)}")

        return [
            {
                "signature_path": str(row[sig_col]),
                "score": float(row[score_col]),
                "archive_path": (str(row[archive_col]) if archive_col and pd.notna(row[archive_col]) else None),
            }
            for _, row in filtered.iterrows()
        ]
    except Exception as e:
        log(f"  خطا در پردازش CSV: {e}", 'ERROR')
        return []

def build_portfolios_queue(repo, token, password, min_score=45.0):
    log("=" * 60)
    log("🔄 مرحله ۳: ساخت صف portfolios")
    log("=" * 60)

    log("  دانلود completed_portfolios.json...")
    completed_content = gh_api_get(repo, "completed_portfolios.json")
    if completed_content is not None:
        try:
            completed_portfolios = json.loads(completed_content)
            if not isinstance(completed_portfolios, list):
                completed_portfolios = []
        except Exception:
            completed_portfolios = []
    else:
        completed_portfolios = []

    completed_portfolios = _normalize_completed_list(completed_portfolios, "completed_portfolios.json")
    completed_set = set(completed_portfolios)
    log(f"  تعداد signatureهای قبلاً ثبت‌شده: {len(completed_set)}")

    # دانلود و رمزگشایی golden_scores.csv.enc
    enc_path = "/tmp/golden_scores.csv.enc"
    targz_path = "/tmp/golden_scores.tar.gz"
    extract_dir = "/tmp/golden_scores_extract"

    log("  دانلود golden_scores.csv.enc...")
    if not gh_api_get_binary(repo, "golden_scores.csv.enc", enc_path):
        log("  golden_scores.csv.enc یافت نشد — احتمالاً golden هنوز اجرا نشده. رد می‌شویم.", 'WARNING')
        return 0

    log("  رمزگشایی golden_scores.csv.enc...")
    if not decrypt_file(enc_path, targz_path, password):
        raise RuntimeError("رمزگشایی golden_scores.csv.enc ناموفق بود")

    if not os.path.exists(targz_path) or os.path.getsize(targz_path) == 0:
        raise RuntimeError("فایل رمزگشایی‌شده خالی است")

    log(f"  رمزگشایی موفق ({os.path.getsize(targz_path)} bytes) — استخراج tar.gz...")

    shutil.rmtree(extract_dir, ignore_errors=True)
    os.makedirs(extract_dir, exist_ok=True)
    tar_result = subprocess.run(
        ["tar", "-xzf", targz_path, "-C", extract_dir],
        capture_output=True, text=True
    )
    if tar_result.returncode != 0:
        raise RuntimeError(f"استخراج tar.gz ناموفق بود: {tar_result.stderr[:300]}")

    found_csv = []
    for root, _, files in os.walk(extract_dir):
        for fn in files:
            if fn.endswith('.csv'):
                found_csv.append(os.path.join(root, fn))

    if not found_csv:
        raise RuntimeError("هیچ فایل .csv داخل tar.gz استخراج‌شده یافت نشد")

    # اولویت با golden_scores.csv در صورت وجود چند فایل CSV
    preferred = [p for p in found_csv if os.path.basename(p) == 'golden_scores.csv']
    dec_path = preferred[0] if preferred else found_csv[0]
    log(f"  فایل CSV استخراج شد: {dec_path} ({os.path.getsize(dec_path)} bytes) ✅")

    log(f"  فیلتر signatureها با score >= {min_score}...")
    qualified = _get_qualified_signatures(dec_path, min_score)

    for fp in [enc_path, targz_path]:
        try:
            os.unlink(fp)
        except Exception:
            pass
    shutil.rmtree(extract_dir, ignore_errors=True)

    if not qualified:
        log("  هیچ signature واجد شرایطی یافت نشد ✅")
        return 0

    # ====================================================================
    # تغییر اصلی: به جای استفاده از signature (امضای خبری)، از signature_path استفاده می‌کنیم
    # چون golden.py اصلاح‌شده، signature_path را در خروجی ذخیره می‌کند.
    # ====================================================================
    log("  دانلود all_combinations_portfolios.json...")
    combos_content = gh_api_get(repo, "all_combinations_portfolios.json")
    if combos_content is not None:
        try:
            existing_combos = json.loads(combos_content)
            if not isinstance(existing_combos, list):
                existing_combos = []
        except Exception:
            existing_combos = []
    else:
        existing_combos = []
    log(f"  ترکیب‌های موجود در صف: {len(existing_combos)}")

    # نگاشت signature_path -> archive_path
    #
    # اجباری: منبع اصلی و اول archive_path حالا خودِ سورس گلدن است —
    # یعنی همان مقداری که _get_qualified_signatures مستقیماً از ستون
    # archive_path/source_file داخل golden_scores.csv خوانده (q["archive_path"]).
    # هیچ اسکن مجدد آرشیوها (که کند و سنگین است) اینجا انجام نمی‌شود.
    #
    # ایندکس دائمی signature_archive_index.json فقط به‌عنوان یک fallback
    # سبک و رایگان (فقط یک دانلود JSON، بدون دانلود/رمزگشایی آرشیو) برای
    # مواردی نگه داشته می‌شود که سورس گلدن هنوز این ستون را ندارد یا خالی
    # است — نه به‌عنوان منبع اصلی.
    log("  [ARCHIVE-MAP] منبع اصلی archive_path: ستون سورس گلدن (golden_scores.csv)")
    log("  [ARCHIVE-MAP] دریافت ایندکس سبک signature_path -> archive_path به‌عنوان fallback...")
    sig_to_archive = _load_source_index(repo)
    log(f"  [ARCHIVE-MAP] ایندکس fallback بارگذاری شد: {len(sig_to_archive)} نگاشت")

    # بررسی و مهاجرت صف قدیمی (در صورت نیاز)
    needs_migration = any(
        isinstance(item, dict) and "archive_path" not in item
        for item in existing_combos
    )
    if needs_migration:
        log("  [MIGRATE] فرمت قدیمی در صف portfolios شناسایی شد — migrate می‌شود...", 'WARNING')
        existing_combos, _ = _migrate_legacy_queue_format(
            repo, "all_combinations_portfolios.json", existing_combos, sig_to_archive,
            "migrate portfolios queue to include archive_path"
        )

    # فیلتر کردن signatureهای جدید (نه در completed و نه در صف)
    existing_in_queue = {
        s["signature_path"] if isinstance(s, dict) else s
        for s in existing_combos
    }

    new_signatures = []
    from_golden_source_count = 0
    from_index_fallback_count = 0
    missing_archive_count = 0
    for q in qualified:
        sig_path = q["signature_path"]
        if sig_path in completed_set or sig_path in existing_in_queue:
            continue
        # اجباری: اول از سورس گلدن (q["archive_path"])، فقط اگر خالی بود از ایندکس fallback
        archive_path = q.get("archive_path")
        if archive_path:
            from_golden_source_count += 1
        else:
            archive_path = sig_to_archive.get(sig_path)
            if archive_path:
                from_index_fallback_count += 1
        new_signatures.append({
            "signature_path": sig_path,
            "archive_path": archive_path,
            "score": q["score"]
        })
        if not archive_path:
            missing_archive_count += 1

    log(f"  [ARCHIVE-MAP] archive_path از سورس گلدن: {from_golden_source_count} | "
        f"از ایندکس fallback: {from_index_fallback_count} | یافت نشد: {missing_archive_count}")

    if missing_archive_count:
        log(
            f"  ⚠️ {missing_archive_count} از {len(new_signatures)} signature جدید "
            f"نه در سورس گلدن و نه در ایندکس archive_path داشتند (archive_path=None ثبت شد و توسط "
            f"analysis_portfolios.yml رد خواهند شد)",
            'WARNING'
        )

    log(f"  تعداد signatureهای واقعاً جدید (نه در صف و نه در completed): {len(new_signatures)}")

    if not new_signatures:
        log("  هیچ signature جدیدی برای اضافه کردن وجود ندارد ✅")
        return 0

    updated_combos = existing_combos + new_signatures

    tmp_file = "/tmp/all_combinations_portfolios.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(updated_combos, f, indent=2, ensure_ascii=False)

    sha_combos = get_file_sha(repo, "all_combinations_portfolios.json")
    if not upload_file_with_curl(repo, "all_combinations_portfolios.json", tmp_file, sha_combos, "update portfolios queue"):
        raise RuntimeError("آپلود all_combinations_portfolios.json ناموفق بود")

    new_sig_paths = [s["signature_path"] for s in new_signatures]
    updated_completed = completed_portfolios + new_sig_paths
    tmp_completed = "/tmp/completed_portfolios.json"
    with open(tmp_completed, "w", encoding="utf-8") as f:
        json.dump(updated_completed, f, indent=2, ensure_ascii=False)

    sha_completed = get_file_sha(repo, "completed_portfolios.json")
    if not upload_file_with_curl(repo, "completed_portfolios.json", tmp_completed, sha_completed, "update completed portfolios"):
        raise RuntimeError("آپدیت completed_portfolios.json ناموفق بود")

    log(f"✅ تعداد {len(new_signatures)} signature جدید به all_combinations_portfolios.json اضافه شد")
    return len(new_signatures)

def bootstrap_source_index(repo, password):
    """اجرای یک‌باره: کل signature_archives/ را (حتی آرشیوهای قبلاً کش‌شده)
    از نو اسکن می‌کند تا نگاشت signature_path -> archive_path آرشیوهایی که
    قبل از وجود این ایندکس پردازش شده بودند (و نگاشتشان گم شده بود) بازسازی
    و در signature_archive_index.json ذخیره شود.

    این تابع گران است (دانلود همه‌ی آرشیوها) و فقط یک‌بار لازم است اجرا شود؛
    بعد از آن، get_all_signatures_from_archives به‌صورت خودکار و تدریجی
    ایندکس را برای هر آرشیو جدید تکمیل می‌کند و دیگر هیچ‌وقت rescan کامل
    لازم نیست. برای اجرا: BOOTSTRAP_SOURCE_INDEX=1 را در محیط تنظیم کنید.
    """
    log("=" * 60)
    log("🔄 Bootstrap یک‌باره: بازسازی کامل signature_archive_index.json")
    log("=" * 60)
    all_signatures = get_all_signatures_from_archives(
        repo, password, key_name="signature_path", force_refresh=True
    )
    entries = {
        s["signature_path"]: s.get("archive_path")
        for s in all_signatures
        if isinstance(s, dict) and s.get("signature_path") and s.get("archive_path")
    }
    log(f"  {len(entries)} نگاشت از rescan کامل به‌دست آمد — ادغام در ایندکس دائمی...")
    _upload_source_index(repo, entries)
    log("✅ Bootstrap ایندکس تمام شد")


def repair_portfolios_queue_archive_paths(repo, password):
    """اجرای یک‌باره: null هایی که همین الان داخل all_combinations_portfolios.json
    نشسته‌اند را با استفاده از ایندکس دائمی signature_archive_index.json پر می‌کند.

    فرق این تابع با fix قبلی: آن fix فقط جلوی null شدن آیتم‌های *جدید* را
    می‌گیرد. این تابع آیتم‌هایی را که از قبل با archive_path=None در صف
    نوشته شده‌اند اصلاح می‌کند. حتماً باید بعد از bootstrap_source_index
    (یا بعد از چند اجرای عادی که ایندکس را پر کرده باشند) اجرا شود، وگرنه
    ایندکس هنوز نگاشت لازم را ندارد.
    """
    log("=" * 60)
    log("🔧 Repair یک‌باره: پر کردن archive_path=null در صف موجود portfolios")
    log("=" * 60)

    content = gh_api_get(repo, "all_combinations_portfolios.json")
    if content is None:
        log("  all_combinations_portfolios.json یافت نشد — کاری برای repair نیست", 'WARNING')
        return 0
    try:
        combos = json.loads(content)
        if not isinstance(combos, list):
            log("  محتوای صف از نوع لیست نیست — repair متوقف شد", 'ERROR')
            return 0
    except Exception as e:
        log(f"  خطا در parse صف: {e}", 'ERROR')
        return 0

    null_items = [
        it for it in combos
        if isinstance(it, dict) and it.get("signature_path") and not it.get("archive_path")
    ]
    log(f"  تعداد کل آیتم‌ها: {len(combos)} — آیتم‌های archive_path=null: {len(null_items)}")

    if not null_items:
        log("  هیچ آیتم null ای برای اصلاح وجود ندارد ✅")
        return 0

    sig_to_archive = _load_source_index(repo)
    if not sig_to_archive:
        log("  ⚠️ ایندکس signature_archive_index.json خالی است — ابتدا BOOTSTRAP_SOURCE_INDEX=1 را اجرا کنید", 'WARNING')
        return 0

    fixed_count = 0
    still_missing = 0
    for it in combos:
        if isinstance(it, dict) and it.get("signature_path") and not it.get("archive_path"):
            found = sig_to_archive.get(it["signature_path"])
            if found:
                it["archive_path"] = found
                fixed_count += 1
            else:
                still_missing += 1

    log(f"  ✅ {fixed_count} آیتم اصلاح شد — {still_missing} آیتم همچنان بدون archive_path ماند "
        f"(احتمالاً آرشیوشان دیگر در signature_archives/ وجود ندارد)")

    if fixed_count == 0:
        return 0

    tmp_file = "/tmp/all_combinations_portfolios_repaired.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(combos, f, indent=2, ensure_ascii=False)

    sha = get_file_sha(repo, "all_combinations_portfolios.json")
    if not upload_file_with_curl(repo, "all_combinations_portfolios.json", tmp_file, sha, "repair null archive_path in portfolios queue"):
        raise RuntimeError("آپلود صف اصلاح‌شده‌ی portfolios ناموفق بود")

    log(f"✅ صف all_combinations_portfolios.json با {fixed_count} archive_path بازیابی‌شده به‌روزرسانی شد")
    return fixed_count


# ================================ تابع اصلی ================================

def main():
    global LOG_FILE
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    LOG_FILE = str(log_dir / "queue_builder.log")

    log("=" * 60)
    log("🚀 شروع build_all_queues.py")
    log("=" * 60)

    third_repo = os.environ.get('THIRD_REPO')
    gh_token = os.environ.get('GH_TOKEN')
    results_password = os.environ.get('RESULTS_PASSWORD') or os.environ.get('DATA_PASSWORD')

    log(f"  THIRD_REPO: {'✅ ' + third_repo if third_repo else '❌ تنظیم نشده'}")
    log(f"  GH_TOKEN: {'✅ تنظیم شده' if gh_token else '❌ تنظیم نشده'}")
    log(f"  RESULTS_PASSWORD: {'✅ تنظیم شده' if results_password else '⚠️ تنظیم نشده (golden/portfolios نیاز دارند)'}")

    if not third_repo or not gh_token:
        log("THIRD_REPO یا GH_TOKEN تنظیم نشده است. خروج.", 'ERROR')
        sys.exit(1)

    if os.environ.get('BOOTSTRAP_SOURCE_INDEX') == '1':
        if not results_password:
            log("BOOTSTRAP_SOURCE_INDEX=1 ست شده اما RESULTS_PASSWORD موجود نیست. خروج.", 'ERROR')
            sys.exit(1)
        try:
            bootstrap_source_index(third_repo, results_password)
        except Exception as e:
            log(f"❌ Bootstrap ایندکس با خطا مواجه شد: {e}", 'ERROR')
            sys.exit(1)

    if os.environ.get('REPAIR_PORTFOLIOS_ARCHIVE_PATHS') == '1':
        if not results_password:
            log("REPAIR_PORTFOLIOS_ARCHIVE_PATHS=1 ست شده اما RESULTS_PASSWORD موجود نیست. خروج.", 'ERROR')
            sys.exit(1)
        try:
            repair_portfolios_queue_archive_paths(third_repo, results_password)
        except Exception as e:
            log(f"❌ Repair صف portfolios با خطا مواجه شد: {e}", 'ERROR')
            sys.exit(1)

    try:
        filtered_set = filter_strategies_by_conditions(third_repo, gh_token, results_password)
    except Exception as e:
        log(f"❌ فیلتر استراتژی‌ها بر اساس ۷ شرط با خطا مواجه شد: {e}", 'ERROR')
        sys.exit(1)

    if not filtered_set:
        log("⚠️ هیچ استراتژی‌ای شرایط ۷گانه را برآورده نکرد — خروج بدون ساخت صف", 'WARNING')
        sys.exit(0)

    results = {}

    try:
        count = build_work_queue(third_repo, gh_token, filtered_set=filtered_set)
        results['work'] = count
        log(f"✅ مرحله ۱ (work) با موفقیت تمام شد — {count} ترکیب جدید")
    except Exception as e:
        log(f"❌ مرحله ۱ (work) با خطا مواجه شد: {e}", 'ERROR')
        sys.exit(1)

    if not results_password:
        log("⚠️ RESULTS_PASSWORD تنظیم نشده — مراحل golden و portfolios رد می‌شوند", 'WARNING')
        results['golden'] = 0
        results['portfolios'] = 0
    else:
        try:
            count = build_golden_queue(third_repo, gh_token, results_password)
            results['golden'] = count
            log(f"✅ مرحله ۲ (golden) با موفقیت تمام شد — {count} signature جدید")
        except Exception as e:
            log(f"❌ مرحله ۲ (golden) با خطا مواجه شد: {e}", 'ERROR')
            sys.exit(1)

        try:
            count = build_portfolios_queue(third_repo, gh_token, results_password)
            results['portfolios'] = count
            log(f"✅ مرحله ۳ (portfolios) با موفقیت تمام شد — {count} signature جدید")
        except Exception as e:
            log(f"❌ مرحله ۳ (portfolios) با خطا مواجه شد: {e}", 'ERROR')
            sys.exit(1)

    log("=" * 60)
    log("📊 خلاصه کل عملیات:")
    log(f"   work        → {results.get('work', 0)} ترکیب جدید")
    log(f"   golden      → {results.get('golden', 0)} signature جدید")
    log(f"   portfolios  → {results.get('portfolios', 0)} signature جدید")
    log("✅ همه‌ی صف‌ها با موفقیت ساخته شدند")
    log(f"📄 فایل لاگ: {LOG_FILE}")
    log("=" * 60)

if __name__ == "__main__":
    main()
