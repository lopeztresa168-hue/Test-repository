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
#   ۲. فایل‌های خروجی نادرست را از batch_outputs/*/ (مخزن اصلی، چک‌اوت‌شده
#      لوکال) حذف می‌کند
#   ۳. همان فایل‌های نادرست را در مخزن سوم هم پیدا و پاک می‌کند — چون
#      loop_analysis.yml معمولاً batch_outputs/*/signatures و
#      */{module}_patterns را بسته‌بندی+رمزنگاری کرده و به
#      signature_archives/{module}/{strat}/*.tar.gz.enc و
#      pattern_archives/{module}/{strat}/*.tar.gz.enc در مخزن سوم منتقل
#      می‌کند و batch_outputs محلی را پاک می‌کند. اگر این مهاجرت قبل از اجرای
#      این اسکریپت انجام شده باشد، بخش ۲ (بالا) دیگر چیزی در batch_outputs
#      پیدا نمی‌کند و خروجی‌های نادرست بدون این بخش در مخزن سوم دست‌نخورده
#      باقی می‌مانند. تشخیص اینکه کدام آرشیو حاوی کدام (module,strat,coin)
#      است بدون دانلود/رمزگشایی همه‌ی آرشیوها، از روی ایندکس‌های دائمی
#      signature_archive_index.json و pattern_archive_index.json (که خود
#      build_all_queues.py نگه می‌دارد) انجام می‌شود.
#   ۴. همان ترکیب‌ها را با --session درست، دوباره به all_combinations.json
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
#
# نیازمندی‌های اجرایی برای بخش مخزن سوم: دستورهای "node" و "tar" باید در
# PATH موجود باشند (همان‌هایی که build_all_queues.py/loop_analysis.yml هم
# برای رمزگشایی/رمزنگاری AES-256-CBC استفاده می‌کنند).

import os
import sys
import json
import argparse
import subprocess
import shutil
import tarfile
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
    چون ری‌کیو در بخش ۳ آن‌ها را با --session درست از نو می‌سازد.
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
# بخش ۲: پیداکردن و پاک‌سازی ورودی‌های نادرست در مخزن سوم
# (signature_archives/ و pattern_archives/) — برای زمانی که loop_analysis.yml
# قبلاً batch_outputs را بسته‌بندی/رمزنگاری و به مخزن سوم منتقل کرده و
# batch_outputs محلی را پاک کرده است (پس بخش ۱ بالا دیگر چیزی پیدا نمی‌کند).
# ---------------------------------------------------------------------------

def _target_prefixes(mismatches):
    """پیشوندهای {module}/{strat}/{safe_coin}/ که باید داخل
    signature_archive_index.json / pattern_archive_index.json جست‌وجو شوند.
    دقیقاً همان کلیدهایی که build_all_queues.py در آن ایندکس‌ها ذخیره
    می‌کند (پیشوند 'signatures/' یا 'patterns/' از قبل از آن‌ها حذف شده)."""
    prefixes = set()
    for m in mismatches:
        strat, coin = m["strat"], m["coin"]
        for module in baq.MODULE_INTERVALS:
            prefixes.add(f"{module}/{strat}/{_safe_coin(coin)}/")
    return prefixes


def find_stale_third_repo_entries(third_repo, mismatches):
    """با استفاده از ایندکس‌های دائمی signature_archive_index.json و
    pattern_archive_index.json (بدون دانلود/رمزگشایی هیچ آرشیوی — سریع،
    مناسب برای --scan-only)، مشخص می‌کند کدام آرشیوها در
    signature_archives/ و pattern_archives/ حاوی فایل‌های نادرست هستند.

    خروجی: {"signature": {archive_path: [signature_path, ...]},
             "pattern":   {archive_path: [pattern_path, ...]}}
    """
    prefixes = _target_prefixes(mismatches)

    log("  دانلود signature_archive_index.json و pattern_archive_index.json از مخزن سوم...")
    sig_index = baq._load_source_index(third_repo)
    pat_index = baq._load_pattern_index(third_repo)

    result = {"signature": defaultdict(list), "pattern": defaultdict(list)}
    for path, archive_path in sig_index.items():
        if not archive_path:
            continue
        if any(path.startswith(p) for p in prefixes):
            result["signature"][archive_path].append(path)
    for path, archive_path in pat_index.items():
        if not archive_path:
            continue
        if any(path.startswith(p) for p in prefixes):
            result["pattern"][archive_path].append(path)

    n_sig = sum(len(v) for v in result["signature"].values())
    n_pat = sum(len(v) for v in result["pattern"].values())
    log(f"  🗄️ ورودی نادرست در signature_archive_index.json: {n_sig} (در {len(result['signature'])} آرشیو)")
    log(f"  🗄️ ورودی نادرست در pattern_archive_index.json: {n_pat} (در {len(result['pattern'])} آرشیو)")
    return {"signature": dict(result["signature"]), "pattern": dict(result["pattern"])}


def _encrypt_bytes_to_file(data_bytes, out_path, password):
    """رمزنگاری AES-256-CBC (کلید scrypt با salt='salt'، iv تصادفی ۱۶بایتی
    چسبیده به ابتدای فایل خروجی) — دقیقاً همان طرحی که loop_analysis.yml
    برای ساخت آرشیوهای signature_archives/pattern_archives استفاده می‌کند
    (و baq._decrypt_enc_bytes برای رمزگشایی آن به کار می‌رود)، تا آرشیوهای
    بازنویسی‌شده توسط این اسکریپت با بقیه‌ی خط لوله سازگار بمانند."""
    tmp_in = "/tmp/_purge_reencrypt_input.bin"
    with open(tmp_in, "wb") as f:
        f.write(data_bytes)
    node_code = f"""
      const crypto=require('crypto'),fs=require('fs');
      const pass={json.dumps(password)};
      const data=fs.readFileSync({json.dumps(tmp_in)});
      const key=crypto.scryptSync(pass,'salt',32);
      const iv=crypto.randomBytes(16);
      const cipher=crypto.createCipheriv('aes-256-cbc',key,iv);
      const enc=Buffer.concat([cipher.update(data),cipher.final()]);
      fs.writeFileSync({json.dumps(out_path)}, Buffer.concat([iv, enc]));
    """
    result = subprocess.run(["node", "-e", node_code], capture_output=True, text=True)
    try:
        os.unlink(tmp_in)
    except Exception:
        pass
    if result.returncode != 0:
        raise RuntimeError(f"رمزنگاری ناموفق: {result.stderr[:300]}")


def _delete_file_via_api(repo, path, sha, commit_msg):
    """حذف یک فایل از مخزن از طریق GitHub Contents API (معادل git rm برای
    مخزنی که لوکال چک‌اوت نشده — دقیقاً همان مخزن سومی که بقیه‌ی این اسکریپت
    هم فقط از طریق gh api/curl با آن کار می‌کند، نه git clone)."""
    cmd = [
        "gh", "api", f"repos/{repo}/contents/{path}",
        "-X", "DELETE",
        "-f", f"message={commit_msg}",
        "-f", f"sha={sha}",
        "-f", "branch=main",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log(f"    ⚠️ gh api DELETE برای {path} ناموفق بود: {result.stderr[:200]}", 'WARNING')
    return result.returncode == 0


def _remove_index_entries(repo, index_path, keys_to_remove, max_retries=3):
    """معادل 'حذف' برای baq._upload_source_index/_upload_pattern_index — آن
    دو تابع فقط ادغام/افزودن (update) انجام می‌دهند و امکان حذف key ندارند،
    و چون اجازه‌ی تغییر build_all_queues.py وجود ندارد، این نسخه‌ی مخصوصِ
    حذف اینجا (فقط برای این اسکریپت) پیاده‌سازی شده. هر تلاش نسخه‌ی تازه‌ی
    فایل را می‌خواند تا با ران‌های هم‌زمان build_all_queues.py تداخل نکند."""
    if not keys_to_remove:
        return True
    keys_set = set(keys_to_remove)
    for attempt in range(1, max_retries + 1):
        content = baq.gh_api_get(repo, index_path)
        try:
            current = json.loads(content) if content else {}
            if not isinstance(current, dict):
                current = {}
        except Exception:
            current = {}
        before = len(current)
        for k in keys_set:
            current.pop(k, None)
        removed = before - len(current)
        sha = baq.get_file_sha(repo, index_path)
        tmp_file = "/tmp/_purge_index_update.json"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=False)
        if baq.upload_file_with_curl(repo, index_path, tmp_file, sha,
                                      f"purge {removed} stale entries after session-mismatch cleanup"):
            log(f"  🧹 {index_path}: {removed} ورودی نادرست حذف شد.")
            return True
        log(f"  تلاش {attempt}/{max_retries} برای آپدیت {index_path} ناموفق بود — تلاش مجدد با sha تازه...", 'WARNING')
    log(f"  ❌ آپدیت {index_path} پس از چند تلاش ناموفق بود", 'ERROR')
    return False


def purge_third_repo_archives(third_repo, results_password, stale_entries):
    """برای هر آرشیوی که در find_stale_third_repo_entries حداقل یک ورودی
    نادرست دارد: دانلود + رمزگشایی + استخراج، حذف دقیق همان فایل‌های
    نادرست، و سپس یا بازفشرده‌سازی+رمزنگاری+آپلود دوباره (اگر فایلی از آن
    آرشیو باقی مانده) یا حذف کامل آرشیو (اگر چیزی باقی نمانده بود). در پایان
    ایندکس‌های signature_archive_index.json/pattern_archive_index.json هم
    از ورودی‌های حذف‌شده پاک‌سازی می‌شوند تا build_all_queues.py دیگر به این
    فایل‌های نابودشده اشاره نکند."""
    kinds = [
        ("signature", "signatures", ".jsonl", stale_entries.get("signature", {})),
        ("pattern", "patterns", ".csv", stale_entries.get("pattern", {})),
    ]

    removed_sig_paths = []
    removed_pat_paths = []
    deleted_archives = 0
    repacked_archives = 0

    for kind, top_dir, suffix, archives in kinds:
        for archive_path, bad_paths in archives.items():
            log(f"  📦 {archive_path}: {len(bad_paths)} ورودی نادرست")
            enc_path = "/tmp/_purge_archive.tar.gz.enc"
            tgz_path = "/tmp/_purge_archive.tar.gz"
            for p in (enc_path, tgz_path):
                if os.path.exists(p):
                    os.remove(p)

            if not baq.gh_api_get_binary(third_repo, archive_path, enc_path):
                log(f"    ⚠️ دانلود {archive_path} ناموفق بود — رد می‌شود (ایندکس هم دست‌نخورده می‌ماند)", 'WARNING')
                continue
            try:
                raw = baq._decrypt_enc_bytes(enc_path, results_password)
            except Exception as e:
                log(f"    ⚠️ رمزگشایی {archive_path} ناموفق بود: {e} — رد می‌شود", 'WARNING')
                continue
            with open(tgz_path, "wb") as f:
                f.write(raw)

            extract_dir = "/tmp/_purge_extract"
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
            os.makedirs(extract_dir)
            try:
                with tarfile.open(tgz_path, "r:gz") as tf:
                    tf.extractall(extract_dir)
            except Exception as e:
                log(f"    ⚠️ استخراج {archive_path} ناموفق بود: {e} — رد می‌شود", 'WARNING')
                continue

            top_root = os.path.join(extract_dir, top_dir)
            for bad_path in bad_paths:
                target = os.path.join(extract_dir, top_dir, bad_path)
                if os.path.exists(target):
                    os.remove(target)
                    # پاک‌سازی پوشه‌های خالی باقی‌مانده (همان کاری که
                    # delete_files_and_commit برای batch_outputs لوکال هم
                    # انجام می‌دهد) — تا داخل tar دوباره‌ساخته‌شده، ورودی
                    # پوشه‌ی خالی/بی‌ربط برای کوینی که کاملاً پاک شده جا نماند.
                    d = os.path.dirname(target)
                    while d.startswith(top_root) and d != top_root and not os.listdir(d):
                        try:
                            os.rmdir(d)
                        except OSError:
                            break
                        d = os.path.dirname(d)
                # چه فایل واقعاً داخل این آرشیو پیدا شده باشد چه نه (مثلاً
                # ایندکس قدیمی/ناهماهنگ بوده)، این ورودی نادرست است و باید
                # از ایندکس هم پاک شود.
                (removed_sig_paths if kind == "signature" else removed_pat_paths).append(bad_path)

            remaining = []
            if os.path.isdir(top_root):
                for root, _, files in os.walk(top_root):
                    for fn in files:
                        if fn.endswith(suffix):
                            remaining.append(os.path.join(root, fn))

            sha = baq.get_file_sha(third_repo, archive_path)
            if not sha:
                log(f"    ❌ گرفتن sha برای {archive_path} ناموفق بود — رد می‌شود", 'ERROR')
                continue

            if not remaining:
                if _delete_file_via_api(third_repo, archive_path, sha,
                                         "purge: remove empty archive after session-mismatch cleanup"):
                    log(f"    🗑️ آرشیو {archive_path} کاملاً حذف شد (چیزی باقی نمانده بود)")
                    deleted_archives += 1
                else:
                    log(f"    ❌ حذف آرشیو خالی {archive_path} ناموفق بود", 'ERROR')
            else:
                new_tgz = "/tmp/_purge_repacked.tar.gz"
                new_enc = "/tmp/_purge_repacked.tar.gz.enc"
                for p in (new_tgz, new_enc):
                    if os.path.exists(p):
                        os.remove(p)
                subprocess.run(["tar", "-czf", new_tgz, "-C", extract_dir, top_dir], check=True)
                with open(new_tgz, "rb") as f:
                    tgz_bytes = f.read()
                try:
                    _encrypt_bytes_to_file(tgz_bytes, new_enc, results_password)
                except Exception as e:
                    log(f"    ❌ رمزنگاری دوباره‌ی {archive_path} ناموفق بود: {e}", 'ERROR')
                    continue
                if baq.upload_file_with_curl(third_repo, archive_path, new_enc, sha=sha,
                                              commit_msg="purge session-mismatched entries from archive"):
                    log(f"    ✅ {archive_path} بازفشرده‌سازی و آپلود شد ({len(remaining)} فایل {suffix} باقی‌مانده)")
                    repacked_archives += 1
                else:
                    log(f"    ❌ آپلود {archive_path} بازفشرده‌شده ناموفق بود", 'ERROR')

    if removed_sig_paths:
        _remove_index_entries(third_repo, baq.SOURCE_INDEX_PATH, removed_sig_paths)
    if removed_pat_paths:
        _remove_index_entries(third_repo, baq.PATTERN_INDEX_PATH, removed_pat_paths)

    return {
        "deleted_archives": deleted_archives,
        "repacked_archives": repacked_archives,
        "removed_signatures": len(removed_sig_paths),
        "removed_patterns": len(removed_pat_paths),
    }


# ---------------------------------------------------------------------------
# بخش ۳: افزودن دوباره‌ی ترکیب‌های درست (با session) به all_combinations.json
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
    log(f"🗑️ تعداد فایل خروجی نادرست پیداشده در {args.batch_outputs_root} (مخزن اصلی): {len(stale_files)}")

    log("")
    log("🔍 اسکن آرشیوهای مخزن سوم (signature_archives/ و pattern_archives/) از روی ایندکس‌های دائمی...")
    third_repo_stale = find_stale_third_repo_entries(third_repo, mismatches)

    requeue_items = build_requeue_items(mismatches)
    log(f"🔁 تعداد ترکیب برای ری‌کیو (با --session درست): {len(requeue_items)}")

    if not apply_changes:
        log("")
        log("ℹ️ حالت scan-only — هیچ فایلی حذف و هیچ صفی تغییر نکرد. برای اعمال واقعی، با --apply اجرا کن.")
        sys.exit(0)

    log("")
    log("🗑️ در حال حذف فایل‌های نادرست از مخزن اصلی (batch_outputs)...")
    deleted_count = delete_files_and_commit(stale_files, repo_root=".")

    log("")
    log("🗑️ در حال پاک‌سازی ورودی‌های نادرست از مخزن سوم (signature_archives/ و pattern_archives/)...")
    third_repo_result = purge_third_repo_archives(third_repo, results_password, third_repo_stale)

    log("")
    log("🔁 در حال ری‌کیو کردن ترکیب‌ها با session درست در مخزن سوم...")
    added_count = requeue_to_third_repo(third_repo, gh_token, requeue_items)

    log("=" * 60)
    log("📊 خلاصه:")
    log(f"   (استراتژی,کوین) نادرست شناسایی‌شده: {len(mismatches)}")
    log(f"   فایل حذف‌شده از batch_outputs (مخزن اصلی): {deleted_count}")
    log(f"   آرشیو حذف‌شده در مخزن سوم (کاملاً خالی شده بود): {third_repo_result['deleted_archives']}")
    log(f"   آرشیو بازفشرده‌سازی‌شده در مخزن سوم: {third_repo_result['repacked_archives']}")
    log(f"   ورودی signature حذف‌شده از signature_archive_index.json: {third_repo_result['removed_signatures']}")
    log(f"   ورودی pattern حذف‌شده از pattern_archive_index.json: {third_repo_result['removed_patterns']}")
    log(f"   ترکیب جدید اضافه‌شده به all_combinations.json: {added_count}")
    log("✅ پاک‌سازی و ری‌کیو تمام شد.")
    log("=" * 60)


if __name__ == "__main__":
    main()
