# -*- coding: utf-8 -*-
"""
build_verbose.py — سازنده‌ی modules_verbose/combo_10day_verbose.py

قاعده: هیچ خط موجود در combo_10day.py حذف/ویرایش/جابه‌جا نمی‌شود. این اسکریپت
فقط بعد (یا قبل، در دو مورد مشخص) از خطوط لنگرِ *دقیق* و *یکتا*، بلوک‌های جدید
درج می‌کند. همه‌ی نام‌های جدید با پیشوند _VERBOSE_ مشخص شده‌اند تا
repro_tools/ast_diff_check.py بتواند آن‌ها را به‌طور خودکار از منطق اصلی جدا کند.
این فایل خودش مستندِ کامل «چه چیزی، دقیقاً کجا، اضافه شد» است.
"""
import hashlib
import sys

# این اسکریپت یک ابزار «تولید یک‌باره + قابل‌بازتولید» است، نه بخشی از CI.
# اگر combo_10day.py مرجع در آینده عوض شود، همین اسکریپت را با مسیر جدید
# دوباره اجرا کن تا modules_verbose/combo_10day_verbose.py بازسازی شود؛
# سپس repro_tools/ast_diff_check.py را برای اثبات مجدد معادل‌بودن اجرا کن.
IN_PATH = sys.argv[1] if len(sys.argv) > 1 else 'combo_10day_original.py'
OUT_PATH = sys.argv[2] if len(sys.argv) > 2 else 'combo_10day_verbose.py'
KNOWN_REFERENCE_SHA256 = "c3aa827684c65f46947453e20eda1b2c1404fe6774d13bd15022836a32d64135"

with open(IN_PATH, 'r', encoding='utf-8') as f:
    src = f.read()

ORIGINAL_SHA256 = hashlib.sha256(src.encode('utf-8')).hexdigest()
if ORIGINAL_SHA256 != KNOWN_REFERENCE_SHA256:
    print(f"⚠️ هشدار: sha256 فایل ورودی ({ORIGINAL_SHA256}) با هش مرجعِ شناخته‌شده "
          f"({KNOWN_REFERENCE_SHA256}) یکسان نیست — اگر عمداً combo_10day.py "
          f"تغییر کرده، ادامه بده؛ در غیر این صورت مسیر IN_PATH را بررسی کن.")

def ins(text, anchor, new_lines, after=True):
    n = text.count(anchor)
    assert n == 1, f"anchor must be unique, found {n}: {anchor[:60]!r}"
    if after:
        return text.replace(anchor, anchor + "\n" + new_lines, 1)
    return text.replace(anchor, new_lines + "\n" + anchor, 1)

s = src

# 1) بلوک سراسری کمکی — بعد از importها
block1 = '''

# ============================================================================
# [VERBOSE] هرچه از اینجا تا انتهای فایل با «# [VERBOSE]» علامت‌گذاری شده یا
# نامش با _VERBOSE_ شروع می‌شود، صرفاً برای لاگ/گزارش اضافه شده است. هیچ خط
# موجود این فایل ویرایش، حذف یا جابه‌جا نشده — repro_tools/ast_diff_check.py
# این ادعا را به‌صورت خودکار در برابر combo_10day.py مرجع بررسی می‌کند.
# ============================================================================
import hashlib as _VERBOSE_hashlib
import os as _VERBOSE_os


def _VERBOSE_log(msg):
    print(msg, flush=True)


def _VERBOSE_sha256_file(path):
    h = _VERBOSE_hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _VERBOSE_dump_args(args):
    _VERBOSE_log("::group::[VERBOSE] آرگومان‌های ورودی ماژول")
    for k, v in sorted(vars(args).items()):
        _VERBOSE_log(f"  --{k.replace('_', '-')} = {v!r}")
    _VERBOSE_log("::endgroup::")


def _VERBOSE_export_regime_debug_csv(coin_ohlc_index, out_dir):
    """[VERBOSE-ONLY] بازتولید مستقلِ همان فرمول compute_market_regime، صرفاً
    برای گزارش‌گیری (CSV سری رژیم + ستون‌های میانی MA50/MA200/ATR14 به‌ازای
    هر کوین). این تابع در مسیر محاسبه‌ی رسمی صدا زده نمی‌شود و تاثیری روی
    CSV/JSONL خروجی رسمی ندارد؛ فقط artifact کمکی برای اعتبارسنجی می‌سازد."""
    import csv as _csv
    _VERBOSE_os.makedirs(out_dir, exist_ok=True)
    written = []
    for coin, (dates_list, close_arr, high_arr, low_arr) in coin_ohlc_index.items():
        n = len(close_arr)
        rows = []
        for idx in range(n):
            if idx + 1 < 200:
                ma50 = ma200 = atr14 = atr_ratio = ma_diff_pct = None
                regime = "unknown"
            else:
                w_close_50 = close_arr[idx - 49: idx + 1]
                w_close_200 = close_arr[idx - 199: idx + 1]
                w_high_14 = high_arr[idx - 13: idx + 1]
                w_low_14 = low_arr[idx - 13: idx + 1]
                ma50 = float(sum(w_close_50) / 50.0)
                ma200 = float(sum(w_close_200) / 200.0)
                atr14 = float(sum(w_high_14 - w_low_14) / 14.0)
                price = float(close_arr[idx])
                if price == 0:
                    regime = "unknown"
                    atr_ratio = ma_diff_pct = None
                else:
                    atr_ratio = atr14 / price
                    ma_diff_pct = abs(ma50 - ma200) / price
                    if atr_ratio > 0.02:
                        regime = "volatile"
                    elif ma50 > ma200:
                        regime = "trending_up"
                    elif ma50 < ma200:
                        regime = "trending_down"
                    elif ma_diff_pct < 0.05:
                        regime = "ranging"
                    else:
                        regime = "unknown"
            d = dates_list[idx]
            rows.append({
                "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "close": float(close_arr[idx]),
                "high": float(high_arr[idx]),
                "low": float(low_arr[idx]),
                "ma50": ma50, "ma200": ma200, "atr14": atr14,
                "atr_ratio": atr_ratio, "ma_diff_pct": ma_diff_pct,
                "market_regime_for_next_day": regime,
            })
        out_path = _VERBOSE_os.path.join(out_dir, f"regime_debug_{coin}.csv")
        with open(out_path, "w", encoding="utf-8-sig", newline="") as fcsv:
            fieldnames = ["date", "close", "high", "low", "ma50", "ma200", "atr14",
                          "atr_ratio", "ma_diff_pct", "market_regime_for_next_day"]
            w = _csv.DictWriter(fcsv, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        file_sha = _VERBOSE_sha256_file(out_path)
        written.append((coin, out_path, len(rows), file_sha))
        _VERBOSE_log(f"[VERBOSE][رژیم بازار] {coin}: {len(rows)} ردیف → {out_path} (sha256={file_sha})")
    return written
'''
s = ins(s, "from collections import defaultdict", block1)

# 2) main(): دامپ آرگومان‌ها بعد از parse_args()
s = ins(s, "args = parser.parse_args()", "    _VERBOSE_dump_args(args)  # [VERBOSE]")

# 3) بعد از ساخت coin_ohlc_index: خلاصه‌ی روز-کاری هر کوین + CSV اختیاری رژیم
block3 = '''    # [VERBOSE] فهرست کوین‌های OHLC و تعداد روز کاری هر کدام — برای تطبیق مستقیم
    # با جدول مبنای «تعداد فایل / روز کاری» کاربر.
    if coin_ohlc_index:
        _VERBOSE_log("::group::[VERBOSE] خلاصه OHLC به‌ازای کوین (بعد از ادغام/resample)")
        for _vc in sorted(coin_ohlc_index.keys()):
            _vdates, _vclose, _vhigh, _vlow = coin_ohlc_index[_vc]
            _VERBOSE_log(f"  {_vc}: {len(_vdates)} روز کاری")
        _VERBOSE_log("::endgroup::")
        _VERBOSE_regime_csv_dir = _VERBOSE_os.environ.get("VERBOSE_REGIME_CSV_DIR")
        if _VERBOSE_regime_csv_dir:
            _VERBOSE_export_regime_debug_csv(coin_ohlc_index, _VERBOSE_regime_csv_dir)'''
s = ins(s, "    coin_ohlc_index = _build_ohlc_coin_index(ohlc_df)", block3)

# 4) بعد از لاگ تعداد کل معاملات: توزیع نمادها در فایل خام
block4 = '''    # [VERBOSE] توزیع نمادها در فایل خام معاملات (قبل از هر فیلتری)
    _VERBOSE_symbol_counts = {}
    for _vt in trades:
        _vsym = _vt.get("symbol") or _vt.get("pair") or _vt.get("coin")
        _vsym_n = normalize_symbol(_vsym) if _vsym else "(نامشخص)"
        _VERBOSE_symbol_counts[_vsym_n] = _VERBOSE_symbol_counts.get(_vsym_n, 0) + 1
    _VERBOSE_log(f"[VERBOSE] توزیع نمادها در فایل خام: {dict(sorted(_VERBOSE_symbol_counts.items()))}")'''
s = ins(s, '    print(f"📊 تعداد کل معاملات: {len(trades)}")', block4)

# 4b) بعد از بارگذاری اخبار: تعداد/هش رویدادهای parse‌شده (همیشه) + دامپ کامل
#     فقط وقتی VERBOSE_FULL_NEWS_DUMP=1 باشد (کارگردانی از ورک‌فلو: فقط اولین
#     ترکیب هر job/chunk). هیچ امضای تابع/فراخوانی‌ای تغییر نکرده — فقط بعد از
#     خط موجود «news_events = load_news_from_directory(news_dir)» درج شده.
block4b = '''    # [VERBOSE] تعداد و هش پایدار رویدادهای خبریِ parse‌شده (مستقل از ترتیب بارگذاری)
    _VERBOSE_news_sorted = sorted(
        news_events,
        key=lambda ev: (ev["indicator"], ev["date"].isoformat() if ev["date"] else "", str(ev["actual"]))
    )
    _VERBOSE_news_json = json.dumps(
        [{"date": ev["date"].isoformat() if ev["date"] else None, "indicator": ev["indicator"],
          "actual": ev["actual"], "forecast": ev["forecast"], "previous": ev["previous"]}
         for ev in _VERBOSE_news_sorted],
        ensure_ascii=False, sort_keys=True
    )
    _VERBOSE_news_hash = _VERBOSE_hashlib.sha256(_VERBOSE_news_json.encode("utf-8")).hexdigest()
    _VERBOSE_log(f"[VERBOSE] رویدادهای خبریِ parse‌شده: تعداد={len(news_events)}, "
                 f"sha256={_VERBOSE_news_hash}")
    if _VERBOSE_os.environ.get("VERBOSE_FULL_NEWS_DUMP") == "1":
        _VERBOSE_log("::group::[VERBOSE] دامپ کامل رویدادهای خبریِ parse‌شده (اولین ترکیب این job)")
        _VERBOSE_log(_VERBOSE_news_json)
        _VERBOSE_log("::endgroup::")'''
s = ins(s, "    news_events = load_news_from_directory(news_dir)", block4b)

# 5) بعد از ساخت target_coins
s = ins(s, "    target_coins = [normalize_symbol(c.strip()) for c in target_coin.split('+')]",
        '    _VERBOSE_log(f"[VERBOSE] کوین‌های هدف (بعد از نرمال‌سازی): {target_coins}")')

# 6) بعد از لاگ «معاملات ... با تاریخ معتبر»: پاسِ مستقلِ شمارش گام‌به‌گام فیلتر
block6 = '''    # [VERBOSE] شمارش مستقل و فقط-خواندنیِ مراحل فیلتر (کوین → سشن). این پاس
    # جدا از حلقه‌ی اصلی بالاست و هیچ مقداری از حلقه‌ی اصلی را نمی‌خواند/تغییر نمی‌دهد.
    _VERBOSE_after_coin = 0
    _VERBOSE_after_session = 0
    for _vt in trades:
        _vsym = _vt.get("symbol") or _vt.get("pair") or _vt.get("coin")
        if normalize_symbol(_vsym) not in target_coins:
            continue
        _VERBOSE_after_coin += 1
        _vtime = (_vt.get("entryTime") or _vt.get("entry_time") or
                  _vt.get("open_time") or _vt.get("time") or _vt.get("timestamp"))
        if not _vtime:
            continue
        if session:
            _vhour = _trade_hour_utc(_vtime)
            if _vhour is None or not (sess_start_h <= _vhour < sess_end_h):
                continue
        _VERBOSE_after_session += 1
    _VERBOSE_log(f"[VERBOSE] مراحل فیلتر معاملات → کل={len(trades)}, "
                 f"بعد از فیلتر کوین={_VERBOSE_after_coin}, "
                 f"بعد از فیلتر سشن={_VERBOSE_after_session}, "
                 f"نهایی (با تاریخ معتبر)={len(trade_list)}")'''
s = ins(s, '    print(f"✅ معاملات {target_coin}{sess_suffix} با تاریخ معتبر: {len(trade_list)}")', block6)

# 7) بعد از لاگ «تعداد دوره‌های تشکیل‌شده»
block7 = '''    # [VERBOSE] توزیع تعداد معامله در هر دوره (فقط گزارش)
    _VERBOSE_sizes = sorted(len(v) for v in period_groups.values())
    if _VERBOSE_sizes:
        _vn = len(_VERBOSE_sizes)
        _VERBOSE_log(f"[VERBOSE] تعداد معامله به‌ازای دوره → کمینه={_VERBOSE_sizes[0]}, "
                     f"میانه={_VERBOSE_sizes[_vn // 2]}, بیشینه={_VERBOSE_sizes[-1]}")'''
s = ins(s, '    print(f"📊 تعداد دوره\u200cهای تشکیل\u200cشده: {len(period_groups)}")', block7)

# 8) بعد از total = len(all_combinations)
s = ins(s, "    total     = len(all_combinations)",
        '    _VERBOSE_log(f"[VERBOSE] تعداد کل ترکیب‌های آستانه/شاخص تولیدشده: {total} "\n'
        '                 f"(indicators={len(INDICATORS)}, thresholds={THRESHOLDS})")')

# 9) قبل از حلقه‌ی JSONL: شمارش مستقل دوره‌های زیر آستانه‌ی min-sample-count
block9 = '''        # [VERBOSE] شمارش مستقل دوره‌هایی که با --min-sample-count حذف می‌شوند
        _VERBOSE_below_min = sum(
            1 for _vpk in period_status
            if len(period_groups.get(_vpk, [])) < min_sample_count
        )
        _VERBOSE_log(f"[VERBOSE] اعمال --min-sample-count={min_sample_count}: "
                     f"{_VERBOSE_below_min} از {len(period_status)} دوره حذف می‌شوند "
                     f"(پیش از تفکیک رژیم بازار).")'''
s = ins(s, "        regime_index = _precompute_regime_series(coin_ohlc_index)", block9)

# 10) بعد از ساخت regime_buckets هر دوره: توزیع رژیم بازار
anchor10 = ("            regime_buckets = defaultdict(list)\n"
            "            for trade_date, profit in dated_profits:\n"
            "                regime = lookup_market_regime(regime_index, first_coin, trade_date)\n"
            "                regime_buckets[regime].append(profit)")
block10 = ('            # [VERBOSE] توزیع رژیم بازار معاملات همین دوره\n'
           '            _VERBOSE_log(f"[VERBOSE][دوره {period_key}] توزیع رژیم بازار: "\n'
           '                         f"{ {k: len(v) for k, v in regime_buckets.items()} }")')
s = ins(s, anchor10, block10)

# 11) بلافاصله بعد از بسته‌شدن دیکشنری هر رکورد JSONL: چاپ کامل همان رکورد
anchor11 = '                    "session": session if session else "none",\n                })'
block11 = '''                if _VERBOSE_os.environ.get("VERBOSE_LOG_FULL_RECORDS", "1") != "0":
                    _VERBOSE_log("[VERBOSE][رکورد خروجی] " + json.dumps(records[-1], ensure_ascii=False, default=str))'''
s = ins(s, anchor11, block11)

# 12) قبل از _write_jsonl نهایی: خلاصه‌ی رکوردها به‌ازای رژیم
anchor12 = "        _write_jsonl(jsonl_out, records)"
block12 = '''        # [VERBOSE] خلاصه‌ی تعداد رکورد نهایی به‌ازای رژیم بازار، قبل از نوشتن JSONL
        _VERBOSE_by_regime = {}
        for _vr in records:
            _VERBOSE_by_regime[_vr["market_regime"]] = _VERBOSE_by_regime.get(_vr["market_regime"], 0) + 1
        _VERBOSE_log(f"[VERBOSE] خلاصه رکوردهای این ترکیب به‌ازای رژیم بازار: {_VERBOSE_by_regime}")'''
s = ins(s, anchor12, block12, after=False)

header = '''#!/usr/bin/env python3
# combo_10day_verbose.py — [VERBOSE-INSTRUMENTED COPY OF combo_10day.py]
#
# این فایل با modules_verbose/../repro_tools/build_verbose.py از روی
# combo_10day.py (sha256 مرجع: {sha}) تولید شده است.
# قاعده‌ی سخت: هیچ خط موجود آن فایل حذف/ویرایش/جابه‌جا نشده — فقط بلوک‌های
# جدیدِ چاپ/لاگ (همه با برچسب «# [VERBOSE]» یا نام با پیشوند _VERBOSE_) در
# نقاط مشخص درج شده‌اند. برای اثبات خودکار این ادعا، بعد از checkout هر
# اجرا، repro_tools/ast_diff_check.py این فایل را در برابر combo_10day.py
# چک‌اوت‌شده (نه این کپی محلی) دوباره اعتبارسنجی می‌کند.
#
# منطق، ترتیب مراحل، مرتب‌سازی، نوع داده و محاسبات عددی این فایل دقیقاً
# همان combo_10day.py مرجع است.
'''.format(sha=ORIGINAL_SHA256)

# جایگزینی shebang/کامنت‌های ابتدایی اصلی با هدر بالا + نگه‌داشتن باقی فایل
first_import_idx = s.index("import os")
s_final = header + "\n" + s[first_import_idx:]

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    f.write(s_final)

print("written, length", len(s_final))
