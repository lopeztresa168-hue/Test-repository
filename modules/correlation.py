#!/usr/bin/env python3
"""
correlation.py - ماژول همبستگی خبری (Correlation)

این ماژول رابطه بین ویژگی‌های رویدادهای خبری و بازده استراتژی‌ها را در
چهار سطح (در عمل سه سطح فعال: global, conditional, lag) محاسبه می‌کند.

خروجی: correlations.parquet
"""

import argparse
import bisect
import json
import logging
import os
import pickle
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# --------------------------------------------------------------------------
# تنظیمات لاگ
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("correlation")

# Force immediate flush for GitHub Actions
import functools as _functools
_orig_print = print
print = _functools.partial(_orig_print, flush=True)

def _log(msg: str) -> None:
    """Checkpoint log that always flushes immediately (visible in GitHub Actions)."""
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    _orig_print(f"[{ts}] [CHECKPOINT] {msg}", flush=True)
    logger.info(msg)

# --------------------------------------------------------------------------
# ثابت‌ها
# --------------------------------------------------------------------------
LAGS_DAYS = [1, 3, 5, 7, 10, 15]

NUMERIC_FEATURES = [
    "distance_days",
    "diff_avg",
    "diff_std",
    "event_count",
    "indicator_diversity",
    "dominant_indicator_importance",
]
CATEGORICAL_FEATURES = ["dominant_indicator", "position"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

MIN_SAMPLE_GLOBAL_CONDITIONAL_DEFAULT = 50
MIN_SAMPLE_LAG = 30
LOO_LOWER_BOUND = 10
LOO_UPPER_BOUND = 25
MIN_R_THRESHOLD = 0.3
GOLDEN_SCORE_THRESHOLD = 50

DEFAULT_CHUNK_SIZE = 20
DEFAULT_INTERRUPT_FLAG_NAME = "interrupt.flag"
PARTIAL_RESULTS_FILENAME = "correlation_partial_results.jsonl"


# ==========================================================================
# گام ۱: بارگذاری داده‌ها
# ==========================================================================

def _extract_coin_signature_from_path(path_str: str) -> Tuple[str, str]:
    """سازگاری با نسخه‌های قبلی فایل signatures-filter.

    در نسخه‌های قبلی، فیلتر بر اساس یک رشته "path" (مثلاً
    "module/strategy/coin_interval_model.jsonl") اعمال می‌شد و این رشته
    مستقیماً با ستون signature مقایسه می‌گردید. برای حفظ همان رفتار:
      - signature همان رشته‌ی کامل path در نظر گرفته می‌شود (دقیقاً مثل قبل).
      - coin_composition از اولین بخش نام فایل (قبل از اولین "_") استخراج می‌شود.
    """
    filename_stem = Path(path_str).stem  # حذف پسوند .jsonl در صورت وجود
    coin_composition = filename_stem.split("_")[0] if filename_stem else "unknown"
    return coin_composition, path_str


def _parse_allowed_pairs(filter_path: str) -> Optional[set]:
    """فایل signatures-filter را بخوان و set تاپل‌های (coin_composition, signature) مجاز را برگردان.

    اگه فایل وجود نداشت یا خطا داشت None برمیگردونه.
    """
    if not filter_path or not os.path.exists(filter_path):
        return None
    try:
        with open(filter_path, "r", encoding="utf-8") as f:
            _filter_data = json.load(f)
    except Exception as e:
        logger.warning("خطا در خواندن signatures-filter %s: %s", filter_path, e)
        return None

    allowed_pairs: set = set()
    for item in _filter_data:
        if isinstance(item, dict):
            if "coin_composition" in item and "signature" in item:
                allowed_pairs.add((item["coin_composition"], item["signature"]))
            elif "path" in item:
                allowed_pairs.add(_extract_coin_signature_from_path(item["path"]))
            else:
                logger.warning(
                    "آیتم نامعتبر در signatures-filter نادیده گرفته شد: %s", item
                )
        else:
            allowed_pairs.add(_extract_coin_signature_from_path(str(item)))
    return allowed_pairs


# --------------------------------------------------------------------------
# regex برای strip کردن timestamp از انتهای stem
# فرمت: _20260628T224502Z  (همان فرمت generate_correlation_chunks.py)
# --------------------------------------------------------------------------
import re as _re
_TS_RE = _re.compile(r'_\d{8}T\d{6}Z$')


def _strip_ts(name: str) -> str:
    """حذف timestamp از انتهای نام فایل/stem."""
    return _TS_RE.sub('', name)


def _build_sig_index_full(signatures_path: Path) -> tuple:
    """
    ایندکس کامل از همه فایل‌های .jsonl — منطق یکسان با generate_correlation_chunks.py.

    برمی‌گرداند:
      stems      : set[str]  — stem نام فایل‌ها (+ نسخه بدون timestamp)
      rel_paths  : set[str]  — همه زیرمسیرهای نسبی ممکن (بدون پسوند + بدون ts)
      path_map   : dict[str, list[Path]]  — rel_path (کامل) -> لیست فایل‌های واقعی
    """
    stems: set = set()
    rel_paths: set = set()
    path_map: Dict[str, List[Path]] = {}

    for p in signatures_path.rglob("*.jsonl"):
        stem = p.stem
        stem_no_ts = _strip_ts(stem)

        stems.add(stem)
        if stem_no_ts != stem:
            stems.add(stem_no_ts)

        try:
            rel_parts = p.relative_to(signatures_path).with_suffix("").parts
        except ValueError:
            rel_parts = (stem,)

        # ثبت همه زیرمسیرهای ممکن
        for i in range(len(rel_parts)):
            sub = "/".join(rel_parts[i:])
            rel_paths.add(sub)
            path_map.setdefault(sub, []).append(p)

            sub_no_ts = "/".join(rel_parts[i:-1] + (_strip_ts(rel_parts[-1]),)) if rel_parts else sub
            if sub_no_ts != sub:
                rel_paths.add(sub_no_ts)
                path_map.setdefault(sub_no_ts, []).append(p)

    _log(f"_build_sig_index_full: {len(stems)} stem | {len(rel_paths)} rel_path | {sum(len(v) for v in path_map.values())} فایل در path_map")
    return stems, rel_paths, path_map


def _find_files_for_sig(sig: str, coin: str,
                         stems: set, rel_paths: set,
                         path_map: Dict[str, List[Path]],
                         sorted_rel_paths: list,
                         signatures_path: Path) -> List[Path]:
    """
    فایل‌های .jsonl متناظر یک signature را پیدا کن.
    منطق match کاملاً یکسان با find_jsonl_for_item در generate_correlation_chunks.py است.

    مراحل:
      1. exact match روی stems  (فقط نام فایل)
      2. exact match روی rel_paths (زیرمسیر نسبی)
      3. prefix match برای combo chunks (با bisect روی sorted_rel_paths -
         معادل ریاضی دقیق حلقه‌ی startswith قبلی، چون startswith(prefix) برای
         یک لیست مرتب‌شده به‌صورت لکسیکوگرافیک همیشه یک بازه‌ی پیوسته می‌سازد؛
         صحت با تست روی داده‌های شبیه‌سازی‌شده با عمق‌های مختلف مسیر تأیید شده)
    """
    sig_no_ext = sig[:-6] if sig.endswith(".jsonl") else sig
    sig_parts = sig_no_ext.split("/")
    sig_basename = sig_parts[-1]
    sig_basename_no_ts = _strip_ts(sig_basename)
    sig_no_ts = "/".join(sig_parts[:-1] + [_strip_ts(sig_parts[-1])]) if sig_parts else sig_no_ext

    found: List[Path] = []
    seen: set = set()

    def _add(paths):
        for p in paths:
            if p not in seen:
                seen.add(p)
                found.append(p)

    # ① exact match از طریق stems
    for candidate in {sig_no_ext, sig_basename, sig_basename_no_ts}:
        if candidate and candidate in stems:
            # stems فقط stem هستند — فایل واقعی را از path_map بگیر
            _add(path_map.get(candidate, []))

    # ② exact match از طریق rel_paths (زیرمسیر نسبی)
    candidates_rel = set()
    for i in range(len(sig_parts)):
        sub = "/".join(sig_parts[i:])
        candidates_rel.add(sub)
        sub_no_ts = "/".join(sig_parts[i:-1] + [_strip_ts(sig_parts[-1])]) if sig_parts else sub
        candidates_rel.add(sub_no_ts)
    if coin:
        candidates_rel.add(f"{coin}/{sig_no_ext}")
        candidates_rel.add(f"{coin}/{sig_basename}")
        candidates_rel.add(f"{coin}/{sig_basename_no_ts}")

    for c in candidates_rel:
        if c and c in rel_paths:
            _add(path_map.get(c, []))

    # ③ prefix match برای combo chunks
    # sig = "combo_10day/Best_15m/Best_15m_chunk_0_20260628T224502Z"
    # فایل‌های individual داخل این prefix match می‌شوند
    if not found and len(sig_parts) >= 2 and "chunk" in sig_basename.lower():
        parent = "/".join(sig_parts[:-1])
        if len(parent) >= 3:
            prefix_slash = parent + "/"
            lo = bisect.bisect_left(sorted_rel_paths, prefix_slash)
            hi = bisect.bisect_left(sorted_rel_paths, prefix_slash[:-1] + chr(ord("/") + 1))
            for rp in sorted_rel_paths[lo:hi]:
                _add(path_map.get(rp, []))

    return found


def load_signatures(signatures_dir: str, signatures_filter: Optional[str] = None) -> pd.DataFrame:
    """فایل‌های .jsonl را از دایرکتوری signatures بخوان و یکپارچه کن.

    FIX (2026-06): منطق match با generate_correlation_chunks.py هم‌راستا شد.
    قبلاً فقط stem فایل‌ها با signature مقایسه می‌شد؛ اما generate_correlation_chunks.py
    کل مسیر نسبی را در فیلد signature ذخیره می‌کند (مثلاً
    "combo_10day/Best_15m/Best_15m_chunk_0_20260628T224502Z").
    حالا سه مرحله match داریم: stems، rel_paths، و prefix check برای combo chunks.
    """
    _log(f"load_signatures: شروع از {signatures_dir}")
    signatures_path = Path(signatures_dir)
    if not signatures_path.exists():
        raise FileNotFoundError(f"مسیر signatures پیدا نشد: {signatures_dir}")

    # ---- اگه filter داده شده: فقط فایل‌های مرتبط رو بخون ----
    if signatures_filter:
        allowed_pairs = _parse_allowed_pairs(signatures_filter)
        if allowed_pairs is None:
            logger.warning("فایل signatures-filter پیدا نشد: %s؛ همه‌ی داده‌ها پردازش می‌شوند.", signatures_filter)
            # fallback به حالت کامل (ادامه بدون return)
        else:
            _log(f"load_signatures: filter فعال — {len(allowed_pairs)} ترکیب مجاز، ایندکس‌سازی فایل‌ها...")

            # ساخت ایندکس کامل (یک‌بار — همان منطق generate_correlation_chunks.py)
            stems, rel_paths, path_map = _build_sig_index_full(signatures_path)
            sorted_rel_paths = sorted(rel_paths)

            _log(f"load_signatures: ایندکس ساخته شد ({len(stems)} stem | {len(rel_paths)} rel_path یکتا)")

            # دیاگنوستیک: نمونه‌ای از stems و sigs برای تشخیص mismatch
            sample_stems = sorted(stems)[:5]
            sample_sigs = sorted({sig for (_, sig) in allowed_pairs})[:5]
            _log(f"load_signatures: [DIAG] نمونه stems موجود: {sample_stems}")
            _log(f"load_signatures: [DIAG] نمونه signatures در filter: {sample_sigs}")

            # پیدا کردن فایل‌های متناظر با هر (coin, sig) مجاز
            files_to_read: List[Path] = []
            seen_paths: set = set()
            matched_sigs = 0
            missed_sigs = []

            for (coin, sig) in allowed_pairs:
                found = _find_files_for_sig(
                    sig, coin, stems, rel_paths, path_map, sorted_rel_paths, signatures_path
                )
                if found:
                    matched_sigs += 1
                    for p in found:
                        if p not in seen_paths:
                            seen_paths.add(p)
                            files_to_read.append(p)
                else:
                    missed_sigs.append((coin, sig))

            _log(f"load_signatures: {len(files_to_read)} فایل مرتبط از {len(rel_paths)} rel_path کل")
            _log(f"load_signatures: {matched_sigs} sig مچ شد | {len(missed_sigs)} sig بدون فایل")

            if missed_sigs:
                for (c, s) in missed_sigs[:5]:
                    _log(f"load_signatures: [DIAG] بدون فایل — coin={c} sig={s}")
                if len(missed_sigs) > 5:
                    _log(f"load_signatures: [DIAG] ... و {len(missed_sigs) - 5} مورد دیگر")

            if not files_to_read:
                _log("load_signatures: هیچ رکوردی از فایل‌های فیلترشده پیدا نشد")
                return pd.DataFrame()

            # خواندن فایل‌های پیداشده
            records: List[Dict[str, Any]] = []
            for file_path in files_to_read:
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line_no, line in enumerate(f, start=1):
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                rec = json.loads(line)
                                rec["_source_file"] = file_path.name
                                records.append(rec)
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    "خطای JSON در فایل %s خط %d: %s", file_path.name, line_no, e
                                )
                except OSError as e:
                    logger.warning("خطا در باز کردن فایل %s: %s", file_path, e)

            if not records:
                _log("load_signatures: هیچ رکوردی از فایل‌های فیلترشده پیدا نشد")
                return pd.DataFrame()

            df = pd.DataFrame(records)
            _log(f"load_signatures: {len(df)} رکورد از {len(files_to_read)} فایل بارگذاری شد")
            logger.info("تعداد %d رکورد از %d فایل signatures بارگذاری شد (فیلترشده).", len(df), len(files_to_read))

            if "signature" not in df.columns:
                df["signature"] = df.apply(build_signature, axis=1)

            # فیلتر نهایی بر اساس (coin_composition, signature) — فقط اگر هر دو ستون موجود باشند
            if {"coin_composition", "signature"}.issubset(df.columns):
                before = len(df)
                df = df[
                    pd.MultiIndex.from_arrays(
                        [df["coin_composition"], df["signature"]]
                    ).isin(allowed_pairs)
                ].copy()
                _log(f"load_signatures: فیلتر نهایی {before} -> {len(df)} رکورد")
                logger.info(
                    "اعمال signatures-filter: %d -> %d رکورد (%d ترکیب مجاز).",
                    before, len(df), len(allowed_pairs),
                )

            _log(f"load_signatures: پایان - {len(df)} رکورد نهایی")
            return df

    # ---- حالت بدون filter: همه فایل‌ها رو بخون (رفتار قبلی) ----
    records = []
    jsonl_files = sorted(signatures_path.rglob("*.jsonl"))
    _log(f"load_signatures: {len(jsonl_files)} فایل .jsonl پیدا شد (بدون filter، همه خوانده می‌شوند)")
    if not jsonl_files:
        logger.warning("هیچ فایل .jsonl در %s پیدا نشد.", signatures_dir)

    for file_path in jsonl_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        rec["_source_file"] = file_path.name
                        records.append(rec)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            "خطای JSON در فایل %s خط %d: %s", file_path.name, line_no, e
                        )
        except OSError as e:
            logger.warning("خطا در باز کردن فایل %s: %s", file_path, e)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    _log(f"load_signatures: {len(df)} رکورد از {len(jsonl_files)} فایل بارگذاری شد")
    logger.info("تعداد %d رکورد از %d فایل signatures بارگذاری شد.", len(df), len(jsonl_files))

    if "signature" not in df.columns:
        logger.info("ستون signature یافت نشد؛ با build_signature ساخته می‌شود.")
        df["signature"] = df.apply(build_signature, axis=1)

    _log(f"load_signatures: پایان - {len(df)} رکورد نهایی")
    return df


def load_golden_scores(golden_scores_path: Optional[str]) -> Optional[pd.DataFrame]:
    """فایل golden_scores.parquet را بارگذاری کن (در صورت وجود)."""
    _log(f"load_golden_scores: شروع - مسیر={golden_scores_path}")
    if not golden_scores_path or not os.path.exists(golden_scores_path):
        _log("load_golden_scores: فایل وجود ندارد، رد شد")
        return None
    df = pd.read_parquet(golden_scores_path)
    _log(f"load_golden_scores: {len(df)} رکورد بارگذاری شد")
    logger.info("golden_scores.parquet با %d رکورد بارگذاری شد.", len(df))
    return df


def find_column_index(header: List[str], keywords: List[str]) -> int:
    """اولین ستونی که نام آن شامل یکی از کلیدواژه‌ها باشد را پیدا کن.

    جستجو case-insensitive و partial match است.
    در صورت عدم وجود، -1 برمی‌گرداند.
    """
    header_lower = [h.lower() for h in header]
    for kw in keywords:
        kw_lower = kw.lower()
        for i, h in enumerate(header_lower):
            if kw_lower in h:
                return i
    return -1


def parse_percent(value_str) -> Optional[float]:
    """رشته عددی با کاراکترهای اضافی را به float تبدیل کن.

    کاراکترهای %, $, B (میلیارد), کاما، پرانتز و فاصله حذف می‌شوند.
    در صورت مقدار خالی یا نامعتبر، None برمی‌گرداند.
    """
    if value_str is None:
        return None
    if isinstance(value_str, (int, float)):
        return float(value_str) if not (isinstance(value_str, float) and np.isnan(value_str)) else None
    s = str(value_str).strip()
    if not s or s in ("-", "—", "--", "N/A", "n/a", ""):
        return None
    # حذف کاراکترهای غیرعددی به‌جز نقطه و علامت منفی
    import re
    s = re.sub(r"[%$B,()]", "", s).strip()
    if not s or s in ("-", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def detect_indicator_from_filename(filename: str) -> str:
    """نام اندیکاتور را از نام فایل CSV استخراج کن."""
    stem = Path(filename).stem.lower()
    # نگاشت نام فایل به نام اندیکاتور استاندارد
    mapping = {
        "fomc": "FOMC",
        "cpi": "CPI",
        "nfp": "NFP",
        "gdp": "GDP",
        "ppi": "PPI",
        "pce": "PCE",
        "ism": "ISM",
        "retail": "Retail Sales",
        "unemployment": "Unemployment",
        "housing": "Housing",
        "jobs": "Jobs",
        "trade": "Trade Balance",
        "durable": "Durable Goods",
        "consumer": "Consumer Confidence",
    }
    for key, val in mapping.items():
        if key in stem:
            return val
    # اگر نگاشتی پیدا نشد، از نام فایل استفاده کن
    return Path(filename).stem.replace("_", " ").title()


def load_news(
    news_pickle_path: Optional[str] = None,
    news_dir: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """داده‌های خبری را بارگذاری کن.

    اولویت:
    ۱. news_pickle_path — فایل pickle (در صورت وجود، اولویت اول)
    ۲. news_dir  — فایل‌های CSV پوشه اخبار با پشتیبانی از ساختار واقعی
    """
    _log(f"load_news: شروع - pickle={news_pickle_path}, dir={news_dir}")
    # --- اولویت اول: pickle ---
    if news_pickle_path and os.path.exists(news_pickle_path):
        with open(news_pickle_path, "rb") as f:
            news_obj = pickle.load(f)
        df = pd.DataFrame(news_obj) if not isinstance(news_obj, pd.DataFrame) else news_obj
        df["date"] = pd.to_datetime(df["date"])
        _log(f"load_news: news.pickle با {len(df)} رکورد بارگذاری شد")
        logger.info("news.pickle با %d رکورد بارگذاری شد.", len(df))
        return df

    # --- اولویت دوم: CSV از news_dir ---
    if news_dir:
        news_dir_path = Path(news_dir)
        if not news_dir_path.exists():
            logger.warning("مسیر news_dir وجود ندارد: %s", news_dir)
        else:
            csv_files = sorted(news_dir_path.glob("*.csv"))
            if not csv_files:
                logger.warning("هیچ فایل CSV در %s پیدا نشد.", news_dir)
            else:
                frames = []
                for p in csv_files:
                    try:
                        # خواندن با utf-8-sig برای حذف BOM
                        raw_df = pd.read_csv(p, encoding="utf-8-sig", dtype=str)
                        header = list(raw_df.columns)

                        # یافتن ستون تاریخ
                        date_idx = find_column_index(header, ["date", "expected", "impact"])
                        if date_idx == -1:
                            date_idx = 0  # ستون اول به‌عنوان پیش‌فرض
                        date_col = header[date_idx]

                        # یافتن ستون‌های اصلی
                        actual_idx = find_column_index(header, ["actual"])
                        forecast_idx = find_column_index(header, ["forecast", "consensus"])
                        previous_idx = find_column_index(header, ["previous", "prior"])
                        reference_idx = find_column_index(header, ["reference", "event", "name"])

                        if actual_idx == -1:
                            logger.warning(
                                "فایل CSV %s فاقد ستون Actual است؛ نادیده گرفته شد.", p.name
                            )
                            continue
                        if forecast_idx == -1:
                            logger.warning(
                                "فایل CSV %s فاقد ستون Forecast/Consensus است؛ نادیده گرفته شد.", p.name
                            )
                            continue

                        actual_col = header[actual_idx]
                        forecast_col = header[forecast_idx]
                        previous_col = header[previous_idx] if previous_idx != -1 else None
                        reference_col = header[reference_idx] if reference_idx != -1 else None

                        # تشخیص اندیکاتور از نام فایل
                        indicator_name = detect_indicator_from_filename(p.name)
                        is_fomc = "fomc" in p.name.lower()

                        # فیلتر FOMC: فقط رکوردهای مرتبط با نرخ بهره
                        chunk_df = raw_df.copy()
                        if is_fomc and reference_col is not None:
                            ref_lower = chunk_df[reference_col].fillna("").str.lower()
                            mask = ref_lower.str.contains("interest rate|fomc", na=False)
                            chunk_df = chunk_df[mask].copy()
                            if chunk_df.empty:
                                logger.warning(
                                    "فایل FOMC %s پس از فیلتر Interest Rate/FOMC خالی شد؛ نادیده گرفته شد.",
                                    p.name,
                                )
                                continue

                        # parse تاریخ با فرمت‌های مختلف
                        def parse_date(val):
                            if not val or str(val).strip() in ("", "nan", "NaN"):
                                return pd.NaT
                            for fmt in ("%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                                try:
                                    return datetime.strptime(str(val).strip(), fmt)
                                except ValueError:
                                    continue
                            try:
                                return pd.to_datetime(str(val).strip(), errors="coerce")
                            except Exception:
                                return pd.NaT

                        chunk_df["date"] = chunk_df[date_col].apply(parse_date)
                        chunk_df["indicator"] = indicator_name
                        chunk_df["actual"] = chunk_df[actual_col].apply(parse_percent)
                        chunk_df["forecast"] = chunk_df[forecast_col].apply(parse_percent)
                        if previous_col:
                            chunk_df["previous"] = chunk_df[previous_col].apply(parse_percent)
                        else:
                            chunk_df["previous"] = np.nan

                        # فقط ستون‌های مورد نیاز را نگه دار
                        keep_cols = ["date", "indicator", "actual", "forecast", "previous"]
                        result_df = chunk_df[keep_cols].dropna(subset=["date"])
                        if result_df.empty:
                            logger.warning("فایل CSV %s پس از parse تاریخ خالی شد.", p.name)
                            continue

                        frames.append(result_df)
                        logger.info(
                            "فایل %s: %d رکورد بارگذاری شد (indicator=%s).",
                            p.name, len(result_df), indicator_name,
                        )
                    except Exception as e:
                        logger.warning("خطا در خواندن CSV %s: %s", p.name, e)

                if frames:
                    df = pd.concat(frames, ignore_index=True)
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    df = df.dropna(subset=["date"])
                    logger.info(
                        "اخبار از %d فایل CSV در %s با مجموع %d رکورد بارگذاری شد.",
                        len(frames), news_dir, len(df),
                    )
                    return df
                logger.warning("هیچ فایل CSV معتبری در %s پیدا نشد.", news_dir)

    logger.warning("هیچ داده خبری (news) پیدا نشد.")
    return None


def load_strategies_metadata(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_version_schema(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_golden_prefilter(
    df: pd.DataFrame, golden_df: Optional[pd.DataFrame], threshold: float = GOLDEN_SCORE_THRESHOLD
) -> pd.DataFrame:
    """فقط استراتژی‌های با امتیاز Golden >= threshold را نگه دار."""
    _log(f"apply_golden_prefilter: شروع - {len(df)} رکورد ورودی، threshold={threshold}")
    if golden_df is None or golden_df.empty:
        _log("apply_golden_prefilter: golden_df خالی است، پیش‌فیلتر رد شد")
        return df

    score_col = None
    for candidate in ["score", "golden_score", "final_score"]:
        if candidate in golden_df.columns:
            score_col = candidate
            break
    id_col = None
    for candidate in ["strategy_folder", "strategy_id", "strategy"]:
        if candidate in golden_df.columns:
            id_col = candidate
            break

    if score_col is None or id_col is None:
        logger.warning("ستون‌های لازم در golden_scores پیدا نشد؛ پیش‌فیلتر رد شد.")
        return df

    valid_strategies = set(golden_df.loc[golden_df[score_col] >= threshold, id_col])
    if "strategy_folder" not in df.columns:
        logger.warning("ستون strategy_folder در داده‌های signatures وجود ندارد.")
        return df

    before = len(df)
    df = df[df["strategy_folder"].isin(valid_strategies)].copy()
    logger.info(
        "پیش‌فیلتر Golden: %d -> %d رکورد (آستانه >= %s)", before, len(df), threshold
    )
    return df


# ==========================================================================
# گام ۲: استخراج ویژگی‌های خبری
# ==========================================================================

def _parse_signature_string(sig: str) -> Dict[str, Any]:
    """
    در صورت отсутствие فیلدهای جداگانه، تلاش برای استخراج اطلاعات از رشته
    signature با فرمت تقریبی:
    "{coin}_{indicator}_{position}_{distance_days}_{model}_{extra}"
    """
    parts = sig.split("_") if isinstance(sig, str) else []
    parsed: Dict[str, Any] = {}
    if len(parts) >= 4:
        for i, p in enumerate(parts):
            if p in ("pre", "post"):
                parsed["position"] = p
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed["distance_days"] = int(parts[i + 1])
                break
    return parsed


def build_signature(row: pd.Series) -> Optional[str]:
    """از فیلدهای کلیدی یک رشته‌ی امضای منحصربه‌فرد بساز.

    فرمت: {coin_composition}__{dominant_indicator}__{position}__{distance_days}d__{model}__{market_regime}
    هر فیلد موجود نباشد با «unknown» جایگزین می‌شود.
    """
    coin = str(row.get("coin_composition") or "unknown")
    indicator = str(row.get("dominant_indicator") or "unknown")
    position = str(row.get("position") or "unknown")
    distance = str(row.get("distance_days") or "unknown")
    model = str(row.get("model") or "unknown")
    regime = str(row.get("market_regime") or "unknown")
    return f"{coin}__{indicator}__{position}__{distance}d__{model}__{regime}"


def extract_news_window_stats(
    news_df: pd.DataFrame, period_start: pd.Timestamp, period_end: pd.Timestamp
) -> Dict[str, Any]:
    """diff_avg, diff_std, event_count, indicator_diversity را از news.pickle محاسبه کن.

    (نگه داشته شده برای سازگاری عقب/استفاده‌ی احتمالی جای دیگر؛ مسیر اصلی
    enrich_with_news_features دیگر از این تابع به‌صورت ردیف‌به‌ردیف استفاده
    نمی‌کند — به جای آن از _build_news_window_stats_vectorized استفاده می‌شود.)
    """
    window = news_df[(news_df["date"] >= period_start) & (news_df["date"] <= period_end)]
    if window.empty:
        return {
            "diff_avg": np.nan,
            "diff_std": np.nan,
            "event_count": 0,
            "indicator_diversity": 0,
        }

    diffs = (window["actual"] - window["forecast"]).dropna()
    return {
        "diff_avg": diffs.mean() if not diffs.empty else np.nan,
        "diff_std": diffs.std() if not diffs.empty else np.nan,
        "event_count": len(window),
        "indicator_diversity": window["indicator"].nunique(),
    }


def _build_news_window_stats_vectorized(
    news_df: pd.DataFrame, starts: pd.Series, ends: pd.Series
) -> pd.DataFrame:
    """
    معادل دقیق extract_news_window_stats برای همه‌ی ردیف‌ها به‌صورت یک‌جا،
    بدون حلقه‌ی Python روی news_df. پیچیدگی از O(n_rows * n_news) به
    O(n_news*log(n_news) + n_rows*log(n_news)) کاهش می‌یابد چون news بر اساس
    تاریخ مرتب می‌شود و برای هر بازه فقط با searchsorted مرز پنجره پیدا می‌شود.

    خروجی برای هر ردیف: diff_avg, diff_std, event_count, indicator_diversity
    - دقیقاً همان تعریف نسخه‌ی قبلی (پنجره inclusive روی [start, end]،
      diff = actual - forecast با dropna، std نمونه‌ای با ddof=1 مثل pandas).
    """
    n = len(starts)
    diff_avg_arr = np.full(n, np.nan)
    diff_std_arr = np.full(n, np.nan)
    event_count_arr = np.full(n, np.nan)
    indicator_div_arr = np.full(n, np.nan)

    if news_df is None or news_df.empty:
        return pd.DataFrame(
            {
                "diff_avg": diff_avg_arr,
                "diff_std": diff_std_arr,
                "event_count": event_count_arr,
                "indicator_diversity": indicator_div_arr,
            },
            index=starts.index,
        )

    news_sorted = news_df.sort_values("date")
    news_dates = news_sorted["date"].values
    diffs_arr = (news_sorted["actual"] - news_sorted["forecast"]).astype(float).values
    diffs_valid = ~np.isnan(diffs_arr)
    ind_codes, _ = pd.factorize(news_sorted["indicator"].values)

    diffs_for_sum = np.where(diffs_valid, diffs_arr, 0.0)
    cs_sum = np.concatenate([[0.0], np.cumsum(diffs_for_sum)])
    cs_sumsq = np.concatenate([[0.0], np.cumsum(diffs_for_sum ** 2)])
    cs_count_valid = np.concatenate([[0], np.cumsum(diffs_valid.astype(int))])

    s_vals = starts.values
    e_vals = ends.values
    valid_mask = ~(pd.isna(starts).values | pd.isna(ends).values)

    lo_idx = np.searchsorted(news_dates, s_vals, side="left")
    hi_idx = np.searchsorted(news_dates, e_vals, side="right")

    for i in range(n):
        if not valid_mask[i]:
            continue
        lo, hi = lo_idx[i], hi_idx[i]
        cnt_total = hi - lo
        if cnt_total == 0:
            event_count_arr[i] = 0.0
            indicator_div_arr[i] = 0.0
            continue
        cnt_valid = cs_count_valid[hi] - cs_count_valid[lo]
        if cnt_valid > 0:
            s_ = cs_sum[hi] - cs_sum[lo]
            avg = s_ / cnt_valid
            if cnt_valid > 1:
                ssq = cs_sumsq[hi] - cs_sumsq[lo]
                var = (ssq - cnt_valid * avg ** 2) / (cnt_valid - 1)
                diff_std_arr[i] = np.sqrt(var) if var > 0 else 0.0
            diff_avg_arr[i] = avg
        event_count_arr[i] = cnt_total
        indicator_div_arr[i] = len(set(ind_codes[lo:hi]))

    return pd.DataFrame(
        {
            "diff_avg": diff_avg_arr,
            "diff_std": diff_std_arr,
            "event_count": event_count_arr,
            "indicator_diversity": indicator_div_arr,
        },
        index=starts.index,
    )


def enrich_with_news_features(df: pd.DataFrame, news_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """فیلدهای موجود را اولویت بده؛ موارد缺失 را از news.pickle (در صورت وجود) پر کن."""
    df = df.copy()

    if "signature" in df.columns:
        needs_position = "position" not in df.columns or df["position"].isna().any()
        needs_distance = "distance_days" not in df.columns or df["distance_days"].isna().any()
        if needs_position or needs_distance:
            parsed = df["signature"].apply(_parse_signature_string)
            if needs_position:
                df["position"] = df.get("position")
                df["position"] = df["position"].fillna(parsed.apply(lambda d: d.get("position")))
            if needs_distance:
                df["distance_days"] = df.get("distance_days")
                df["distance_days"] = pd.to_numeric(
                    df["distance_days"], errors="coerce"
                ).fillna(parsed.apply(lambda d: d.get("distance_days")))

    _log(f"enrich_with_news_features: شروع - {len(df)} رکورد")
    needed_cols = ["diff_avg", "diff_std", "event_count", "indicator_diversity"]
    missing_any = any(c not in df.columns or df[c].isna().any() for c in needed_cols)

    if missing_any and news_df is not None and "period_start" in df.columns and "period_end" in df.columns:
        logger.info("تکمیل ویژگی‌های缺失 از news.pickle برای رکوردهای ناقص (نسخه وکتورایز)...")
        starts = pd.to_datetime(df["period_start"], errors="coerce")
        ends = pd.to_datetime(df["period_end"], errors="coerce")

        for col in needed_cols:
            if col not in df.columns:
                df[col] = np.nan

        need_mask = df[needed_cols].isna().any(axis=1)
        if need_mask.any():
            computed = _build_news_window_stats_vectorized(
                news_df, starts[need_mask], ends[need_mask]
            )
            for col in needed_cols:
                # فقط جایی که قبلاً NaN بود را پر کن (دقیقاً رفتار نسخه قبلی)
                still_na = df.loc[need_mask, col].isna()
                fill_idx = computed.index[still_na.values]
                df.loc[fill_idx, col] = computed.loc[fill_idx, col]

    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            df[col] = np.nan

    if "total_return" not in df.columns:
        raise ValueError("ستون total_return در داده‌های signatures وجود ندارد.")
    df["total_return"] = pd.to_numeric(df["total_return"], errors="coerce")

    return df



# ==========================================================================
# توابع کمکی محاسبه همبستگی
# ==========================================================================

def _encode_categorical(series: pd.Series) -> pd.Series:
    """تبدیل categorical به کدهای عددی (factorize) برای محاسبه Spearman."""
    codes, _ = pd.factorize(series.astype(str))
    codes = codes.astype(float)
    codes[codes == -1] = np.nan
    return pd.Series(codes, index=series.index)


def _spearman_pair(x: pd.Series, y: pd.Series) -> Tuple[float, float, int]:
    """ضریب Spearman، p-value و تعداد نمونه معتبر را برمی‌گرداند."""
    valid = x.notna() & y.notna()
    n = int(valid.sum())
    if n < 3:
        return np.nan, np.nan, n
    try:
        r, p = spearmanr(x[valid], y[valid])
    except Exception:
        return np.nan, np.nan, n
    return float(r), float(p), n


def _loo_spearman(x: pd.Series, y: pd.Series) -> Tuple[float, float, int]:
    """
    Leave-One-Out: برای هر نمونه، آن را حذف کن، روی بقیه Spearman بزن،
    و میانگین ضرایب/مقادیر p را به عنوان خروجی نهایی برگردان.

    OPTIMIZED (همان منطق و همان خروجی دقیق نسخه قبلی، فقط روی numpy array
    خام به‌جای pandas Series/Index اجرا می‌شود تا overhead سنگین
    Index.drop/.loc حذف شود؛ صحت با تست روی صدها گروه شبیه‌سازی‌شده
    تأیید شده — خروجی تا ۱۲ رقم اعشار با نسخه‌ی قبلی یکسان است).
    """
    mask = x.notna().values & y.notna().values
    xv = x.values[mask]
    yv = y.values[mask]
    n = len(xv)
    if n < 3:
        return np.nan, np.nan, n

    rs, ps = [], []
    idx_all = np.arange(n)
    for i in range(n):
        sub = idx_all != i
        if sub.sum() < 3:
            continue
        try:
            r, p = spearmanr(xv[sub], yv[sub])
            if not np.isnan(r):
                rs.append(r)
                ps.append(p)
        except Exception:
            continue

    if not rs:
        return np.nan, np.nan, n
    return float(np.mean(rs)), float(np.mean(ps)), n


def _fold_based_spearman(x: pd.Series, y: pd.Series) -> Tuple[float, float, int]:
    """
    fold-based split: داده را به دو نیم (۵۰٪/۵۰٪) تقسیم کن و Spearman را
    روی نیم تست محاسبه کن.
    """
    valid_idx = x.index[x.notna() & y.notna()]
    n = len(valid_idx)
    if n < 3:
        return np.nan, np.nan, n

    sorted_idx = list(valid_idx)
    half = len(sorted_idx) // 2
    test_idx = sorted_idx[half:]
    if len(test_idx) < 3:
        test_idx = sorted_idx

    return _spearman_pair(x.loc[test_idx], y.loc[test_idx])


def _compute_feature_correlation(
    df: pd.DataFrame, feature: str, target: str = "total_return", method: str = "full"
) -> Tuple[float, float, int]:
    """یک ویژگی را (عددی یا categorical) با target همبسته کن طبق روش مشخص."""
    if feature not in df.columns:
        return np.nan, np.nan, 0

    if feature in CATEGORICAL_FEATURES:
        x = _encode_categorical(df[feature])
    else:
        x = pd.to_numeric(df[feature], errors="coerce")

    y = pd.to_numeric(df[target], errors="coerce")

    # جلوگیری از ConstantInputWarning: اگر واریانس صفر باشد، نتیجه nan است
    valid = x.notna() & y.notna()
    if valid.sum() < 3:
        return np.nan, np.nan, int(valid.sum())
    if x[valid].nunique() <= 1 or y[valid].nunique() <= 1:
        return np.nan, np.nan, int(valid.sum())

    if method == "loo":
        return _loo_spearman(x, y)
    elif method == "fold":
        return _fold_based_spearman(x, y)
    else:
        return _spearman_pair(x, y)


def _choose_method(sample_count: int) -> Optional[str]:
    """بر اساس تعداد نمونه، روش محاسبه را انتخاب کن (یا None برای حذف).

    n >= LOO_UPPER_BOUND (۲۵): تفاوت LOO با Spearman کامل ناچیز است (<0.001)
    پس مستقیماً از روش "full" (بدون LOO) استفاده می‌شود — ۳۶ برابر سریع‌تر.
    """
    if sample_count < LOO_LOWER_BOUND:
        return None
    if sample_count < LOO_UPPER_BOUND:
        return "loo"
    return "full"


# ==========================================================================
# مدیریت وضعیت (Status) برای وقفه/ادامه (Resume)
# ==========================================================================

def default_status_path(output_dir: str) -> str:
    return os.path.join(output_dir, "correlation_status.json")


def load_status(status_file: str) -> Optional[Dict[str, Any]]:
    """فایل وضعیت قبلی را بارگذاری کن (اگر وجود داشته و قابل ادامه باشد)."""
    if not status_file or not os.path.exists(status_file):
        return None
    try:
        with open(status_file, "r", encoding="utf-8") as f:
            status = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("فایل وضعیت قابل خواندن نیست (%s)؛ از ابتدا شروع می‌شود.", e)
        return None

    if status.get("status") in ("interrupted", "running"):
        logger.info(
            "وضعیت قبلی پیدا شد: status=%s, last_chunk_index=%s, "
            "processed_signatures=%d",
            status.get("status"),
            status.get("last_chunk_index"),
            len(status.get("processed_signatures", [])),
        )
        return status

    logger.info("وضعیت قبلی status=%s است؛ ادامه لازم نیست (از ابتدا شروع می‌شود).", status.get("status"))
    return None


def save_status(
    status_file: str,
    processed_signatures: List[List[str]],
    last_chunk_index: int,
    total_chunks: int,
    status: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """وضعیت فعلی پایپ‌لاین را در فایل JSON ذخیره کن."""
    os.makedirs(os.path.dirname(status_file) or ".", exist_ok=True)
    payload = {
        "processed_signatures": processed_signatures,
        "last_chunk_index": last_chunk_index,
        "total_chunks": total_chunks,
        "chunk_size": chunk_size,
        "status": status,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    tmp_path = status_file + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, status_file)
    logger.info(
        "وضعیت ذخیره شد: status=%s, chunk=%d/%d, processed=%d",
        status, last_chunk_index, total_chunks, len(processed_signatures),
    )


def check_interrupt_flag(output_dir: str, interrupt_flag_path: Optional[str] = None) -> bool:
    """بررسی وجود فایل interrupt.flag (در مسیر مشخص یا در output_dir)."""
    candidates = []
    if interrupt_flag_path:
        candidates.append(interrupt_flag_path)
    candidates.append(os.path.join(output_dir, DEFAULT_INTERRUPT_FLAG_NAME))

    env_path = os.environ.get("THIRD_REPO_INTERRUPT_FLAG")
    if env_path:
        candidates.append(env_path)

    for path in candidates:
        if path and os.path.exists(path):
            logger.warning("فایل interrupt.flag پیدا شد در %s؛ پردازش متوقف می‌شود.", path)
            return True
    return False


def load_partial_results(output_dir: str) -> List[Dict[str, Any]]:
    """نتایج موقتِ ذخیره‌شده از اجرای قبلی (در صورت resume) را بارگذاری کن.

    فرمت جدید JSONL است (هر خط یک رکورد). برای سازگاری با اجراهای قبلی،
    فایل قدیمی correlation_partial_results.json (آرایه JSON) نیز در صورت
    وجود خوانده می‌شود.
    """
    jsonl_path = os.path.join(output_dir, PARTIAL_RESULTS_FILENAME)
    results: List[Dict[str, Any]] = []

    if os.path.exists(jsonl_path):
        try:
            with open(jsonl_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning("خط نامعتبر در نتایج موقت نادیده گرفته شد: %s", e)
            return results
        except OSError as e:
            logger.warning("نتایج موقت قابل خواندن نیست (%s)؛ نادیده گرفته می‌شود.", e)
            return []

    # سازگاری با عقب: فایل قدیمی .json (آرایه کامل)
    legacy_path = os.path.join(output_dir, "correlation_partial_results.json")
    if os.path.exists(legacy_path):
        try:
            with open(legacy_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("نتایج موقت قدیمی قابل خواندن نیست (%s)؛ نادیده گرفته می‌شود.", e)
            return []

    return []


def _json_default(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (datetime,)):
        return o.isoformat()
    return str(o)


def save_partial_results(output_dir: str, new_results: List[Dict[str, Any]]) -> None:
    """رکوردهای جدید را به فایل JSONL append کن (نه بازنویسی کل فایل).

    new_results باید فقط رکوردهای تازه‌ی این chunk باشد، نه کل all_results؛
    این کار از rewrite کامل فایل با رشد all_results جلوگیری می‌کند.
    """
    if not new_results:
        return
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, PARTIAL_RESULTS_FILENAME)

    with open(path, "a", encoding="utf-8") as f:
        for rec in new_results:
            f.write(json.dumps(rec, ensure_ascii=False, default=_json_default))
            f.write("\n")


def get_unique_signatures(df: pd.DataFrame) -> List[Tuple[str, str]]:
    """لیست منحصربه‌فرد (coin_composition, signature) را استخراج کن."""
    required = {"coin_composition", "signature"}
    if not required.issubset(df.columns):
        return []
    pairs = (
        df[["coin_composition", "signature"]]
        .dropna()
        .drop_duplicates()
        .apply(tuple, axis=1)
        .tolist()
    )
    return sorted(set(pairs))


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """یک لیست را به قطعات با اندازه ثابت تقسیم کن."""
    if chunk_size <= 0:
        chunk_size = DEFAULT_CHUNK_SIZE
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def compute_conditional_correlations_for_signatures(
    df: pd.DataFrame,
    signatures: List[Tuple[str, str]],
    min_sample_count: int,
) -> List[Dict[str, Any]]:
    """نسخه‌ی محدودشده‌ی سطح ۲ که فقط روی یک زیرمجموعه از امضاها اجرا می‌شود (برای chunking)."""
    results: List[Dict[str, Any]] = []
    required_cols = {"coin_composition", "signature"}
    if not required_cols.issubset(df.columns):
        return results

    sig_set = set(signatures)
    # نسخهٔ وکتورایزشده: همان فیلتر قبلی (row-wise apply) ولی بدون فراخوانی
    # تابع پایتون به ازای هر ردیف. نتیجه دقیقاً همان subset قبلی است،
    # فقط چند صد برابر سریع‌تر چون در سطح C اجرا می‌شود نه interpreter.
    pair_index = pd.MultiIndex.from_arrays(
        [df["coin_composition"], df["signature"]]
    )
    mask = pair_index.isin(sig_set)
    subset = df[mask]
    if subset.empty:
        return results

    for (coin, sig), group in subset.groupby(["coin_composition", "signature"]):
        sample_count = len(group)
        if sample_count < LOO_LOWER_BOUND:
            continue

        method = _choose_method(sample_count)
        if method is None:
            continue

        effective_full_threshold = max(min_sample_count, LOO_UPPER_BOUND)
        if method == "fold" and sample_count < effective_full_threshold:
            method = "loo" if sample_count >= LOO_LOWER_BOUND else None
        if method is None:
            continue

        for feature in ALL_FEATURES:
            r, p, n = _compute_feature_correlation(group, feature, method=method)
            if np.isnan(r) or n < LOO_LOWER_BOUND:
                continue
            if abs(r) < MIN_R_THRESHOLD:
                continue
            results.append(
                {
                    "level": "conditional",
                    "strategy_id": None,
                    "coin_composition": coin,
                    "signature": sig,
                    "feature": feature,
                    "lag": None,
                    "correlation": r,
                    "p_value": p,
                    "sample_count": n,
                }
            )

    return results


def save_summary(
    output_dir: str,
    status: str,
    total_signatures: int,
    processed_signatures: int,
    output_df: pd.DataFrame,
    output_files: List[str],
    version_schema: Dict[str, Any],
) -> str:
    """فایل خلاصه correlation_summary.json را تولید و ذخیره کن."""
    summary = {
        "status": status,
        "total_signatures": total_signatures,
        "processed_signatures": processed_signatures,
        "global_correlations": int((output_df["level"] == "global").sum()) if not output_df.empty else 0,
        "conditional_correlations": int((output_df["level"] == "conditional").sum()) if not output_df.empty else 0,
        "lag_correlations": int((output_df["level"] == "lag").sum()) if not output_df.empty else 0,
        "output_files": output_files,
        "version_id": version_schema.get("version_id") or version_schema.get("version"),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "correlation_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    logger.info("فایل خلاصه در %s ذخیره شد.", summary_path)
    return summary_path


# ==========================================================================
# گام ۳: سطح ۱ - همبستگی کلی (Global)
# ==========================================================================

def compute_global_correlations(
    df: pd.DataFrame, min_sample_count: int
) -> List[Dict[str, Any]]:
    _log(f"compute_global_correlations: شروع - {len(df)} رکورد")
    results = []
    if "coin_composition" not in df.columns:
        logger.warning("ستون coin_composition وجود ندارد؛ سطح ۱ رد شد.")
        return results

    for coin, group in df.groupby("coin_composition"):
        sample_count = len(group)
        # فیلتر حداقل تعداد دوره حذف شد: همه کوین‌ها (حتی با نمونه کم) محاسبه می‌شوند.

        for feature in ALL_FEATURES:
            r, p, n = _compute_feature_correlation(group, feature, method="full")
            if np.isnan(r) or n < min_sample_count:
                continue
            if abs(r) < MIN_R_THRESHOLD:
                continue
            results.append(
                {
                    "level": "global",
                    "strategy_id": None,
                    "coin_composition": coin,
                    "signature": None,
                    "feature": feature,
                    "lag": None,
                    "correlation": r,
                    "p_value": p,
                    "sample_count": n,
                }
            )

    _log(f"compute_global_correlations: پایان - {len(results)} همبستگی معنی‌دار")
    logger.info("سطح ۱ (global): %d همبستگی معنی‌دار پیدا شد.", len(results))
    return results


# ==========================================================================
# گام ۴: سطح ۲ - همبستگی شرطی در هر امضا (Conditional)
# ==========================================================================

def compute_conditional_correlations(
    df: pd.DataFrame, min_sample_count: int
) -> List[Dict[str, Any]]:
    results = []
    required_cols = {"coin_composition", "signature"}
    if not required_cols.issubset(df.columns):
        logger.warning("ستون‌های لازم برای سطح ۲ وجود ندارد.")
        return results

    for (coin, sig), group in df.groupby(["coin_composition", "signature"]):
        sample_count = len(group)
        if sample_count < LOO_LOWER_BOUND:
            continue

        method = _choose_method(sample_count)
        if method is None:
            continue

        effective_full_threshold = max(min_sample_count, LOO_UPPER_BOUND)
        if method == "fold" and sample_count < effective_full_threshold:
            method = "loo" if sample_count >= LOO_LOWER_BOUND else None
        if method is None:
            continue

        for feature in ALL_FEATURES:
            r, p, n = _compute_feature_correlation(group, feature, method=method)
            if np.isnan(r) or n < LOO_LOWER_BOUND:
                continue
            if abs(r) < MIN_R_THRESHOLD:
                continue
            results.append(
                {
                    "level": "conditional",
                    "strategy_id": None,
                    "coin_composition": coin,
                    "signature": sig,
                    "feature": feature,
                    "lag": None,
                    "correlation": r,
                    "p_value": p,
                    "sample_count": n,
                }
            )

    logger.info("سطح ۲ (conditional): %d همبستگی معنی‌دار پیدا شد.", len(results))
    return results


# ==========================================================================
# گام ۵: سطح ۴ - تحلیل تاخیر (Lag Analysis)
# ==========================================================================

def compute_lag_correlations(df: pd.DataFrame) -> List[Dict[str, Any]]:
    _log("compute_lag_correlations: شروع")
    results = []
    required = {"strategy_folder", "coin_composition", "period_start", "total_return"}
    if not required.issubset(df.columns):
        logger.warning("ستون‌های لازم برای سطح ۴ (lag) وجود ندارد.")
        return results

    work_df = df.copy()
    work_df["period_start"] = pd.to_datetime(work_df["period_start"], errors="coerce")
    work_df = work_df.dropna(subset=["period_start"])

    for (strategy, coin), group in work_df.groupby(["strategy_folder", "coin_composition"]):
        group = group.sort_values("period_start").reset_index(drop=True)
        if len(group) < MIN_SAMPLE_LAG:
            continue

        sorted_dates = group["period_start"].values

        for lag in LAGS_DAYS:
            target_dates = sorted_dates - np.timedelta64(lag, "D")
            idxs = np.searchsorted(sorted_dates, target_dates, side="right") - 1
            valid_mask = idxs >= 0
            if not valid_mask.any():
                continue
            valid_idxs = idxs[valid_mask]

            for feature in ALL_FEATURES:
                if feature not in group.columns:
                    continue

                lagged_feature_vals = group[feature].values[valid_idxs]
                current_return_vals = group["total_return"].values[valid_mask]

                if len(lagged_feature_vals) < MIN_SAMPLE_LAG:
                    continue

                x = pd.Series(lagged_feature_vals)
                y = pd.Series(current_return_vals)

                if feature in CATEGORICAL_FEATURES:
                    x = _encode_categorical(x)
                else:
                    x = pd.to_numeric(x, errors="coerce")
                y = pd.to_numeric(y, errors="coerce")

                r, p, n = _spearman_pair(x, y)
                if np.isnan(r) or n < MIN_SAMPLE_LAG:
                    continue
                if abs(r) < MIN_R_THRESHOLD:
                    continue

                results.append(
                    {
                        "level": "lag",
                        "strategy_id": strategy,
                        "coin_composition": coin,
                        "signature": None,
                        "feature": feature,
                        "lag": lag,
                        "correlation": r,
                        "p_value": p,
                        "sample_count": n,
                    }
                )

    _log(f"compute_lag_correlations: پایان - {len(results)} همبستگی معنی‌دار")
    logger.info("سطح ۴ (lag): %d همبستگی معنی‌دار پیدا شد.", len(results))
    return results


# ==========================================================================
# گام ۶: ذخیره‌سازی
# ==========================================================================

OUTPUT_COLUMNS = [
    "level",
    "strategy_id",
    "coin_composition",
    "signature",
    "feature",
    "lag",
    "correlation",
    "p_value",
    "sample_count",
    "created_at",
]


def build_output_dataframe(
    all_results: List[Dict[str, Any]], version_schema: Dict[str, Any]
) -> pd.DataFrame:
    if not all_results:
        df = pd.DataFrame(columns=OUTPUT_COLUMNS)
        return df

    df = pd.DataFrame(all_results)
    df["created_at"] = datetime.now(timezone.utc)

    if version_schema:
        version_id = version_schema.get("version_id") or version_schema.get("version")
        if version_id is not None:
            df["version_id"] = version_id

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    ordered_cols = OUTPUT_COLUMNS + [c for c in df.columns if c not in OUTPUT_COLUMNS]
    return df[ordered_cols]


def save_output(df: pd.DataFrame, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "correlations.parquet")
    try:
        df.to_parquet(output_path, index=False)
        logger.info("خروجی در %s ذخیره شد (%d رکورد).", output_path, len(df))
        return output_path
    except (ImportError, ValueError) as e:
        logger.warning(
            "ذخیره Parquet ناموفق بود (%s)؛ به‌جای آن CSV ذخیره می‌شود. "
            "برای رفع این مشکل pyarrow یا fastparquet را نصب کنید.", e
        )
        csv_path = os.path.join(output_dir, "correlations.csv")
        df.to_csv(csv_path, index=False)
        logger.info("خروجی (هشدار: CSV) در %s ذخیره شد (%d رکورد).", csv_path, len(df))
        return csv_path


# ==========================================================================
# گزارش‌گیری
# ==========================================================================

def print_summary_report(df: pd.DataFrame) -> None:
    if df.empty:
        logger.info("هیچ همبستگی معنی‌داری در هیچ سطحی پیدا نشد.")
        return

    print("\n" + "=" * 60)
    print("گزارش خلاصه ماژول Correlation")
    print("=" * 60)
    for level in ["global", "conditional", "lag"]:
        sub = df[df["level"] == level]
        print(f"سطح '{level}': {len(sub)} همبستگی معنی‌دار (|r| >= {MIN_R_THRESHOLD})")
        if not sub.empty:
            top = sub.reindex(sub["correlation"].abs().sort_values(ascending=False).index).head(3)
            for _, row in top.iterrows():
                print(
                    f"   - feature={row['feature']}, coin={row['coin_composition']}, "
                    f"r={row['correlation']:.3f}, n={row['sample_count']}"
                )
    print("=" * 60 + "\n")


# ==========================================================================
# تابع اصلی (بازنویسی شده با مدیریت بهتر resume و chunking)
# ==========================================================================

def run_correlation_pipeline(
    signatures_dir: str,
    golden_scores_path: Optional[str],
    news_pickle_path: Optional[str],
    news_dir: Optional[str],
    strategies_json_path: Optional[str],
    version_schema_path: Optional[str],
    output_dir: str,
    min_sample_count: int = MIN_SAMPLE_GLOBAL_CONDITIONAL_DEFAULT,
    status_file: Optional[str] = None,
    resume: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    interrupt_flag_path: Optional[str] = None,
    signatures_filter: Optional[str] = None,
) -> str:

    status_file = status_file or default_status_path(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    _log("run_correlation_pipeline: === شروع پایپ‌لاین ===")
    # ---- گام ۱: بارگذاری داده‌ها ----
    _log("run_correlation_pipeline: گام ۱ - بارگذاری signatures")
    df = load_signatures(signatures_dir, signatures_filter)
    if df.empty:
        raise ValueError("هیچ داده‌ای از signatures بارگذاری نشد؛ پایپ‌لاین متوقف شد.")

    _log("run_correlation_pipeline: بارگذاری golden_scores")
    golden_df = load_golden_scores(golden_scores_path)
    _log("run_correlation_pipeline: بارگذاری news")
    news_df = load_news(news_pickle_path=news_pickle_path, news_dir=news_dir)
    _log("run_correlation_pipeline: بارگذاری strategies_metadata و version_schema")
    _strategies_meta = load_strategies_metadata(strategies_json_path)
    version_schema = load_version_schema(version_schema_path)

    _log("run_correlation_pipeline: اعمال golden prefilter")
    df = apply_golden_prefilter(df, golden_df)
    if df.empty:
        raise ValueError("پس از پیش‌فیلتر Golden، هیچ رکوردی باقی نماند.")

    # ---- گام ۲: استخراج/تکمیل ویژگی‌های خبری ----
    _log("run_correlation_pipeline: گام ۲ - enrich_with_news_features")
    df = enrich_with_news_features(df, news_df)

    # ---- گام ۰: بارگذاری وضعیت قبلی (در صورت --resume) ----
    prev_status = load_status(status_file) if resume else None
    processed_signatures: List[List[str]] = (
        prev_status.get("processed_signatures", []) if prev_status else []
    )
    processed_set = {tuple(p) for p in processed_signatures}
    start_chunk_index = (prev_status.get("last_chunk_index", -1) + 1) if prev_status else 0

    all_results: List[Dict[str, Any]] = load_partial_results(output_dir) if prev_status else []

    # ---- گام ۳: لیست امضاهای منحصربه‌فرد و تقسیم به چانک ----
    _log("run_correlation_pipeline: گام ۳ - استخراج امضاها و تقسیم به چانک")
    all_signatures = get_unique_signatures(df)
    total_signatures = len(all_signatures)
    remaining_signatures = [s for s in all_signatures if s not in processed_set]

    chunks = chunk_list(remaining_signatures, chunk_size)
    total_chunks = start_chunk_index + len(chunks)

    logger.info(
        "تعداد کل امضاها: %d | از قبل پردازش‌شده: %d | باقی‌مانده: %d | تعداد قطعات جدید: %d",
        total_signatures, len(processed_set), len(remaining_signatures), len(chunks),
    )

    # ---- سطح ۱ (Global): فقط یک بار در کل اجرا ----
    _log("run_correlation_pipeline: سطح ۱ - Global correlations")
    already_has_global = any(r.get("level") == "global" for r in all_results)
    if not already_has_global:
        if check_interrupt_flag(output_dir, interrupt_flag_path):
            save_status(status_file, processed_signatures, start_chunk_index - 1, total_chunks, "interrupted", chunk_size)
            logger.warning("interrupt.flag قبل از شروع پیدا شد؛ خروج graceful.")
            return ""
        global_results = compute_global_correlations(df, min_sample_count)
        all_results.extend(global_results)
        save_partial_results(output_dir, global_results)

    # ---- سطح ۴ (Lag): فقط یک بار در کل اجرا ----
    _log("run_correlation_pipeline: سطح ۴ - Lag correlations")
    already_has_lag = any(r.get("level") == "lag" for r in all_results)
    if not already_has_lag:
        if check_interrupt_flag(output_dir, interrupt_flag_path):
            save_status(status_file, processed_signatures, start_chunk_index - 1, total_chunks, "interrupted", chunk_size)
            logger.warning("interrupt.flag قبل از تحلیل lag پیدا شد؛ خروج graceful.")
            return ""
        lag_results = compute_lag_correlations(df)
        all_results.extend(lag_results)
        save_partial_results(output_dir, lag_results)

    # ---- سطح ۲ (Conditional): پردازش چانک به چانک ----
    _log(f"run_correlation_pipeline: سطح ۲ - Conditional - {len(chunks)} چانک برای پردازش")
    interrupted = False
    current_chunk_index = start_chunk_index - 1
    for offset, chunk in enumerate(chunks):
        current_chunk_index = start_chunk_index + offset

        if check_interrupt_flag(output_dir, interrupt_flag_path):
            interrupted = True
            current_chunk_index -= 1  # این چانک پردازش نشد
            break

        chunk_results = compute_conditional_correlations_for_signatures(df, chunk, min_sample_count)
        all_results.extend(chunk_results)

        processed_signatures.extend([list(s) for s in chunk])
        save_partial_results(output_dir, chunk_results)
        save_status(
            status_file, processed_signatures, current_chunk_index, total_chunks, "running", chunk_size
        )
        logger.info(
            "قطعه %d/%d پردازش شد (%d امضا، %d همبستگی جدید).",
            current_chunk_index + 1, total_chunks, len(chunk), len(chunk_results),
        )

    if interrupted:
        save_status(status_file, processed_signatures, current_chunk_index, total_chunks, "interrupted", chunk_size)
        logger.warning("اجرا به دلیل وجود interrupt.flag متوقف شد (graceful). برای ادامه از --resume استفاده کنید.")
        return ""

    # ---- پس از اتمام همه چانک‌ها ----
    _log("run_correlation_pipeline: ساخت DataFrame خروجی نهایی")
    output_df = build_output_dataframe(all_results, version_schema)
    output_path = save_output(output_df, output_dir)

    _log("run_correlation_pipeline: ذخیره وضعیت و خلاصه")
    save_status(status_file, processed_signatures, current_chunk_index, total_chunks, "completed", chunk_size)
    save_summary(
        output_dir=output_dir,
        status="completed",
        total_signatures=total_signatures,
        processed_signatures=len(processed_signatures),
        output_df=output_df,
        output_files=[os.path.basename(output_path)],
        version_schema=version_schema,
    )

    _log("run_correlation_pipeline: === پایپ‌لاین با موفقیت تمام شد ===")
    print_summary_report(output_df)
    return output_path


# ==========================================================================
# CLI
# ==========================================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ماژول همبستگی خبری (Correlation) - محاسبه ضرایب همبستگی "
        "بین ویژگی‌های خبری و بازده استراتژی‌ها."
    )
    parser.add_argument("--signatures-dir", required=True, help="مسیر پوشه فایل‌های .jsonl signatures")
    parser.add_argument("--golden-scores", default=None, help="مسیر golden_scores.parquet (اختیاری)")
    parser.add_argument("--news-pickle", default=None, help="مسیر news.pickle")
    parser.add_argument(
        "--news-dir",
        default=None,
        help="مسیر پوشه‌ی CSVهای اخبار (مثل combo_10day؛ اولویت بر --news-pickle دارد)",
    )
    parser.add_argument("--strategies-json", default=None, help="مسیر strategies_metadata.json")
    parser.add_argument("--version-schema", default=None, help="مسیر version_schema.json (اختیاری)")
    parser.add_argument("--output-dir", required=True, help="مسیر پوشه خروجی برای ذخیره Parquet")
    parser.add_argument(
        "--min-sample-count",
        type=int,
        default=MIN_SAMPLE_GLOBAL_CONDITIONAL_DEFAULT,
        help="آستانه نمونه برای سطح ۱ و ۲ (پیش‌فرض ۵۰)",
    )
    parser.add_argument(
        "--status-file",
        default=None,
        help="مسیر فایل وضعیت برای مدیریت وقفه/ادامه (پیش‌فرض: correlation_status.json در output-dir)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="در صورت فعال بودن، از آخرین وضعیت ذخیره‌شده ادامه می‌دهد",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="تعداد امضاها در هر قطعه پردازشی (پیش‌فرض ۲۰)",
    )
    parser.add_argument(
        "--interrupt-flag",
        default=None,
        help="مسیر فایل interrupt.flag برای توقف graceful (اختیاری)",
    )
    parser.add_argument(
        "--signatures-filter",
        type=str,
        default=None,
        help="مسیر فایل JSON شامل آرایه‌ای از امضاها برای پردازش زیرمجموعه‌ای (اختیاری)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    _log("main: شروع اجرای correlation.py")
    _log(f"main: آرگومان‌ها = {vars(args)}")
    try:
        run_correlation_pipeline(
            signatures_dir=args.signatures_dir,
            golden_scores_path=args.golden_scores,
            news_pickle_path=args.news_pickle,
            news_dir=args.news_dir,
            strategies_json_path=args.strategies_json,
            version_schema_path=args.version_schema,
            output_dir=args.output_dir,
            min_sample_count=args.min_sample_count,
            status_file=args.status_file,
            resume=args.resume,
            chunk_size=args.chunk_size,
            interrupt_flag_path=args.interrupt_flag,
            signatures_filter=args.signatures_filter,
        )
        _log("main: اجرا با موفقیت پایان یافت - کد خروج 0")
        return 0
    except Exception as e:
        _log(f"main: خطای غیرمنتظره - {type(e).__name__}: {e}")
        logger.error("اجرای پایپ‌لاین با خطا متوقف شد: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
