#!/usr/bin/env python3
"""
portfolios.py - ماژول دوم: سبدهای مکمل (Complementary Portfolios)

این ماژول با استفاده از خروجی ماژول Golden (golden_scores.parquet) و داده‌های
خام per-period (signatures/*.jsonl)، ترکیب‌های بهینه ۲، ۳ و ۴ استراتژیِ هم‌گروه
(coin_composition, signature) را پیدا می‌کند: ترکیب‌هایی که بیشترین نرخ بقا
(Survival Rate) و جبران‌سازی متقابل (Compensation) و کمترین همبستگی شرطی را
دارند. خروجی نهایی در portfolios.csv ذخیره می‌شود.

نیازمندی‌ها:
    pip install pandas numpy scipy pyarrow

اجرا:
    python portfolios.py \
        --signatures-dir /tmp/signatures \
        --golden-scores /tmp/golden_scores.parquet \
        --version-schema /tmp/version_schema.json \
        --output-dir /tmp/portfolios_output \
        --top-n 15 \
        --status-file /tmp/portfolios_status.json \
        --resume
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("portfolios")

# -----------------------------------------------------------------------------
# ثابت‌ها
# -----------------------------------------------------------------------------
GOLDEN_SCORE_THRESHOLD = 45.0
MIN_PAIR_OVERLAP = 10
MIN_PORTFOLIO_SAMPLES = 10
CORR_PERCENTILE_THRESHOLD = 25
PORTFOLIO_SIZES = (2, 3, 4)
ABS_MIN_SURVIVAL_RATE = 50.0
ABS_MIN_COMPENSATION_RATIO = 0.6
ABS_MIN_AVG_RETURN = 0.0
SCORE_WEIGHTS = {
    "survival": 0.35,
    "compensation": 0.25,
    "correlation": 0.25,
    "return": 0.15,
}
DEFAULT_VERSION_ID = "v1.0.0"
DEFAULT_CHUNK_SIZE = 20

_POSITION_RE = re.compile(r"_(pre|post)_(\d+)_")

# -----------------------------------------------------------------------------
# بررسی کتابخانه‌ی Parquet (فقط برای خواندن ورودی‌های احتمالی مثل golden_scores.parquet)
# -----------------------------------------------------------------------------
try:
    import pyarrow  # noqa: F401
    _HAS_PARQUET = True
except ImportError:
    try:
        import fastparquet  # noqa: F401
        _HAS_PARQUET = True
    except ImportError:
        _HAS_PARQUET = False


def _save_dataframe(df: pd.DataFrame, path: Path) -> Path:
    """ذخیره DataFrame همیشه به‌صورت CSV (Parquet دیگر تولید نمی‌شود)."""
    out = path.with_suffix(".csv")
    df.to_csv(out, index=False)
    return out


def _read_parquet_or_csv(path: Path) -> pd.DataFrame:
    """خواندن فایل Parquet یا CSV."""
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    elif path.suffix == ".csv":
        return pd.read_csv(path)
    # تلاش با هر دو پسوند
    for suffix in (".parquet", ".csv"):
        candidate = path.with_suffix(suffix)
        if candidate.exists():
            return _read_parquet_or_csv(candidate)
    raise FileNotFoundError(f"فایل {path} پیدا نشد.")


# -----------------------------------------------------------------------------
# مدیریت وضعیت (Status Management)
# -----------------------------------------------------------------------------

def _default_status() -> dict:
    return {
        "processed_signatures": [],
        # ========== رفع باگ: مثل processed_files در golden.py، این فیلد هم‌سطح صف
        # (all_combinations_portfolios.json) است — رشته‌ی تخت signature، نه جفت
        # [coin_composition, signature]. processed_signatures فقط برای resume
        # داخلی است و نباید مستقیم در done_items.json/cleanup استفاده شود، چون
        # صف بر اساس رشته‌ی signature حذف می‌شود (signature از قبل coin_composition
        # را در خودش دارد، پس جفت غیرلازم است و با صف تطبیق پیدا نمی‌کند). ==========
        "processed_signature_strings": [],
        # ========== رفع باگ ناسازگاری فضای شناسه‌ها ==========
        # این فیلد، برخلاف processed_signature_strings، دقیقاً با فرمت
        # signature_path/path آیتم‌های صف (all_combinations_portfolios.json)
        # یکی است و باید در ورک‌فلو برای ساخت done_items.json استفاده شود.
        "processed_signature_paths": [],
        "last_chunk_index": -1,
        "total_chunks": 0,
        "chunk_size": DEFAULT_CHUNK_SIZE,
        "status": "running",
        "total_raw_portfolios": 0,  # ========== باگ ۷: شمارش کاندیدهای پیش از فیلتر مطلق ==========
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


def _expand_processed_paths(processed_set: set[tuple], group_queue_keys: dict[tuple, set[str]]) -> list[str]:
    """برای هر گروه (coin_composition, signature) پردازش‌شده، تمام queue_key های
    عضو آن را (که دقیقاً با فرمت signature_path صف مطابقت دارند) برمی‌گرداند."""
    paths: set[str] = set()
    for key in processed_set:
        paths.update(group_queue_keys.get(key, set()))
    return sorted(paths)


def load_status(status_file: Path) -> dict:
    """بارگذاری فایل وضعیت در صورت وجود، در غیر این صورت وضعیت پیش‌فرض."""
    if status_file.exists():
        try:
            with open(status_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            log.info("وضعیت قبلی بارگذاری شد از %s (آخرین chunk: %d)",
                     status_file, data.get("last_chunk_index", -1))
            return data
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("خطا در خواندن فایل وضعیت: %s — از ابتدا شروع می‌شود.", exc)
    return _default_status()


def save_status(status_file: Path, status: dict) -> None:
    """ذخیره وضعیت در فایل JSON."""
    status["last_updated"] = datetime.now(timezone.utc).isoformat()
    try:
        status_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = status_file.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False, indent=2)
        tmp.replace(status_file)
    except OSError as exc:
        log.error("خطا در ذخیره فایل وضعیت: %s", exc)


def check_interrupt_flag(output_dir: Path, interrupt_flag: Optional[Path] = None) -> bool:
    """بررسی وجود فایل interrupt.flag.

    اگر مسیر سفارشی interrupt_flag داده شده باشد (مثلاً runner.temp در CI)،
    آن نیز علاوه بر مسیرهای پیش‌فرض بررسی می‌شود.
    """
    candidates = [
        output_dir / "interrupt.flag",
        Path("interrupt.flag"),
    ]
    if interrupt_flag is not None:
        candidates.insert(0, Path(interrupt_flag))
    for p in candidates:
        if p.exists():
            log.warning("فایل interrupt.flag شناسایی شد: %s", p)
            return True
    return False


# -----------------------------------------------------------------------------
# گام ۱: بارگذاری داده‌ها
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# ساخت امضای خبری (signature)
# -----------------------------------------------------------------------------
# ========== رفع باگ: این تابع باید عیناً با build_signature در golden.py یکسان
# باشد. داده‌ی خام JSONL (خروجی combo_10day.py/combo_monthly.py) هیچ‌وقت ستون
# "signature" ندارد؛ golden.py آن را در لحظه‌ی بارگذاری می‌سازد (نه از فایل
# می‌خواند). اگر اینجا فرمول متفاوتی استفاده شود، merge با golden_scores.parquet
# در prefilter_candidates بی‌صدا صفر نتیجه می‌دهد (چون رشته‌های signature تطبیق
# پیدا نمی‌کنند) — پس این تابع باید کلمه‌به‌کلمه با نسخه‌ی golden.py یکی بماند. ==========

def build_signature(row: pd.Series) -> str:
    """ساخت امضای خبری از فیلدهای یک رکورد (باید عیناً مطابق golden.py باشد)."""
    regime = row.get("market_regime")
    if regime is None or (isinstance(regime, float) and pd.isna(regime)) or regime == "":
        regime = "unknown"
    coin = row.get("coin_composition", "")
    indicator = row.get("dominant_indicator", "")
    position = row.get("position")
    if position is None or (isinstance(position, float) and pd.isna(position)):
        position = "none"
    distance = row.get("distance_days")
    if distance is None or (isinstance(distance, float) and pd.isna(distance)):
        distance = 0
    model = row.get("model", "")
    # ========== رفع باگ اصلی (ریشه‌ی خالی ماندن portfolios): این تابع فیلد
    # "session" را نداشت، در حالی که golden.py ([فیکس ۹]) آن را به signature
    # اضافه کرده بود. نتیجه: signature ساخته‌شده اینجا همیشه یک segment کمتر
    # از signature داخل golden_scores.csv داشت و merge در prefilter_candidates
    # برای صددرصد رکوردها (نه فقط بعضی) بی‌صدا صفر می‌شد. این فرمول باید
    # کلمه‌به‌کلمه با golden.py یکی بماند. ==========
    session = row.get("session")
    if session is None or (isinstance(session, float) and pd.isna(session)) or session == "":
        session = "none"
    return f"{coin}_{indicator}_{position}_{distance}_{model}_{session}_{regime}"


def load_signatures(signatures_dir: Path, signatures_filter: Optional[Path] = None) -> pd.DataFrame:
    """تمام فایل‌های .jsonl را از دایرکتوری signatures بارگذاری و یکی می‌کند.

    در صورتی که signatures_filter داده شده باشد، فقط رکوردهایی که فیلد
    signature آنها در لیست موجود در فایل JSON فیلتر قرار دارد نگه داشته می‌شوند.
    """
    files = sorted(signatures_dir.rglob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"هیچ فایل .jsonl در {signatures_dir} پیدا نشد.")

    frames = []
    for fp in files:
        log.info("در حال خواندن %s", fp.name)
        try:
            df = pd.read_json(fp, lines=True)
        except ValueError as exc:
            log.warning("رد شدن از %s به دلیل خطای پارس JSON: %s", fp.name, exc)
            continue
        if df.empty:
            continue
        # ========== رفع باگ اصلی: عدم یکتایی basename ==========
        # قبلاً فقط fp.name (basename) ذخیره می‌شد، در حالی که آیتم‌های صف
        # (signature_path در build_all_queues.py) با مسیر نسبیِ کامل داخل
        # آرشیو شناسایی می‌شوند (مثلاً
        # "combo_10day/Best_15m/BTCUSDT/fixed_5d_simple_hybrid.jsonl").
        # چون اسم فایل‌ها بین پوشه‌های کوین/استراتژی مختلف تکراری‌ست (همان‌طور
        # که در لاگ دیده می‌شود: monthly_simple_hybrid.jsonl چندبار پشت‌سرهم
        # خوانده می‌شود)، ذخیره‌ی فقط basename باعث می‌شد تطبیق مستقیم
        # (data["__source_file"].isin(allowed_signatures)) هیچ‌وقت برقرار
        # نشود و کد همیشه به fallback غیریکتای __source_stem بیفتد که
        # ده‌ها/صدها queue_key واقعی را به یک کلید مشترک collapse می‌کرد و در
        # نتیجه‌ی group_queue_keys (که یک set است) همیشه فقط همان تعداد کم
        # مسیر یکتا برای حذف از صف باقی می‌ماند. با ذخیره‌ی مسیر نسبی کامل،
        # این تطبیق مستقیم و صحیح انجام می‌شود و هر فایل queue_key یکتای خودش
        # را می‌گیرد.
        # نکته: build_all_queues.py هنگام ساخت signature_path پیشوند "signatures/"
        # را از مسیر داخل tar حذف می‌کند (چون خودِ tar آن پوشه را دارد ولی صف
        # بدون آن ذخیره می‌شود). این‌جا هم باید همان پیشوند حذف شود، وگرنه
        # __source_file هیچ‌وقت با signature_path های صف برابر نمی‌شود و کد
        # کاملاً به fallback غیریکتای stem سقوط می‌کند.
        _rel = fp.relative_to(signatures_dir).as_posix()
        if _rel.startswith("signatures/"):
            _rel = _rel[len("signatures/"):]
        df["__source_file"] = _rel
        frames.append(df)

    if not frames:
        raise ValueError("هیچ رکورد معتبری در فایل‌های signatures پیدا نشد.")

    data = pd.concat(frames, ignore_index=True)

    # ========== رفع باگ: ستون "signature" هیچ‌وقت در JSONL خام وجود ندارد ==========
    # درست مثل golden.py، اینجا هم باید signature از روی فیلدهای خام ساخته شود؛
    # قبلاً این مرحله جا افتاده بود و کد فقط انتظار داشت ستون از قبل موجود باشد.
    base_signature_cols = {
        "coin_composition", "dominant_indicator", "position",
        "distance_days", "model", "market_regime",
    }
    missing_base = base_signature_cols - set(data.columns)
    if missing_base:
        raise ValueError(
            f"ستون‌های لازم برای ساخت signature یافت نشد: {missing_base}\n"
            f"ستون‌های موجود در داده: {sorted(data.columns.tolist())}"
        )
    data["signature"] = data.apply(build_signature, axis=1)

    # ========== رفع باگ: عدم تطبیق signature استراتژی‌های چند-کوینه ==========
    # golden.py (weighted_multi_coin_score) برای استراتژی‌هایی که روی چند کوین
    # اجرا شده‌اند، پیشوند کوین را از signature حذف می‌کند (base_sig) و آن را در
    # golden_scores.parquet ذخیره می‌کند. بنابراین صف portfolios (که از
    # golden_scores.parquet ساخته می‌شود) برای این استراتژی‌ها signature بدون
    # پیشوند کوین دارد. اما رکوردهای خام JSONL اینجا هرکدام مربوط به یک کوین
    # تکی هستند و signature ساخته‌شده همیشه پیشوند کوین را دارد — در نتیجه
    # هیچ‌وقت match نمی‌شدند و کل صف merged-شده صفر پردازش می‌شد.
    # راه‌حل: برای هر رکورد خام base_signature را هم (دقیقاً با فرمول golden.py)
    # حساب می‌کنیم تا با نسخه‌ی merged-شده در فیلتر تطبیق یابد.
    data["base_signature"] = data.apply(
        lambda r: str(r["signature"]).replace(f"{r.get('coin_composition', '')}_", "", 1),
        axis=1,
    )

    required_cols = {
        "coin_composition", "signature", "strategy_folder",
        "period_start", "period_end", "total_return",
    }
    missing = required_cols - set(data.columns)
    if missing:
        raise ValueError(
            f"ستون‌های ضروری در داده‌های signatures یافت نشد: {missing}\n"
            f"ستون‌های موجود در داده: {sorted(data.columns.tolist())}"
        )

    # ========== رفع باگ: strategy_folder خالی از combo_10day.py ==========
    # combo_10day.py در نسخه‌های قدیمی مقدار strategy_folder را همیشه ""
    # ثبت می‌کرد (همان باگی که در golden.py هم رفع شد). چون این تابع مستقل
    # از golden.py دوباره از روی jsonl خام strategy_id می‌سازد، اینجا هم باید
    # همان بازیابی از روی مسیر __source_file انجام شود، وگرنه صف portfolios
    # هم برای همان رکوردها strategy_id خالی خواهد داشت.
    if "strategy_folder" in data.columns and "__source_file" in data.columns:
        _empty_mask = data["strategy_folder"].isna() | (
            data["strategy_folder"].astype(str).str.strip() == ""
        )
        if _empty_mask.any():
            def _derive_strategy_folder(src):
                if not isinstance(src, str) or not src:
                    return ""
                parts = src.split("/")
                return parts[1] if len(parts) >= 2 else ""
            _derived = data.loc[_empty_mask, "__source_file"].apply(_derive_strategy_folder)
            data.loc[_empty_mask, "strategy_folder"] = _derived
            log.info(
                "[FIX] %d رکورد با strategy_folder خالی از روی __source_file بازیابی شد.",
                int(_empty_mask.sum()),
            )

    data["strategy_id"] = data["strategy_folder"].astype(str)
    data["period_start"] = pd.to_datetime(data["period_start"])
    data["period_end"] = pd.to_datetime(data["period_end"])
    data["total_return"] = pd.to_numeric(data["total_return"], errors="coerce")
    data = data.dropna(subset=["total_return"])

    if signatures_filter is not None and Path(signatures_filter).exists():
        with open(signatures_filter, "r", encoding="utf-8") as f:
            _filter_data = json.load(f)
        # filter.json ممکن است آرایه‌ای از dict (با کلید path) یا آرایه‌ای از string باشد
        # ========== باگ ۵ رفع شد: "path" باید با مسیر/نام فایل JSONL مقایسه شود نه data["signature"] ==========
        allowed_raw = []
        for item in _filter_data:
            if isinstance(item, dict):
                val = item.get("path") or item.get("signature") or item.get("signature_path")
            else:
                val = item
            if val:
                allowed_raw.append(str(val))

        # ========== رفع باگ اصلی: حذف fallback غیریکتای stem ==========
        # قبلاً یک شرط چهارم اضافه (`__source_stem.isin(allowed_stems)`) وجود
        # داشت که فقط basename فایل (بدون مسیر) را مقایسه می‌کرد. چون اسم
        # فایل‌های jsonl (مثل monthly_simple_hybrid.jsonl) بین صدها پوشه‌ی
        # کوین/دوره/آرشیو مختلف تکراری‌ست، این شرط عملاً هر رکوردی با یکی از
        # چند اسم پرتکرار را از هر آرشیوی قبول می‌کرد (نه فقط آرشیو/مسیر مجاز)
        # و باعث می‌شد mask میلیون‌ها رکورد اضافه را قبول کند و در نتیجه‌ی
        # _resolve_queue_key هم همه‌ی آن‌ها به همان تعداد کم stem یکتا نگاشت
        # شوند (دقیقاً همان چیزی که در لاگ دیده شد: فقط ۶ signature حذف شد).
        # این fallback عمداً حذف شده؛ حالا فقط تطبیق دقیق روی signature،
        # base_signature یا مسیر نسبی کامل فایل (__source_file، که دیگر با
        # همان فرمت signature_path در صف تولید می‌شود) انجام می‌شود.
        allowed_signatures: set[str] = set(allowed_raw)

        before = len(data)
        mask = (
            data["signature"].isin(allowed_signatures)
            | data["base_signature"].isin(allowed_signatures)
            | data["__source_file"].isin(allowed_signatures)
        )
        data = data[mask].copy()

        # ========== رفع باگ ناسازگاری فضای شناسه‌ها ==========
        # processed_signature_strings قبلاً همیشه از "signature" ساخته‌شده
        # (coin_indicator_position_...) پر می‌شد، در حالی که آیتم‌های صف
        # (all_combinations_portfolios.json) با "signature_path" (مسیر فایل
        # jsonl) شناسایی می‌شوند. این دو فرمت هیچ‌وقت با هم match نمی‌شدند و
        # cleanup صف عملاً کاری انجام نمی‌داد. اینجا برای هر رکورد، همان رشته‌ی
        # خامی که در فیلتر با آن match شده (queue_key) را نگه می‌داریم تا در
        # خروجی نهایی (processed_signature_paths) دقیقاً با فرمت صف یکی باشد.
        def _resolve_queue_key(row) -> str:
            if row["signature"] in allowed_signatures:
                return row["signature"]
            if row["base_signature"] in allowed_signatures:
                return row["base_signature"]
            if row["__source_file"] in allowed_signatures:
                return row["__source_file"]
            # نباید به اینجا برسیم چون mask بالا از قبل تطبیق را تضمین کرده،
            # ولی برای اطمینان مقدار signature را به‌عنوان fallback برمی‌گردانیم.
            return row["signature"]

        data["queue_key"] = data.apply(_resolve_queue_key, axis=1)
        log.info(
            "اعمال signatures-filter: %d -> %d رکورد (%d مورد مجاز).",
            before, len(data), len(allowed_raw),
        )

        # ========== ادامه رفع باگ چند-کوینه ==========
        # صف (و مرحله‌ی cleanup که بعداً بر اساس این رشته حذف می‌کند) روی
        # base_signature (بدون پیشوند کوین) برای استراتژی‌های merged-شده کار
        # می‌کند. اگر اینجا "signature" را روی مقدار خام (با پیشوند کوین) نگه
        # داریم، پردازش درست انجام می‌شود ولی گزارش processed_signature_strings
        # هیچ‌وقت با آیتم صف match نمی‌شود و دوباره چیزی از صف حذف نمی‌شود.
        # پس هر رکورد را با همان کلیدی که در فیلتر match شده (raw یا base)
        # برچسب می‌زنیم.
        data["signature"] = data.apply(
            lambda r: r["base_signature"]
            if r["base_signature"] in allowed_signatures
            else r["signature"],
            axis=1,
        )
    elif signatures_filter is not None:
        log.warning("فایل signatures-filter پیدا نشد: %s؛ همه‌ی داده‌ها پردازش می‌شوند.", signatures_filter)

    if "queue_key" not in data.columns:
        # بدون signatures_filter، شناسه‌ی صف در دسترس نیست؛ به‌عنوان fallback از
        # signature ساخته‌شده استفاده می‌کنیم (فقط برای گزارش‌دهی، نه cleanup صف).
        data["queue_key"] = data["signature"]

    log.info("مجموع رکوردهای signatures بارگذاری‌شده: %d", len(data))
    return data


def load_golden_scores(path: Path) -> pd.DataFrame:
    df = _read_parquet_or_csv(path)
    required_cols = {"strategy_id", "coin_composition", "signature", "score"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"ستون‌های ضروری در golden_scores یافت نشد: {missing}")
    df["strategy_id"] = df["strategy_id"].astype(str)
    return df


def load_strategies_metadata(path: Path | None) -> dict:
    """بارگذاری metadata استراتژی‌ها (اختیاری — این فایل در هیچ‌جا استفاده نمی‌شود)."""
    if path is None or not Path(path).exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return {str(item.get("folder")): item for item in raw}
    if isinstance(raw, dict):
        return raw
    return {}


def load_version_schema(path: Optional[Path]) -> str:
    if path is None or not path.exists():
        return DEFAULT_VERSION_ID
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key in ("version_id", "version", "id"):
            if key in raw:
                return str(raw[key])
        log.warning("کلید version_id در version_schema.json یافت نشد — استفاده از پیش‌فرض.")
        return DEFAULT_VERSION_ID
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("خطا در خواندن version_schema.json: %s — استفاده از پیش‌فرض.", exc)
        return DEFAULT_VERSION_ID


# -----------------------------------------------------------------------------
# گام ۲: پیش‌فیلتر استراتژی‌ها با Golden
# -----------------------------------------------------------------------------

def prefilter_candidates(signatures: pd.DataFrame, golden: pd.DataFrame) -> pd.DataFrame:
    """فقط استراتژی‌هایی با امتیاز Golden >= آستانه را نگه می‌دارد."""
    # ========== رفع باگ: coin_composition نباید در کلید join باشد ==========
    # golden.py (weighted_multi_coin_score) برای استراتژی‌های چند-کوینه،
    # coin_composition را به رشته‌ی ترکیبی مثل "BTC+ETH" تغییر می‌دهد، در حالی
    # که هر رکورد خام signatures فقط یک کوین تکی دارد (مثلاً فقط "BTC"). پس
    # join روی coin_composition برای همه‌ی استراتژی‌های چند-کوینه همیشه ۰
    # نتیجه می‌داد. signature (که در load_signatures برای این موارد به
    # base_signature نرمال شده) به همراه strategy_id برای match کافی است —
    # golden.py هم گروه‌بندی نهایی‌اش را روی (strategy_id, base_signature)
    # انجام می‌دهد، نه coin_composition.
    qualified = golden[golden["score"] >= GOLDEN_SCORE_THRESHOLD][
        ["strategy_id", "signature"]
    ].drop_duplicates()

    merged = signatures.merge(
        qualified,
        on=["strategy_id", "signature"],
        how="inner",
    )
    log.info(
        "پیش‌فیلتر Golden (score >= %s): %d/%d رکورد signatures واجد شرایط شدند",
        GOLDEN_SCORE_THRESHOLD, len(merged), len(signatures),
    )

    # ========== تشخیص موقت: صفر مطلق در merged یعنی یکی از دو نیمه‌ی کلید
    # (strategy_id یا signature) اصلاً match نمی‌خورد. این بلوک با join روی
    # هرکدام به‌تنهایی مشخص می‌کند کدام نیمه مقصر است، بدون نیاز به اجرای
    # جداگانه یا حدس زدن — لاگ زیر مستقیماً نمونه‌های عدم تطابق را نشان
    # می‌دهد. بعد از رفع باگ اصلی می‌توان این بلوک را حذف کرد. ==========
    if merged.empty and not signatures.empty and not qualified.empty:
        sig_strategy_ids = set(signatures["strategy_id"].unique())
        sig_signatures = set(signatures["signature"].unique())
        gold_strategy_ids = set(qualified["strategy_id"].unique())
        gold_signatures = set(qualified["signature"].unique())

        overlap_sid = sig_strategy_ids & gold_strategy_ids
        overlap_sig = sig_signatures & gold_signatures

        log.warning(
            "[DIAG] تطابق strategy_id به‌تنهایی: %d/%d (سمت signatures) | "
            "تطابق signature به‌تنهایی: %d/%d (سمت signatures)",
            len(overlap_sid), len(sig_strategy_ids),
            len(overlap_sig), len(sig_signatures),
        )
        log.warning(
            "[DIAG] نمونه strategy_id در signatures: %s",
            sorted(sig_strategy_ids)[:5],
        )
        log.warning(
            "[DIAG] نمونه strategy_id در golden (qualified): %s",
            sorted(gold_strategy_ids)[:5],
        )
        log.warning(
            "[DIAG] نمونه signature در signatures: %s",
            sorted(sig_signatures)[:3],
        )
        log.warning(
            "[DIAG] نمونه signature در golden (qualified): %s",
            sorted(gold_signatures)[:3],
        )

    return merged


# -----------------------------------------------------------------------------
# گام ۳: همبستگی شرطی — تشخیص اشتراک زمانی
# -----------------------------------------------------------------------------

def parse_position(signature: str) -> Optional[str]:
    """استخراج best-effort موقعیت 'pre'/'post' از رشته‌ی signature."""
    m = _POSITION_RE.search(signature)
    return m.group(1) if m else None


def compute_release_date(row: pd.Series) -> pd.Timestamp:
    """تخمین تاریخ انتشار شاخص غالب برای یک رکورد دوره.

    [فیکس ۱۴] قبلاً مسیر پیش‌فرض (بدون ستون release_date، که رایج‌ترین حالت
    است چون JSONLهای خام اصلاً چنین ستونی ندارند) مستقیماً period_end/
    period_start را برمی‌گرداند — که در JSONL یک رشته‌ی متنی است ("2020-01-01")
    نه pd.Timestamp، برخلاف امضای خود تابع. تا الان چون هیچ‌جا از این مقدار
    متد Timestamp-محور (مثل .date()) صدا زده نمی‌شد، این ناسازگاری خودش را
    نشان نمی‌داد.
    """
    if "release_date" in row and pd.notna(row["release_date"]):
        return pd.to_datetime(row["release_date"])

    position = row["position"] if "position" in row and pd.notna(row.get("position")) else None
    if position is None:
        position = parse_position(row["signature"])

    if position == "pre":
        return pd.to_datetime(row["period_end"])
    return pd.to_datetime(row["period_start"])


def build_release_dates(group: pd.DataFrame) -> pd.DataFrame:
    group = group.copy()
    group["release_date"] = group.apply(compute_release_date, axis=1)
    return group


def compute_correlation_matrix(group: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    محاسبه ماتریس همبستگی شرطی Spearman برای یک گروه (coin_composition, signature).

    خروجی:
        corr_df: ستون‌های [a, b, correlation, n]
        valid_periods: دیکشنری {strategy_id: set(release_date های معتبر)}
    """
    pivot = group.pivot_table(
        index="release_date",
        columns="strategy_id",
        values="total_return",
        aggfunc="mean",
    )

    valid_periods = {strat: set(pivot[strat].dropna().index) for strat in pivot.columns}

    strategies = list(pivot.columns)
    rows = []
    for a, b in itertools.combinations(strategies, 2):
        shared = valid_periods[a] & valid_periods[b]
        if len(shared) < MIN_PAIR_OVERLAP:
            continue
        shared_sorted = sorted(shared)
        series_a = pivot.loc[shared_sorted, a]
        series_b = pivot.loc[shared_sorted, b]
        if series_a.nunique() < 2 or series_b.nunique() < 2:
            continue
        corr, _ = spearmanr(series_a, series_b)
        if np.isnan(corr):
            continue
        rows.append({"a": a, "b": b, "correlation": float(corr), "n": len(shared)})

    corr_df = pd.DataFrame(rows, columns=["a", "b", "correlation", "n"])
    return corr_df, valid_periods


# -----------------------------------------------------------------------------
# گام ۴: تعیین آستانه همبستگی (داده‌محور) و فیلتر جفت‌ها
# -----------------------------------------------------------------------------

def filter_pairs_by_correlation(corr_df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """جفت‌هایی با همبستگی بیشتر از صدک ۲۵ام را حذف می‌کند."""
    if corr_df.empty:
        return corr_df, float("nan")
    threshold = float(np.percentile(corr_df["correlation"], CORR_PERCENTILE_THRESHOLD))
    kept = corr_df[corr_df["correlation"] <= threshold].copy()
    return kept, threshold


# -----------------------------------------------------------------------------
# گام ۷: معیارهای سبد
# -----------------------------------------------------------------------------

def compensation_ratio(returns: pd.DataFrame) -> float:
    """
    نرخ جبران‌سازی: مجموع سودهای جبران‌کننده / مجموع زیان‌های جبران‌شده.
    اگر مخرج ۰ باشد = ۱.

    ========== باگ ۳ رفع شد ==========
    دوره‌های کاملاً زیان‌ده (همه‌ی اعضا هم‌زمان ضرر کرده‌اند) نیز به‌عنوان
    زیان جبران‌نشده در مخرج لحاظ می‌شوند، نه فقط دوره‌های mixed.
    """
    gains, losses = 0.0, 0.0
    for _, row in returns.iterrows():
        losers = row[row < 0]
        gainers = row[row > 0]
        if len(losers) > 0 and len(gainers) > 0:
            # دوره‌ی mixed: زیان توسط سود برخی اعضا جبران شده
            losses += float(-losers.sum())
            gains += float(gainers.sum())
        elif len(losers) > 0 and len(gainers) == 0:
            # دوره‌ی کاملاً زیان‌ده: جبران‌سازی کاملاً شکست خورده — فقط در مخرج
            losses += float(-losers.sum())
    if losses == 0:
        return 1.0
    return gains / losses


def survival_rate(returns: pd.DataFrame) -> float:
    """
    درصد دوره‌هایی که سبد عملکرد مثبت داشته است.

    ========== باگ ۲ رفع شد ==========
    معیار مجموع (sum) به‌تنهایی به‌نفع سبدهای ۳عضوی سوگیری دارد (صرفاً به‌خاطر
    تعداد اعضای بیشتر، نه کیفیت ترکیب). برای عدالت در مقایسه‌ی سبدهای با اندازه‌های مختلف
    هم نرخ بقا بر اساس مجموع (قدرت تجمعی سبد) و هم بر اساس میانگین هر عضو
    (بی‌اثر از اندازه‌ی سبد) محاسبه و ترکیب می‌شوند.
    """
    if len(returns) == 0:
        return 0.0
    period_sums = returns.sum(axis=1)
    period_means = returns.mean(axis=1)
    sr_sum = float((period_sums > 0).sum()) / float(len(period_sums)) * 100.0
    sr_mean = float((period_means > 0).sum()) / float(len(period_means)) * 100.0
    return (sr_sum + sr_mean) / 2.0


def avg_return(returns: pd.DataFrame) -> float:
    """
    میانگین بازده سبد در دوره‌های معتبر.

    ========== باگ ۲ رفع شد ==========
    هم میانگینِ مجموع بازده (قدرت تجمعی سبد) و هم میانگینِ بازده هر عضو
    (عادلانه میان سبدهای با اندازه‌های متفاوت) محاسبه و ترکیب می‌شوند.
    """
    if len(returns) == 0:
        return 0.0
    period_sums = returns.sum(axis=1)
    period_means = returns.mean(axis=1)
    avg_sum = float(period_sums.mean())
    avg_mean = float(period_means.mean())
    return (avg_sum + avg_mean) / 2.0


def avg_correlation(members: tuple, corr_lookup: dict) -> float:
    """میانگین همبستگی جفتی بین اعضای سبد."""
    pairs = list(itertools.combinations(sorted(members), 2))
    vals = [corr_lookup[p] for p in pairs if p in corr_lookup]
    if not vals:
        return float("nan")
    return float(np.mean(vals))


# -----------------------------------------------------------------------------
# گام ۹: نرمال‌سازی Percentile Rank
# -----------------------------------------------------------------------------

def percentile_rank(series: pd.Series) -> pd.Series:
    """رتبه‌بندی صدکی بین ۰ تا ۱۰۰ (مقدار بزرگ‌تر => رتبه بالاتر)."""
    if len(series) <= 1:
        return pd.Series(100.0, index=series.index)
    return series.rank(pct=True) * 100.0


# -----------------------------------------------------------------------------
# گام‌های ۵-۹ روی یک گروه (coin_composition, signature)
# -----------------------------------------------------------------------------

def evaluate_group(
    coin_composition: str,
    signature: str,
    group: pd.DataFrame,
    top_n: int,
) -> tuple[list[dict], int]:
    """ارزیابی و رتبه‌بندی سبدهای ۲، ۳ و ۴ استراتژی برای یک گروه.

    خروجی: (لیست سبدهای برتر تا top_n، تعداد کل سبدهای کاندید بررسی‌شده پیش از فیلتر مطلق)
    """
    group = build_release_dates(group)

    corr_df, valid_periods = compute_correlation_matrix(group)
    if corr_df.empty:
        return [], 0

    kept_pairs, _threshold = filter_pairs_by_correlation(corr_df)
    if kept_pairs.empty:
        return [], 0

    corr_lookup = {(r.a, r.b): r.correlation for r in kept_pairs.itertuples()}
    candidate_strategies = sorted(set(kept_pairs["a"]) | set(kept_pairs["b"]))
    if len(candidate_strategies) < 2:
        return [], 0

    pivot = group.pivot_table(
        index="release_date", columns="strategy_id", values="total_return", aggfunc="mean"
    )

    # امضای واقعیِ هر (strategy_id, release_date): برای اینکه در خروجی هر سبد،
    # به‌جای برچسب گروه (که در حالت کل_بازه_زمانی می‌تواند شامل چند امضای
    # متفاوت باشد)، دقیقاً همان امضای رکورد(های) خامی که آن ترکیب مشخص از
    # اعضا/دوره‌ها را تشکیل داده‌اند نوشته شود.
    sig_lookup: dict[tuple, set] = (
        group.groupby(["strategy_id", "release_date"])["signature"]
        .apply(lambda s: set(s.dropna()))
        .to_dict()
    )

    # [فیکس ۱۳] برای هر دوره (release_date)، طول واقعی آن دوره (period_length_days)
    # را نگه می‌داریم تا بعداً بشود «تعداد_روز_فعال» یک سبد را (مجموع طول
    # دوره‌های مشترک اعضا) حساب کرد. اگر این ستون در داده موجود نباشد (نسخه‌ی
    # قدیمی JSONL)، به تعداد دوره‌ها (نه روز) بازمی‌گردیم و این محدودیت را
    # صریحاً مستند می‌کنیم.
    period_length_by_date = {}
    if "period_length_days" in group.columns:
        period_length_by_date = (
            group.groupby("release_date")["period_length_days"].max().to_dict()
        )

    portfolios = []
    raw_candidate_count = 0  # ========== باگ ۷ رفع شد: شمارش کاندیدها پیش از فیلتر مطلق ==========
    for size in PORTFOLIO_SIZES:
        for members in itertools.combinations(candidate_strategies, size):
            pairs = list(itertools.combinations(sorted(members), 2))
            if not all(p in corr_lookup for p in pairs):
                continue

            shared_periods = set.intersection(*(valid_periods[m] for m in members))
            if len(shared_periods) < MIN_PORTFOLIO_SAMPLES:
                continue

            returns = pivot.loc[sorted(shared_periods), list(members)]

            sr = survival_rate(returns)
            comp = compensation_ratio(returns)
            ar = avg_return(returns)
            ac = avg_correlation(members, corr_lookup)

            raw_candidate_count += 1

            # گام ۸: فیلتر مطلق قبل از رنکینگ
            if sr < ABS_MIN_SURVIVAL_RATE:
                continue
            if comp < ABS_MIN_COMPENSATION_RATIO:
                continue
            if ar < ABS_MIN_AVG_RETURN:
                continue

            # [فیکس ۱۳] بازه‌ی زمانی بک‌تست این سبد: اولین/آخرین دوره‌ی
            # مشترک اعضا، و مجموع طول واقعی دوره‌های مشترک (نه کل فاصله‌ی
            # تقویمی — چون بین دوره‌ها ممکن است شکاف باشد).
            sorted_shared = sorted(shared_periods)
            بازه_شروع = sorted_shared[0]
            بازه_پایان = sorted_shared[-1]
            if period_length_by_date:
                روز_فعال = int(sum(period_length_by_date.get(d, 0) for d in sorted_shared))
            else:
                # داده‌ی قدیمی بدون period_length_days: به تعداد دوره (نه
                # روز) برمی‌گردیم — این تخمین کمینه است، نه دقیق.
                روز_فعال = len(sorted_shared)

            combo_signatures: set = set()
            for _d in sorted_shared:
                for _m in members:
                    combo_signatures |= sig_lookup.get((_m, _d), set())
            combo_signature = "|".join(sorted(combo_signatures)) if combo_signatures else signature

            portfolios.append({
                "coin_composition": coin_composition,
                "signature": combo_signature,
                "members": list(members),
                "survival_rate": sr,
                "compensation_ratio": comp,
                "avg_return": ar,
                "avg_correlation": ac,
                "sample_count": len(shared_periods),
                "بازه_زمانی_شروع": بازه_شروع.date().isoformat(),
                "بازه_زمانی_پایان": بازه_پایان.date().isoformat(),
                "تعداد_روز_فعال": روز_فعال,
                "تعداد_روز_کل_بازه": (بازه_پایان - بازه_شروع).days + 1,
            })

    if not portfolios:
        return [], raw_candidate_count

    pf_df = pd.DataFrame(portfolios)

    # گام ۹: نرمال‌سازی و امتیازدهی
    # ========== باگ ۴ رفع شد: survival_rate نیز با percentile_rank نرمال شود تا
    # هم‌مقیاس با سه مؤلفه‌ی دیگر باشد و وزن‌دهی واقعی با SCORE_WEIGHTS مطابقت داشته باشد ==========
    pf_df["survival_norm"] = percentile_rank(pf_df["survival_rate"])
    pf_df["comp_norm"] = percentile_rank(pf_df["compensation_ratio"])
    pf_df["return_norm"] = percentile_rank(pf_df["avg_return"])
    pf_df["corr_norm"] = percentile_rank(-pf_df["avg_correlation"])  # کمتر=بهتر

    pf_df["score"] = (
        SCORE_WEIGHTS["survival"] * pf_df["survival_norm"]
        + SCORE_WEIGHTS["compensation"] * pf_df["comp_norm"]
        + SCORE_WEIGHTS["correlation"] * pf_df["corr_norm"]
        + SCORE_WEIGHTS["return"] * pf_df["return_norm"]
    )

    # ========== باگ ۱ رفع شد: محدودیت top_n روی بهترین سبدها (بر اساس score) اعمال می‌شود ==========
    pf_df = pf_df.sort_values("score", ascending=False)
    if top_n is not None and top_n > 0:
        pf_df = pf_df.head(top_n)
    pf_df = pf_df.drop(columns=["survival_norm", "comp_norm", "corr_norm", "return_norm"])

    return pf_df.to_dict("records"), raw_candidate_count


# -----------------------------------------------------------------------------
# خط‌لوله اصلی با پشتیبانی از chunk-based / resume / interrupt
# -----------------------------------------------------------------------------

def run(
    signatures_dir: Path,
    golden_scores_path: Optional[Path],
    strategies_json_path: Optional[Path],
    version_schema_path: Optional[Path],
    output_dir: Path,
    top_n: int,
    status_file: Path,
    resume: bool,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    signatures_filter: Optional[Path] = None,
    interrupt_flag: Optional[Path] = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    # مسیر پایه‌ی فایل موقت (بدون پسوند) — پسوند واقعی بسته به وجود pyarrow/fastparquet تعیین می‌شود
    temp_results_base = output_dir / "_portfolios_temp"
    temp_parquet_path = temp_results_base.with_suffix(".parquet")
    temp_csv_path = temp_results_base.with_suffix(".csv")

    # ========== باگ ۹ رفع شد ==========
    # در اجرای غیر-resume، فایل موقت باقی‌مانده از اجرای قبلی باید پاک شود تا با
    # داده‌های ران جدید (در صورت --resume بعدی) اشتباهاً ترکیب نشود.
    if not resume:
        for stale in (temp_parquet_path, temp_csv_path):
            if stale.exists():
                try:
                    stale.unlink()
                    log.info("فایل موقت قدیمی پاک شد: %s", stale)
                except OSError as exc:
                    log.warning("خطا در پاک‌کردن فایل موقت قدیمی %s: %s", stale, exc)

    # ---- گام ۰: بارگذاری وضعیت قبلی ----
    status = load_status(status_file) if resume else _default_status()
    processed_set: set[tuple] = {
        tuple(sig) if isinstance(sig, list) else sig
        for sig in status.get("processed_signatures", [])
    }
    total_raw_portfolios = int(status.get("total_raw_portfolios", 0)) if resume else 0

    if resume and processed_set:
        log.info("ادامه از آخرین وقفه — %d امضا قبلاً پردازش شده‌اند.", len(processed_set))

    # ---- گام ۱: بارگذاری داده‌ها ----
    signatures = load_signatures(signatures_dir, signatures_filter)
    _strategies_meta = load_strategies_metadata(strategies_json_path)
    version_id = load_version_schema(version_schema_path)

    # ---- گام ۲: پیش‌فیلتر (در صورت موجود بودن golden_scores) ----
    if golden_scores_path is not None:
        golden = load_golden_scores(golden_scores_path)
        candidates = prefilter_candidates(signatures, golden)
        if candidates.empty:
            log.warning(
                "هیچ استراتژی واجد شرایط (Golden score >= %s) یافت نشد.",
                GOLDEN_SCORE_THRESHOLD
            )
    else:
        log.warning(
            "golden_scores ارائه نشده — پیش‌فیلتر Golden رد می‌شود و همه‌ی "
            "%d رکورد signatures به‌عنوان candidate در نظر گرفته می‌شوند.",
            len(signatures)
        )
        candidates = signatures

    # ---- گام ۳: استخراج لیست امضاهای منحصربه‌فرد ----
    # ========== رفع باگ اصلی: صف هیچ‌وقت برای موارد زیر آستانه‌ی Golden خالی نمی‌شد ==========
    # قبلاً all_sig_keys و group_queue_keys از روی "candidates" (فقط رکوردهایی
    # که از پیش‌فیلتر Golden score >= GOLDEN_SCORE_THRESHOLD رد شده بودند)
    # ساخته می‌شدند. نتیجه: هر گروه (coin_composition, signature) که امتیاز
    # Golden‌اش به آستانه نمی‌رسید، از همان ابتدا از این دو ساختار کنار گذاشته
    # می‌شد؛ پس هیچ‌وقت وارد processed_set / group_queue_keys /
    # processed_signature_paths نمی‌شد و در مرحله‌ی cleanup صف هم match/حذف
    # نمی‌شد — صف برای این موارد برای همیشه ثابت می‌ماند، چون امتیاز Golden آن‌ها
    # بین اجراهای بعدی هم تغییر نمی‌کند.
    # راه‌حل: قلمرو گروه‌ها (all_sig_keys) و نگاشت queue_key (group_queue_keys)
    # باید از روی کل داده‌ی خام signatures ساخته شوند (هر چیزی که واقعاً در صف
    # بوده و بارگذاری شده)، نه فقط زیرمجموعه‌ی Golden-qualified. ارزیابی/ساخت
    # پرتفوی همچنان فقط روی candidates انجام می‌شود (چون groups_df پایین‌تر از
    # candidates ساخته می‌شود)؛ برای گروه‌هایی که در candidates نیستند (چون
    # Golden ردشان کرده)، شاخه‌ی KeyError موجود در حلقه‌ی پردازش (پایین‌تر)
    # آن‌ها را بدون ساخت پرتفوی مستقیماً processed علامت می‌زند — رفتار صحیح،
    # چون این گروه‌ها آگاهانه توسط Golden رد شده‌اند، نه اینکه هنوز در انتظارند.
    all_sig_keys: list[tuple[str, str]] = (
        signatures
        .groupby(["coin_composition", "signature"])
        .size()
        .reset_index()[["coin_composition", "signature"]]
        .apply(tuple, axis=1)
        .tolist()
    )
    log.info("تعداد کل گروه‌های (coin_composition, signature): %d", len(all_sig_keys))

    # هر گروه ممکن است از چند رکورد/فایل خام ساخته شده باشد که هرکدام queue_key
    # متفاوتی دارند؛ این نگاشت باید از "signatures" کامل ساخته شود (نه فقط
    # candidates) وگرنه queue_key های گروه‌های رد‌شده توسط Golden هیچ‌وقت
    # شناخته نمی‌شوند و حذف آن‌ها از صف در cleanup ممکن نخواهد بود.
    group_queue_keys: dict[tuple, set[str]] = (
        signatures
        .groupby(["coin_composition", "signature"])["queue_key"]
        .apply(lambda s: set(s.dropna().astype(str)))
        .to_dict()
    )

    # حذف امضاهای قبلاً پردازش‌شده در حالت resume
    pending_keys = [k for k in all_sig_keys if k not in processed_set]
    log.info("تعداد گروه‌های باقی‌مانده برای پردازش: %d", len(pending_keys))

    # ---- گام ۴: تقسیم به chunk ----
    chunks = [
        pending_keys[i: i + chunk_size]
        for i in range(0, len(pending_keys), chunk_size)
    ]
    total_chunks = len(chunks)  # تعداد chunk‌های باقی‌مانده در همین اجرا (برای پیشرفت لاگ)

    # ========== باگ ۶ رفع شد ==========
    # total_chunks کل (برای گزارش در status) باید مستقل از تعداد chunk‌های باقی‌مانده
    # محاسبه شود: از روی تعداد کل گروه‌های (coin_composition, signature) و chunk_size.
    total_chunks_overall = (
        math.ceil(len(all_sig_keys) / chunk_size) if chunk_size > 0 else 0
    )

    # start_chunk_index: اگر resume فعال باشد و قبلاً chunk‌هایی پردازش شده باشند
    start_chunk_index = 0
    if resume and status.get("last_chunk_index", -1) >= 0:
        # چون pending_keys قبلاً پردازش‌شده‌ها را حذف کرده، از ۰ شروع می‌کنیم
        start_chunk_index = 0

    status["total_chunks"] = total_chunks_overall
    status["chunk_size"] = chunk_size
    status["status"] = "running"
    save_status(status_file, status)

    # بارگذاری نتایج قبلی از فایل موقت اگر resume فعال است
    # ========== باگ ۸ رفع شد: هم .parquet و هم .csv بررسی می‌شوند ==========
    all_portfolios: list[dict] = []
    if resume and (temp_parquet_path.exists() or temp_csv_path.exists()):
        try:
            prev_df = _read_parquet_or_csv(temp_results_base)
            all_portfolios = prev_df.to_dict("records")
            log.info("نتایج قبلی بارگذاری شد: %d سبد", len(all_portfolios))
        except Exception as exc:
            log.warning("خطا در بارگذاری نتایج موقت قبلی: %s — از صفر شروع می‌شود.", exc)

    interrupted = False

    # ---- گام ۵: پردازش chunk به chunk ----
    groups_df = candidates.groupby(["coin_composition", "signature"])

    for chunk_idx, chunk in enumerate(chunks[start_chunk_index:], start=start_chunk_index):

        # بررسی interrupt.flag قبل از هر chunk
        if check_interrupt_flag(output_dir, interrupt_flag):
            log.warning("interrupt.flag شناسایی شد — ذخیره وضعیت و توقف.")
            status["status"] = "interrupted"
            interrupted = True
            # ذخیره نتایج موقت
            if all_portfolios:
                temp_df = pd.DataFrame(all_portfolios)
                _save_dataframe(temp_df, temp_results_base)
            save_status(status_file, status)
            break

        log.info("پردازش chunk %d/%d (%d امضا)", chunk_idx + 1, total_chunks, len(chunk))

        chunk_results: list[dict] = []

        for coin_composition, signature in chunk:
            key = (coin_composition, signature)
            try:
                group = groups_df.get_group(key)
            except KeyError:
                log.warning("گروه %s یافت نشد — رد شدن.", key)
                processed_set.add(key)
                continue

            if group["strategy_id"].nunique() < 2:
                processed_set.add(key)
                continue

            result, raw_count = evaluate_group(coin_composition, signature, group, top_n)
            # استخراج مستقیم شاخص خبری از خود داده (نه parse رشته‌ی signature) —
            # همه‌ی رکوردهای یک گروه (coin_composition, signature) طبق ساخت
            # build_signature همیشه یک dominant_indicator یکسان دارند.
            if result:
                ind_val = group["dominant_indicator"].iloc[0] if "dominant_indicator" in group.columns else ""
                ind_val = "" if pd.isna(ind_val) else str(ind_val)
                for rec in result:
                    rec["شاخص_خبری"] = ind_val
            chunk_results.extend(result)
            total_raw_portfolios += raw_count
            processed_set.add(key)

        all_portfolios.extend(chunk_results)

        # ---- به‌روزرسانی وضعیت پس از هر chunk ----
        status["last_chunk_index"] = chunk_idx
        status["processed_signatures"] = [list(k) for k in processed_set]
        # ========== رفع باگ: نسخه‌ی هم‌سطح صف (رشته‌ی تخت signature) برای done_items.json ==========
        status["processed_signature_strings"] = sorted({k[1] for k in processed_set})
        # ========== رفع باگ ناسازگاری فضای شناسه‌ها: این فیلد را ورک‌فلو برای
        # ساخت done_items.json/cleanup صف استفاده می‌کند چون دقیقاً با فرمت
        # signature_path آیتم‌های صف یکی است (برخلاف processed_signature_strings). ==========
        status["processed_signature_paths"] = _expand_processed_paths(processed_set, group_queue_keys)
        status["total_raw_portfolios"] = total_raw_portfolios
        status["status"] = "running"

        # ذخیره نتایج موقت
        if all_portfolios:
            temp_df = pd.DataFrame(all_portfolios)
            _save_dataframe(temp_df, temp_results_base)

        save_status(status_file, status)
        log.info(
            "chunk %d/%d کامل شد — %d سبد جدید / مجموع %d سبد / %d امضا پردازش‌شده",
            chunk_idx + 1, total_chunks, len(chunk_results),
            len(all_portfolios), len(processed_set),
        )

    if interrupted:
        log.info("اجرا به‌صورت graceful متوقف شد. برای ادامه از --resume استفاده کنید.")
        return output_dir / "portfolios.csv"

    # ---- گام ۶: پس از اتمام همه chunk‌ها ----
    # [فیکس] طبق درخواست کاربر، version_id و created_at از خروجی
    # portfolios.csv حذف شدند — این دو ستون صرفاً متادیتای اجرا بودن، نه
    # چیزی که برای تصمیم معاملاتی لازم باشه.
    columns = [
        "coin_composition", "signature", "شاخص_خبری", "members", "survival_rate",
        "compensation_ratio", "avg_return", "avg_correlation", "score",
        "sample_count",
        # [فیکس ۱۳] بازه‌ی زمانی بک‌تست این سبد
        "بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال", "تعداد_روز_کل_بازه",
    ]

    # ========== باگ ۷ رفع شد ==========
    # total_before_filter اکنون واقعاً تعداد سبدهای کاندید پیش از اعمال فیلترهای
    # مطلق (ABS_MIN_*) را نشان می‌دهد، نه تعداد بعد از فیلتر.
    total_before_filter = total_raw_portfolios
    total_after_filter = len(all_portfolios)

    if not all_portfolios:
        log.warning("هیچ سبدی شرایط لازم را احراز نکرد. فایل خروجی خالی ساخته می‌شود.")
        out_df = pd.DataFrame(columns=columns)
    else:
        out_df = pd.DataFrame(all_portfolios)
        out_df = out_df[[c for c in columns if c in out_df.columns]]

    output_path = output_dir / "portfolios"
    final_path = _save_dataframe(out_df, output_path)
    log.info("ذخیره شد: %s (%d سبد)", final_path, len(out_df))

    # پاک‌کردن فایل موقت
    for suffix in (".parquet", ".csv"):
        candidate = temp_results_base.with_suffix(suffix)
        if candidate.exists():
            try:
                candidate.unlink()
            except OSError:
                pass

    # ---- وضعیت نهایی ----
    status["status"] = "completed"
    status["processed_signatures"] = [list(k) for k in processed_set]
    status["processed_signature_strings"] = sorted({k[1] for k in processed_set})
    status["processed_signature_paths"] = _expand_processed_paths(processed_set, group_queue_keys)
    status["total_raw_portfolios"] = total_raw_portfolios
    save_status(status_file, status)

    # ---- تولید فایل خلاصه ----
    summary = {
        "status": "completed",
        "total_signatures": len(all_sig_keys),
        "processed_signatures": len(processed_set),
        "total_portfolios_before_filter": total_before_filter,
        "total_portfolios_after_filter": total_after_filter,
        "output_files": [str(final_path.name)],
        "version_id": version_id,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    summary_path = output_dir / "portfolios_summary.json"
    try:
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log.info("فایل خلاصه ذخیره شد: %s", summary_path)
    except OSError as exc:
        log.warning("خطا در ذخیره فایل خلاصه: %s", exc)

    return final_path


# -----------------------------------------------------------------------------
# حالت «کل بازه‌ی زمانی» (بدون قید رویداد خبری)
# -----------------------------------------------------------------------------

def run_whole_time(
    signatures_dir: Path,
    golden_scores_path: Optional[Path],
    strategies_json_path: Optional[Path],
    version_schema_path: Optional[Path],
    output_dir: Path,
    top_n: int,
    signatures_filter: Optional[Path] = None,
) -> Path:
    """
    نسخه‌ی «کل بازه‌ی زمانی»: برخلاف run() که سبدها را فقط درون یک شرط خبری
    خاص (coin_composition, signature) می‌سنجد، اینجا فرض می‌شود اعضای سبد
    مستقل از رویداد خبری، در کل بازه‌ی معاملاتی (از اولین تا آخرین رخداد
    موجود در داده) هم‌زمان بوده‌اند. گروه‌بندی فقط بر اساس coin_composition
    است (بدون فیلتر signature)، یعنی همه‌ی دوره‌های ثبت‌شده‌ی آن جفت‌کوین —
    مستقل از این‌که کدام شاخص خبری anchor آن‌ها بوده — با هم دیده می‌شوند.

    همان evaluate_group (همان فرمول‌های survival_rate/compensation_ratio/
    avg_return/avg_correlation/score) عیناً استفاده می‌شود؛ این‌طور تضمین
    می‌شود که همان محافظت در برابر «سود و ضرر خنثی‌کننده‌ی هم» که برای حالت
    خبری طراحی شده، اینجا هم برقرار است.

    برخلاف run()، این تابع chunk/resume/interrupt جداگانه ندارد — چون تعداد
    coin_composition ها معمولاً خیلی کمتر از تعداد کل (coin_composition,
    signature) است و نیازی به تقسیم نیست.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    signatures = load_signatures(signatures_dir, signatures_filter)
    version_id = load_version_schema(version_schema_path)

    if golden_scores_path is not None:
        golden = load_golden_scores(golden_scores_path)
        candidates = prefilter_candidates(signatures, golden)
    else:
        log.warning(
            "golden_scores ارائه نشده — پیش‌فیلتر Golden رد می‌شود و همه‌ی "
            "%d رکورد signatures به‌عنوان candidate در نظر گرفته می‌شوند.",
            len(signatures)
        )
        candidates = signatures

    if candidates.empty:
        log.warning("هیچ رکورد کاندیدی برای حالت کل بازه‌ی زمانی یافت نشد.")

    all_portfolios: list[dict] = []
    total_raw = 0
    coin_groups = candidates.groupby("coin_composition")
    coin_list = list(coin_groups.groups.keys())
    log.info("حالت کل بازه‌ی زمانی: %d ترکیب کوین یافت شد.", len(coin_list))

    for coin_composition in coin_list:
        group = coin_groups.get_group(coin_composition)
        if group["strategy_id"].nunique() < 2:
            continue
        result, raw_count = evaluate_group(coin_composition, "کل_بازه_زمانی", group, top_n)
        total_raw += raw_count
        all_portfolios.extend(result)
        log.info("  coin=%s | %d سبد یافت شد (از %d کاندید خام)",
                  coin_composition, len(result), raw_count)

    # [فیکس] طبق درخواست کاربر، version_id و created_at از خروجی
    # portfolios_whole_time.csv هم حذف شدند.
    columns = [
        "coin_composition", "signature", "members", "survival_rate",
        "compensation_ratio", "avg_return", "avg_correlation", "score",
        "sample_count",
        # [فیکس ۱۳] بازه‌ی زمانی بک‌تست این سبد
        "بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال", "تعداد_روز_کل_بازه",
    ]

    if not all_portfolios:
        log.warning("هیچ سبدی در حالت کل بازه‌ی زمانی شرایط لازم را احراز نکرد.")
        out_df = pd.DataFrame(columns=columns)
    else:
        out_df = pd.DataFrame(all_portfolios)
        out_df = out_df[[c for c in columns if c in out_df.columns]]

    output_path = output_dir / "portfolios_whole_time"
    final_path = _save_dataframe(out_df, output_path)
    log.info("ذخیره شد: %s (%d سبد، %d کاندید خام بررسی‌شده)",
              final_path, len(out_df), total_raw)
    return final_path


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ماژول سبدهای مکمل (Portfolios) — ساخت و امتیازدهی سبدهای ۲، ۳ و ۴ استراتژی",
    )
    parser.add_argument(
        "--signatures-dir", required=True, type=Path,
        help="مسیر پوشه‌ی فایل‌های .jsonl signatures",
    )
    parser.add_argument(
        "--golden-scores", required=False, type=Path, default=None,
        help="مسیر فایل golden_scores.parquet (اختیاری — اگر داده نشود، بدون "
             "پیش‌فیلتر Golden روی همه‌ی signatureها اجرا می‌شود)",
    )
    parser.add_argument(
        "--strategies-json", required=False, type=Path, default=None,
        help="(اختیاری، بی‌استفاده) مسیر فایل strategies_metadata.json",
    )
    parser.add_argument(
        "--version-schema", required=False, type=Path, default=None,
        help="مسیر فایل version_schema.json (اختیاری)",
    )
    parser.add_argument(
        "--output-dir", required=True, type=Path,
        help="مسیر پوشه‌ی خروجی برای ذخیره‌ی portfolios.parquet",
    )
    parser.add_argument(
        "--top-n", required=False, type=int, default=15,
        help="تعداد سبدهای برتر برای هر امضا (پیش‌فرض ۱۵)",
    )
    parser.add_argument(
        "--status-file", required=False, type=Path, default=None,
        help="مسیر فایل وضعیت برای مدیریت ادامه (پیش‌فرض: portfolios_status.json در output-dir)",
    )
    parser.add_argument(
        "--resume", action="store_true", default=False,
        help="ادامه از آخرین وضعیت ذخیره‌شده",
    )
    parser.add_argument(
        "--chunk-size", required=False, type=int, default=DEFAULT_CHUNK_SIZE,
        help=f"تعداد امضا در هر chunk (پیش‌فرض {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--signatures-filter", required=False, type=str, default=None,
        help="مسیر فایل JSON شامل آرایه‌ای از امضاها برای پردازش زیرمجموعه‌ای (اختیاری)",
    )
    parser.add_argument(
        "--interrupt-flag", required=False, type=Path, default=None,
        help="مسیر سفارشی فایل interrupt.flag برای بررسی وقفه (اختیاری، مثلاً runner.temp در CI)",
    )
    parser.add_argument(
        "--whole-time", action="store_true", default=False,
        help="به‌جای حالت مبتنی‌بر شرط خبری، سبدها را روی کل بازه‌ی زمانی "
             "(بدون قید رویداد خبری، فقط بر اساس coin_composition) می‌سازد. "
             "خروجی در فایل جدای portfolios_whole_time.* ذخیره می‌شود.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    status_file: Path = (
        args.status_file
        if args.status_file is not None
        else output_dir / "portfolios_status.json"
    )

    try:
        if args.whole_time:
            run_whole_time(
                signatures_dir=args.signatures_dir,
                golden_scores_path=args.golden_scores,
                strategies_json_path=args.strategies_json,
                version_schema_path=args.version_schema,
                output_dir=output_dir,
                top_n=args.top_n,
                signatures_filter=args.signatures_filter,
            )
        else:
            run(
                signatures_dir=args.signatures_dir,
                golden_scores_path=args.golden_scores,
                strategies_json_path=args.strategies_json,
                version_schema_path=args.version_schema,
                output_dir=output_dir,
                top_n=args.top_n,
                status_file=status_file,
                resume=args.resume,
                chunk_size=args.chunk_size,
                signatures_filter=args.signatures_filter,
                interrupt_flag=args.interrupt_flag,
            )
    except Exception:
        log.exception("اجرای ماژول Portfolios با خطا مواجه شد.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
