#!/usr/bin/env python3
"""
portfolios.py - ماژول دوم: سبدهای مکمل (Complementary Portfolios)

این ماژول با استفاده از خروجی ماژول Golden (golden_scores.parquet) و داده‌های
خام per-period (signatures/*.jsonl)، ترکیب‌های بهینه ۲، ۳ و ۴ استراتژی را پیدا
می‌کند: ترکیب‌هایی که بیشترین نرخ بقا (Survival Rate) و جبران‌سازی متقابل
(Compensation) و کمترین همبستگی را دارند. خروجی نهایی در portfolios.csv
ذخیره می‌شود.

========== رفع دو باگ طراحی اساسی (run() پیش‌فرض) ==========
باگ ۱ - قید نادرست «هم‌کوین و هم‌امضا»: قبلاً فقط استراتژی‌هایی که هم
    coin_composition و هم signature یکسان داشتند کنار هم در یک سبد قرار
    می‌گرفتند. این قید هیچ توجیه تجاری نداشت (فقط برای ساده‌سازی محاسبه‌ی
    همبستگیِ درون‌گروهی گذاشته شده بود) و دقیقاً برعکسِ هدف واقعیِ
    diversification عمل می‌کرد: دو استراتژی با کوین/شاخص خبری متفاوت اما
    همبستگیِ بازدهی پایین، بهترین کاندیدهای مکمل هم هستند. حالا run()
    استخر کاندیدهای Golden-qualified را یک‌جا (فارغ از کوین/امضا) بررسی
    می‌کند و فقط بر اساس همبستگیِ واقعی بازدهی ترکیب می‌سازد.
باگ ۲ - آینده‌نگری در تقاطع دوره‌ها (intersection): قبلاً همبستگی/
    جبران‌سازی/بقا فقط روی دوره‌هایی حساب می‌شد که *همه‌ی* اعضای سبد هم‌زمان
    در آن‌ها داده داشتند (تقاطع release_date دقیق). این هم آینده‌نگرانه بود
    (باید از قبل می‌دانستیم اعضا در چه تاریخ‌هایی هم‌زمان فعال خواهند بود)
    و هم عملاً هر ترکیبی از استراتژی‌های با بازه‌ی فعالیت غیرهم‌پوشان را رد
    می‌کرد. حالا معیار درست جایگزین شده: بازده‌ی هر استراتژی روی محور
    تقویمیِ ماهانه تجمیع می‌شود؛ همبستگی/جبران‌سازی/بقا روی همین سری زمانی
    ماهانه (نه تقاطع دوره‌ها) حساب می‌شوند و سبد در ماه‌هایی که یک عضو
    معامله نداشته، سهم آن عضو را صفر در نظر می‌گیرد (نه اینکه کل ماه را
    حذف کند). sample_count هم به «تعداد ماه‌هایی که حداقل یک عضو فعال بوده»
    تغییر کرد (نه تعداد دوره‌های مشترک).
توجه: بازه‌ی روزانه‌ی واقعیِ فعال‌بودن هر سبد (برای زمان‌بندی اجرای واقعی در
    حالت --timeline) همچنان از روی تاریخ‌های دقیق ساخته می‌شود؛ فقط این کار
    هم اکنون بر اساس Union اعضا انجام می‌شود، نه Intersection.

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
import ast
import itertools
import json
import logging
import math
import os
import re
import statistics
import resource
import sys
from collections import defaultdict
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


def _log_mem(stage: str) -> None:
    """[دیباگ کد ۱۴۳] لاگ حافظه‌ی peak این پروسس + حافظه‌ی سیستم، برای ردیابی
    OOM. چون سیگنال SIGTERM/SIGKILL می‌تواند وسط اجرا پروسس را بکشد (بدون
    traceback پایتون)، هر خط اینجا فوراً flush می‌شود تا حتی اگر لاگ بعدی
    هرگز چاپ نشود، لاگ‌های GitHub Actions تا همین نقطه ثبت شده باشند."""
    try:
        peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        peak_mb = -1
    avail_mb = total_mb = -1
    try:
        with open("/proc/meminfo") as f:
            info = {}
            for line in f:
                k, v = line.split(":", 1)
                info[k] = int(v.strip().split()[0]) / 1024  # kB -> MB
            total_mb = info.get("MemTotal", -1)
            avail_mb = info.get("MemAvailable", -1)
    except Exception:
        pass
    log.info(
        "[MEM] %s | peak_rss=%.0fMB | سیستم: available=%.0fMB / total=%.0fMB",
        stage, peak_mb, avail_mb, total_mb,
    )
    for h in log.handlers:
        try:
            h.flush()
        except Exception:
            pass
    sys.stdout.flush()

# -----------------------------------------------------------------------------
# ثابت‌ها
# -----------------------------------------------------------------------------
GOLDEN_SCORE_THRESHOLD = 45.0
MIN_PAIR_OVERLAP = 10
MIN_PORTFOLIO_SAMPLES = 10
CORR_PERCENTILE_THRESHOLD = 25
PORTFOLIO_SIZES = (2, 3, 4)
ABS_MIN_SURVIVAL_RATE = 70.0
ABS_MIN_AVG_RETURN = 0.5
# [فیکس درخواستی کاربر] فیلترهای بالا دیگر هیچ سبدی را رد نمی‌کنند؛ فقط برای
# تصمیم‌گیری بین دو حالت خروجی استفاده می‌شوند: اگر تعداد سبدهایی که همین
# آستانه‌ها را رعایت می‌کنند حداقل MIN_QUALIFIED_ROWS باشد، همان‌ها (با
# رتبه‌بندی score) برگردانده می‌شوند؛ در غیر این صورت (مثل حالتی که این
# آستانه‌ها اصلاً قابل‌دسترس نیستند)، به‌جای خروجی خالی، بهترین سبدهای
# *موجود* بر اساس نرخ بقا (survival_rate) تا سقف FALLBACK_TOP_N برگردانده
# می‌شوند.
MIN_QUALIFIED_ROWS = 100
FALLBACK_TOP_N = 300
SCORE_WEIGHTS = {
    "survival": 0.35,
    "compensation": 0.25,
    "correlation": 0.25,
    "return": 0.15,
}
DEFAULT_VERSION_ID = "v1.0.0"
DEFAULT_CHUNK_SIZE = 20

# ========== محافظ کارایی برای حذف قید هم‌گروهی (باگ طراحی ۱) ==========
# پس از حذف قید «فقط هم‌کوین و هم‌امضا»، تعداد کاندیدهایی که ممکن است هم‌زمان
# قابل‌ترکیب باشند می‌تواند خیلی بزرگ شود. برشمردن سبدهای ۳ و ۴عضوی حتی با
# پیمایش گراف (به‌جای itertools.combinations خام) روی چند هزار کاندید کند
# می‌شود. وقتی تعداد کاندیدهای پس از فیلتر همبستگی از این سقف بیشتر شود، فقط
# بهترین MAX_GLOBAL_CANDIDATES کاندید (بر اساس میانگین بازده‌ی ماهانه‌ی
# خودشان) نگه داشته می‌شوند. این صرفاً یک محافظ کارایی است و منطق همبستگی/
# جبران‌سازی را تغییر نمی‌دهد. برای غیرفعال‌کردن، None بگذارید.
MAX_GLOBAL_CANDIDATES: Optional[int] = 200

# -----------------------------------------------------------------------------
# ثابت‌های حالت «جدول زمانی پیوسته» (Timeline) — ماژول سوم
# -----------------------------------------------------------------------------
# ========== طراحی: چون score خروجی evaluate_group صدکی و *فقط درون همان گروه
# (coin_composition, signature)* است، بین گروه‌های مختلف قابل مقایسه نیست
# (این نکته قبلاً حین بررسی حالت "بهترین روزانه" کشف شد: چند سبد با score=100
# از گروه‌های کوچک برنده‌ی کاذب کل بازه می‌شدند). برای تصمیم‌گیری بین‌گروهی
# (ادغام دو سبد هم‌پوشان از دو گروه متفاوت، یا انتخاب بهترین پرکننده‌ی شکاف)
# یک quality_score جدید و سراسری تعریف می‌شود: صدک‌بندی avg_return/
# compensation_ratio/survival_rate/avg_correlation روی *کل استخر* کاندیدها
# (نه فقط هم‌گروهی‌ها)، دقیقاً با همان وزن‌هایی که قبلاً به تأیید کاربر رسید. ==========
QUALITY_WEIGHTS = {
    "return": 0.40,
    "compensation": 0.30,
    "survival": 0.20,
    "correlation": 0.10,
}
# حداکثر فاصله (روز) بین دو دوره‌ی واقعیِ یک سبد که هنوز «یک بازه‌ی پیوسته»
# حساب می‌شوند (برای ادغام دوره‌های چسبیده در _merge_intervals)
TIMELINE_ADJACENCY_DAYS = 1

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
    # ========== فیکس: تصادم signature بین anchorهای خبری متفاوت ==========
    # باید کلمه‌به‌کلمه با golden.py یکی بماند (همان‌طور که بالاتر مستند شده)،
    # وگرنه merge در prefilter_candidates دوباره بی‌صدا صفر می‌شود.
    # توضیح کامل فیکس در build_signature معادلِ golden.py آمده است.
    # ========== [پاک‌سازی آینده‌نگری] حذف dominant_indicator از امضا ==========
    # قبلاً یک قطعه‌ی جدا («indicator» از dominant_indicator) هم داخل امضا بود
    # — این فیلد از combo_10day.py/combo_monthly.py کاملاً حذف شده (مقایسه‌ی
    # آینده‌نگر بین شاخص‌ها بود)، پس این قطعه هم از امضا حذف شده — باید عیناً
    # مطابق golden.py بماند.
    indicator_key = row.get("indicator_key")
    if indicator_key is None or (isinstance(indicator_key, float) and pd.isna(indicator_key)) or indicator_key == "":
        indicator_key = "none"
    return f"{coin}_{position}_{distance}_{model}_{session}_{regime}_{indicator_key}"


# ========== [فیکس run_whole_time]: golden.py معادل «بدون رژیم» امضا را با
# _strip_market_regime می‌سازد (پسوند رژیم را از انتهای امضا حذف می‌کند تا
# coin_indicator_position_distance_model_session باقی بماند). این تابع باید
# عیناً همان لیست KNOWN_MARKET_REGIMES و همان منطق golden.py را داشته باشد،
# چون run_whole_time («کل بازه‌ی زمانی») در واقع یعنی «بدون در نظر گرفتن
# رژیم بازار» — نه «بدون در نظر گرفتن کل امضا». بقیه‌ی اجزای امضا (شاخص خبری،
# position، پنجره‌ی زمانی، مدل، سشن معاملاتی) باید همچنان ترکیب را منحصر‌به‌فرد
# نگه دارند، دقیقاً همان‌طور که در run() عادی هستند. ==========
KNOWN_MARKET_REGIMES = ["trending_up", "trending_down", "volatile", "ranging", "unknown"]


def strip_market_regime(signature) -> str:
    """معکوس ساخت رژیم در build_signature: امضا را بدون پسوند رژیم برمی‌گرداند
    (یعنی coin_indicator_position_distance_model_session). باید عیناً مطابق
    _strip_market_regime در golden.py باشد."""
    if not isinstance(signature, str) or not signature:
        return ""
    for regime in sorted(KNOWN_MARKET_REGIMES, key=len, reverse=True):
        suffix = "_" + regime
        if signature.endswith(suffix):
            return signature[: -len(suffix)]
        if signature == regime:
            return ""
    return signature


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
    # [پاک‌سازی آینده‌نگری] "dominant_indicator" از این لیست حذف شد — دیگر در
    # JSONL خام وجود ندارد (combo_10day.py/combo_monthly.py آن را کاملاً حذف
    # کرده‌اند)؛ build_signature هم دیگر از آن استفاده نمی‌کند.
    base_signature_cols = {
        "coin_composition", "position",
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

    # ═══════════════════════════════════════════════════════════════════
    # [فیکس زمان واقعی — باگ کشف‌شده در combo_10day.py] ⚠️
    # period_start/period_end/period_length_days خام فقط عدد نامزیِ برچسب
    # هستند (مثلاً «۵ روز قبل از FOMC»)؛ combo_10day.py هر معامله را به
    # نزدیک‌ترین رویدادِ خبری می‌چسباند بدون چک‌کردنِ اینکه تاریخ خودِ
    # معامله واقعاً داخل آن ۵ روز است یا نه — پس این ستون‌ها مستقیماً وارد
    # sweep-line و ساخت segmentهای portfolios_timeline.csv می‌شوند و
    # ستون‌های «شروع»/«پایان»/«روز» را هم غلط نشان می‌دهند. نسخه‌ی
    # اصلاح‌شده‌ی combo_10day.py حالا real_period_start/real_period_end/
    # real_period_length_days را هم ثبت می‌کند؛ اگر موجود باشند، همین‌جا
    # (یک‌بار، در نقطه‌ی بارگذاری) جایگزین نسخه‌ی نامزی می‌شوند تا تمام
    # منطق پایین‌دستی (evaluate_group، run_timeline، sweep-line) خودکار
    # روی بازه‌ی واقعی کار کند. برای JSONLهای قدیمی بدون این فیکس، رفتار
    # قبلی حفظ می‌شود.
    # ═══════════════════════════════════════════════════════════════════
    if "real_period_start" in data.columns:
        n_fixed = int(pd.to_datetime(data["real_period_start"], errors="coerce").notna().sum())
        real_start = pd.to_datetime(data["real_period_start"], errors="coerce")
        data["period_start"] = real_start.where(real_start.notna(), data["period_start"])
        log.info("[فیکس زمان واقعی] period_start برای %d رکورد با بازه‌ی واقعی جایگزین شد.", n_fixed)
    if "real_period_end" in data.columns:
        real_end = pd.to_datetime(data["real_period_end"], errors="coerce")
        data["period_end"] = real_end.where(real_end.notna(), data["period_end"])
    if "real_period_length_days" in data.columns and "period_length_days" in data.columns:
        real_len = pd.to_numeric(data["real_period_length_days"], errors="coerce")
        data["period_length_days"] = real_len.where(real_len.notna(), data["period_length_days"])

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

    # ========== فیکس معماری: واحد اتمیِ سبد باید (strategy_id, signature) ==========
    # قبلاً همه‌جای پایین‌دستی (همبستگی، ساخت کلیک/سبد، monthly returns) فقط
    # روی strategy_id گروه‌بندی می‌شد — یعنی اگر یک strategy_id چند signature
    # داشت (مثلاً همان استراتژی با فیلترهای زمانیِ مختلف: pre/post رویداد،
    # سشن، رژیم بازار، شاخص خبری)، بازده‌ی همه‌ی آن نسخه‌ها با هم جمع/قاطی
    # می‌شد و یک سری زمانیِ غیرقابل‌معامله تولید می‌کرد. چون signature عملاً
    # یک فیلتر معاملاتی مجزاست (و می‌تواند سودآوری را کاملاً عوض کند)، هر
    # (strategy_id, signature) باید یک «عضو» مستقل و قابل‌اجرا در نظر گرفته
    # شود، نه یک variant که در strategy_id خلاصه/قاطی شود. member_id همین
    # ترکیب یکتا را می‌سازد و از این‌جا به بعد در همبستگی/کلیک/monthly
    # returns/strategy_meta به‌جای strategy_id استفاده می‌شود.
    data["member_id"] = data["strategy_id"].astype(str) + "‖" + data["signature"].astype(str)

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


def build_monthly_returns(group: pd.DataFrame) -> pd.DataFrame:
    """تجمیع بازده هر عضو (member_id = strategy_id + signature) بر اساس ماه
    تقویمیِ release_date (نه دوره‌ی خام). این محور زمانیِ مشترک است که
    بازده‌ی استراتژی‌های با شاخص خبری/کوین متفاوت (که release_date دقیقشان
    هیچ‌وقت برابر نیست) را قابل‌مقایسه می‌کند — پایه‌ی رفع باگ ۲ (آینده‌نگری
    در تقاطع دوره‌ها).

    ========== فیکس معماری (member_id به‌جای strategy_id) ==========
    قبلاً اینجا روی strategy_id گروه‌بندی می‌شد. اگر یک strategy_id چند
    signature داشت (چند فیلتر زمانیِ مختلف روی همان استراتژی خام)، بازده‌ی
    همه‌شان در یک ماه با هم جمع می‌شد و یک سری زمانیِ ترکیبی/غیرقابل‌معامله
    می‌ساخت. حالا واحد گروه‌بندی member_id است — یعنی هر (strategy_id,
    signature) سری زمانیِ ماهانه‌ی مستقل خودش را دارد."""
    g = group.copy()
    g["__month"] = g["release_date"].dt.to_period("M")
    monthly = (
        g.groupby(["__month", "member_id"])["total_return"]
        .sum()
        .unstack("member_id")
        .sort_index()
    )
    return monthly


def compute_monthly_correlation_matrix(group: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    محاسبه ماتریس همبستگی Spearman روی بازده‌ی ماهانه‌ی تجمیعی هر عضو
    (member_id = strategy_id + signature).

    ========== رفع باگ ۲ (آینده‌نگری در تقاطع دوره‌ها) ==========
    قبلاً همبستگی فقط روی release_dateهایی حساب می‌شد که هر دو عضو *هم‌زمان*
    داده داشتند (تقاطع دقیق). این هم برای مقایسه‌ی بین شاخص‌های خبری متفاوت
    بی‌معنی بود (release_date دقیق آن‌ها اصلاً یکی نیست) و هم برای رسیدن به
    آن باید از قبل می‌دانستیم اعضا هم‌زمان کِی فعال خواهند بود. حالا بازده‌ی
    هر استراتژی روی محور تقویمیِ ماهانه تجمیع می‌شود و همبستگی روی این سری
    زمانیِ ماهانه (با حذف زوجیِ ماه‌های بدون داده‌ی هر دو طرف) حساب می‌شود —
    دقیقاً معیاری که برای «آیا این دو استراتژی در ماه‌های مختلف سود می‌دهند؟»
    باید استفاده شود.

    خروجی:
        corr_df: ستون‌های [a, b, correlation, n] — n = تعداد ماه‌هایی که هر دو
            عضو در آن‌ها فعال بوده‌اند (نه تعداد دوره‌ی خام مشترک). a/b اکنون
            مقادیر member_id هستند (strategy_id + signature)، نه strategy_id تنها.
        monthly: pivot ماهانه [ماه × member_id] از بازده‌ی تجمیعی هر ماه —
            برای استفاده‌ی مجدد در ساخت و ارزیابی سبد.
        exact_valid_periods: دیکشنری {member_id: set(release_date دقیق)} —
            فقط برای بازسازیِ بازه‌ی واقعیِ روزانه‌ی فعال‌بودن (زمان‌بندی اجرا)،
            نه برای محاسبه‌ی همبستگی/جبران‌سازی/بقا.
    """
    exact_valid_periods = {
        strat: set(sub["release_date"]) for strat, sub in group.groupby("member_id")
    }

    monthly = build_monthly_returns(group)
    strategies = list(monthly.columns)
    if len(strategies) < 2:
        return pd.DataFrame(columns=["a", "b", "correlation", "n"]), monthly, exact_valid_periods

    notna = monthly.notna()
    corr_matrix = monthly.corr(method="spearman")

    rows = []
    for a, b in itertools.combinations(strategies, 2):
        n = int((notna[a] & notna[b]).sum())
        if n < MIN_PAIR_OVERLAP:
            continue
        corr = corr_matrix.loc[a, b]
        if pd.isna(corr):
            continue
        rows.append({"a": a, "b": b, "correlation": float(corr), "n": n})

    corr_df = pd.DataFrame(rows, columns=["a", "b", "correlation", "n"])
    return corr_df, monthly, exact_valid_periods


def _build_adjacency(strategies: list[str], kept_pairs: pd.DataFrame) -> dict[str, set[str]]:
    """گراف مجاورت از جفت‌هایی که از فیلتر همبستگی رد شده‌اند (kept_pairs)."""
    adj: dict[str, set[str]] = {s: set() for s in strategies}
    for r in kept_pairs.itertuples():
        if r.a in adj and r.b in adj:
            adj[r.a].add(r.b)
            adj[r.b].add(r.a)
    return adj


def _enumerate_clique_members(
    strategies: list[str], adj: dict[str, set[str]], sizes: tuple[int, ...]
) -> dict[int, list[tuple]]:
    """تولید تمام clique های کاملاً متصلِ اندازه‌ی موردنیاز از گراف
    مجاورتِ همبستگی — دقیقاً همان شرط قبلی («همه‌ی جفت‌های داخل سبد باید از
    فیلتر همبستگی رد شده باشند») را نتیجه می‌دهد، اما با پیمایشِ اشتراک
    همسایه‌ها به‌جای شمارشِ کورِ itertools.combinations.

    ========== محافظ کارایی لازم برای رفع باگ ۱ (حذف قید هم‌گروهی) ==========
    وقتی کاندیدها دیگر به یک (coin_composition, signature) محدود نیستند،
    itertools.combinations(candidates, 4) روی چند صد کاندید عملاً
    غیرقابل‌اجراست. این تابع فقط زیرمجموعه‌هایی را تولید می‌کند که از قبل
    می‌دانیم همه‌ی جفت‌های داخلی‌شان معتبرند — نتیجه‌ی ریاضی یکسان، اجرای
    عملی امکان‌پذیر.
    """
    order = sorted(strategies)
    pos = {s: i for i, s in enumerate(order)}
    out: dict[int, list[tuple]] = {n: [] for n in sizes}
    need3 = 3 in sizes
    need4 = 4 in sizes
    for a in order:
        neigh_a = {n for n in adj.get(a, ()) if pos[n] > pos[a]}
        for b in sorted(neigh_a, key=lambda x: pos[x]):
            if 2 in sizes:
                out[2].append((a, b))
            if not (need3 or need4):
                continue
            common_ab = {n for n in neigh_a if n in adj.get(b, ()) and pos[n] > pos[b]}
            for c in sorted(common_ab, key=lambda x: pos[x]):
                if need3:
                    out[3].append((a, b, c))
                if not need4:
                    continue
                common_abc = {n for n in common_ab if n in adj.get(c, ()) and pos[n] > pos[c]}
                for d in sorted(common_abc, key=lambda x: pos[x]):
                    out[4].append((a, b, c, d))
    return out


# -----------------------------------------------------------------------------
# گام ۴: تعیین آستانه همبستگی (داده‌محور) و فیلتر جفت‌ها
# -----------------------------------------------------------------------------

def filter_pairs_by_correlation(corr_df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """
    ========== باگ ۷ رفع شد ==========
    قبلاً این تابع جفت‌هایی با همبستگی بالاتر از صدک ۲۵ام *همان گروهی که
    فراخوانی شده* را حذف می‌کرد. مشکل این بود که این آستانه مطلق نبود،
    نسبت‌به‌اندازه‌/ترکیبِ گروه بود: همان جفتِ استراتژی، بسته به این‌که
    evaluate_group با یک گروه کوچک (مثل run_timeline، یک کوین+امضا) صدا
    زده شده یا با کل استخر (run_whole_time)، ممکن بود یک‌بار قبول و یک‌بار
    رد شود — بدون هیچ ربطی به survival_rate/avg_return واقعی آن جفت. در
    عمل یعنی سبدهایی با نرخ بقای بالاتر می‌توانستند پیش از رسیدن به مرحله‌ی
    امتیازدهی، فقط به‌خاطر رتبه‌ی نسبیِ همبستگی‌شان در همان اجرا، کلاً حذف
    شوند و هیچ‌وقت دیده نشوند.

    حالا این فیلتر دیگر چیزی را حذف نمی‌کند — همه‌ی جفت‌های معتبر (با هر
    همبستگی) وارد مرحله‌ی ساخت سبد می‌شوند و انتخاب نهایی صرفاً بر اساس
    survival_rate/avg_return واقعی هر سبد انجام می‌شود (دقیقاً معیارهایی که
    هدف واقعی است)؛ همبستگی هم‌چنان محاسبه و در ستون avg_correlation خروجی
    گزارش می‌شود، فقط دیگر پیش از دیده‌شدن هیچ سبدی را رد نمی‌کند.
    """
    return corr_df.copy(), float("nan")


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


def _period_bounds_by_date(group: pd.DataFrame) -> dict:
    """برای هر release_date در گروه، بازه‌ی زمانی واقعی [حداقل period_start،
    حداکثر period_end] بین همه‌ی رکوردهای همان تاریخ را برمی‌گرداند — این
    بازه (نه فقط خود release_date که یک نقطه‌ی انکر است) پنجره‌ی واقعی
    فعال‌بودنِ آن دوره‌ی خاص روی محور تقویم است."""
    bounds = {}
    for date, sub in group.groupby("release_date"):
        bounds[date] = (sub["period_start"].min(), sub["period_end"].max())
    return bounds


def _merge_intervals(intervals: list[tuple]) -> list[tuple]:
    """بازه‌های زمانی هم‌پوشان یا چسبیده (فاصله <= TIMELINE_ADJACENCY_DAYS) را
    با هم ادغام می‌کند تا لیست نهایی بازه‌های غیرهم‌پوشانِ یک سبد به‌دست بیاید."""
    if not intervals:
        return []
    ivs = sorted(intervals, key=lambda x: x[0])
    merged = [list(ivs[0])]
    gap = pd.Timedelta(days=TIMELINE_ADJACENCY_DAYS)
    for start, end in ivs[1:]:
        last = merged[-1]
        if start <= last[1] + gap:
            if end > last[1]:
                last[1] = end
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


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

# =============================================================================
# [افزوده] ۱۶ ستون آماری ماهانه/افت‌سرمایه/ریسک‌به‌ریوارد — پورت مستقیم از
# calculators.py (همان تابعی که عیناً در golden.py هم پورت شده؛ کد اینجا
# تکرار شده تا portfolios.py مستقل بماند و به golden.py وابسته نشود).
# ورودی اینجا: (release_date, period_sum) — period_sum یعنی returns.sum(axis=1)
# همان دوره برای اعضای این سبد، دقیقاً همان تعریفی که survival_rate/
# compensation_ratio بالا هم برای «بازده‌ی سبد در آن دوره» استفاده می‌کنند.
# =============================================================================

def _period_monthly_stats(dated_values: list) -> dict:
    """dated_values: لیستی از (date, period_sum)."""
    months: dict = defaultdict(list)
    for d, v in dated_values:
        if d is None:
            continue
        key = f"{d.year:04d}-{d.month:02d}"
        months[key].append(v)

    empty = {
        "بازه_بکتست_ماه": 0, "میانگین_سود_ماهانه": 0.0, "انحراف_معیار_سود_ماهانه": 0.0,
        "بهترین_ماه_درصد": 0.0, "بدترین_ماه_درصد": 0.0, "درصد_ماه‌های_سودده": 0.0,
        "میانگین_سود_در_ماه‌های_سودده": 0.0, "میانگین_ضرر_در_ماه‌های_ضررده": 0.0,
        "بیشترین_ضرر_متوالی_ماهانه": 0, "میانگین_تعداد_دوره_در_ماه": 0.0,
        "انحراف_معیار_تعداد_دوره": 0.0, "حداکثر_افت_سرمایه_درصد": 0.0,
        "مدت_بازگشت_از_افت_ماه": 0, "ریسک_به_ریوارد_کلی": 0.0,
        "ریسک_به_ریوارد_ماه‌های_سوده": 0.0, "ریسک_به_ریوارد_ماه‌های_ضررده": 0.0,
        "حداکثر_ضرر_متوالی_درصد": 0.0,
    }
    if not months:
        return empty

    ordered_keys = sorted(months.keys())
    monthly_returns = [sum(months[k]) for k in ordered_keys]
    trades_per_month = [len(months[k]) for k in ordered_keys]
    n_months = len(ordered_keys)

    avg_ret = statistics.mean(monthly_returns)
    std_ret = statistics.stdev(monthly_returns) if n_months >= 2 else 0.0
    best = max(monthly_returns)
    worst = min(monthly_returns)
    profitable = [r for r in monthly_returns if r > 0]
    losing = [r for r in monthly_returns if r < 0]
    pct_profitable = (len(profitable) / n_months * 100) if n_months else 0.0
    avg_profit_months = statistics.mean(profitable) if profitable else 0.0
    avg_loss_months = statistics.mean(losing) if losing else 0.0

    best_streak = cur = 0
    for r in monthly_returns:
        if r < 0:
            cur += 1
            best_streak = max(best_streak, cur)
        else:
            cur = 0

    avg_trades = statistics.mean(trades_per_month) if trades_per_month else 0.0
    std_trades = statistics.stdev(trades_per_month) if len(trades_per_month) >= 2 else 0.0

    try:
        first_dt = datetime.strptime(ordered_keys[0] + "-01", "%Y-%m-%d")
        last_dt = datetime.strptime(ordered_keys[-1] + "-01", "%Y-%m-%d")
        total_span_months = (last_dt.year - first_dt.year) * 12 + (last_dt.month - first_dt.month) + 1
    except Exception:
        total_span_months = n_months

    cum = 0.0
    peak = 0.0
    mdd = 0.0
    cum_series = []
    for r in monthly_returns:
        cum += r
        cum_series.append(cum)
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > mdd:
            mdd = dd

    recovery_months = 0
    if mdd > 0:
        cur_peak = cum_series[0]
        cur_peak_i = 0
        worst_dd = 0.0
        worst_peak_i = 0
        worst_trough_i = 0
        for i, v in enumerate(cum_series):
            if v > cur_peak:
                cur_peak = v
                cur_peak_i = i
            dd = cur_peak - v
            if dd > worst_dd:
                worst_dd = dd
                worst_peak_i = cur_peak_i
                worst_trough_i = i
        target = cum_series[worst_peak_i]
        rec = None
        for i in range(worst_trough_i + 1, len(cum_series)):
            if cum_series[i] >= target:
                rec = i - worst_trough_i
                break
        recovery_months = rec if rec is not None else 0

    def _pl_ratio(vals):
        pos = [v for v in vals if v > 0]
        neg = [v for v in vals if v < 0]
        if not pos or not neg:
            return 0.0
        return statistics.mean(pos) / abs(statistics.mean(neg))

    all_vals = [v for _, v in dated_values]
    rr_overall = _pl_ratio(all_vals)

    # ========== باگ ۶ رفع شد ==========
    # قبلاً این‌جا مقادیر روزانه/دوره‌ای داخل یک «ماه سودده» فیلتر می‌شدند و
    # دوباره به مثبت/منفی تقسیم می‌شدند (_pl_ratio). چون داده‌ها تُنُک‌اند
    # (بیشتر دوره‌ها بازده صفر دارند)، داخل یک ماهِ از قبل برچسب‌خورده به
    # «سودده» عملاً هیچ مقدار منفی‌ای پیدا نمی‌شد (یا برعکس در ماه‌های
    # ضررده هیچ مقدار مثبتی) و در نتیجه این دو ستون همیشه دقیقاً 0.0
    # برمی‌گشتند — بدون استثنا، در کل خروجی. رفع آن با محاسبه‌ی این نسبت در
    # سطح «بازده‌ی تجمیعیِ هر ماه» (همان profitable/losing که برای
    # avg_profit_months/avg_loss_months بالا هم استفاده شده)، نه در سطح
    # دوره‌ی خام داخل هر ماه.
    rr_profit_months = (avg_profit_months / abs(avg_loss_months)) if (profitable and losing) else 0.0
    rr_loss_months = rr_profit_months

    ordered_vals = [v for _, v in sorted(dated_values, key=lambda x: (x[0] is None, x[0]))]
    best_count, best_sum = 0, 0.0
    cur_count, cur_sum = 0, 0.0
    for v in ordered_vals:
        if v < 0:
            cur_count += 1
            cur_sum += v
            if cur_sum < best_sum:
                best_sum = cur_sum
        else:
            cur_count = 0
            cur_sum = 0.0

    return {
        "بازه_بکتست_ماه": total_span_months,
        "میانگین_سود_ماهانه": avg_ret,
        "انحراف_معیار_سود_ماهانه": std_ret,
        "بهترین_ماه_درصد": best,
        "بدترین_ماه_درصد": worst,
        "درصد_ماه‌های_سودده": pct_profitable,
        "میانگین_سود_در_ماه‌های_سودده": avg_profit_months,
        "میانگین_ضرر_در_ماه‌های_ضررده": avg_loss_months,
        "بیشترین_ضرر_متوالی_ماهانه": best_streak,
        "میانگین_تعداد_دوره_در_ماه": avg_trades,
        "انحراف_معیار_تعداد_دوره": std_trades,
        "حداکثر_افت_سرمایه_درصد": mdd,
        "مدت_بازگشت_از_افت_ماه": recovery_months,
        "ریسک_به_ریوارد_کلی": rr_overall,
        "ریسک_به_ریوارد_ماه‌های_سوده": rr_profit_months,
        "ریسک_به_ریوارد_ماه‌های_ضررده": rr_loss_months,
        "حداکثر_ضرر_متوالی_درصد": best_sum,
    }


EXT_STATS_16_COLUMNS = [
    "ریسک_به_ریوارد_کلی", "ریسک_به_ریوارد_ماه‌های_سوده", "ریسک_به_ریوارد_ماه‌های_ضررده",
    "میانگین_سود_ماهانه", "انحراف_معیار_سود_ماهانه", "بهترین_ماه_درصد", "بدترین_ماه_درصد",
    "درصد_ماه‌های_سودده", "میانگین_سود_در_ماه‌های_سودده", "میانگین_ضرر_در_ماه‌های_ضررده",
    "بیشترین_ضرر_متوالی_ماهانه", "میانگین_تعداد_دوره_در_ماه", "انحراف_معیار_تعداد_دوره",
    "حداکثر_افت_سرمایه_درصد", "مدت_بازگشت_از_افت_ماه", "حداکثر_ضرر_متوالی_درصد",
]


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
    attach_periods: bool = False,
    abs_filters: bool = True,
) -> tuple[list[dict], int]:
    """ارزیابی و رتبه‌بندی سبدهای ۲، ۳ و ۴ استراتژی روی کاندیدهای موجود در
    `group` — `group` دیگر لزوماً یک (coin_composition, signature) واحد
    نیست؛ می‌تواند کل استخر Golden-qualified باشد (بنگرید run()) و
    strategy_idهای با کوین/امضای متفاوت هم می‌توانند در یک سبد کنار هم
    قرار بگیرند، به شرطی که همبستگیِ بازدهی‌شان (روی سری زمانیِ ماهانه)
    پایین باشد.

    coin_composition/signature: فقط وقتی معنادارند که `group` واقعاً به یک
    گروه همگن محدود شده باشد (مثل run_whole_time/run_timeline)؛ در غیر این
    صورت به‌عنوان برچسب سطح‌بالا خالی/عمومی می‌مانند و اطلاعات واقعیِ هر عضو
    در "member_coin_compositions"/"member_signatures" (به‌ازای هر عضو) برگردانده
    می‌شود.

    پارامترهای اضافه (فقط برای حالت Timeline استفاده می‌شوند؛ پیش‌فرض‌ها رفتار
    run()/run_whole_time() را تغییر نمی‌دهند):
        attach_periods: اگر True باشد، به هر سبد خروجی یک کلید داخلی
            "_intervals" (لیست بازه‌های زمانی واقعیِ غیرهم‌پوشانِ فعال‌بودن،
            بر اساس period_start/period_end خام) اضافه می‌شود.
        abs_filters: اگر False باشد، فیلتر مطلق (ABS_MIN_*) اعمال نمی‌شود و
            همه‌ی کاندیدها (نه فقط واجدشرایط‌ها) در خروجی نگه داشته می‌شوند؛
            در عوض هر رکورد یک کلید داخلی "_passes_abs" می‌گیرد که نتیجه‌ی
            همان فیلتر را (بدون حذف رکورد) نشان می‌دهد.

    خروجی: (لیست سبدهای برتر تا top_n، تعداد کل سبدهای کاندید بررسی‌شده پیش از فیلتر مطلق)
    """
    built = evaluate_group_build(group, attach_periods=attach_periods)
    if built is None:
        return [], 0
    (candidate_strategies, clique_members, monthly, corr_lookup, strategy_meta,
     period_length_by_date, exact_valid_periods, period_bounds) = built

    portfolios = []
    raw_candidate_count = 0
    for size in PORTFOLIO_SIZES:
        for members in clique_members.get(size, []):
            record = score_combo(
                members, monthly, corr_lookup, strategy_meta,
                period_length_by_date, exact_valid_periods,
                coin_composition, signature,
                attach_periods=attach_periods, period_bounds=period_bounds,
            )
            if record is None:
                continue
            raw_candidate_count += 1
            portfolios.append(record)

    return evaluate_group_finalize(portfolios, raw_candidate_count, top_n, abs_filters)


# ========== فیکس معماری (matrix-پذیر کردن whole-time): evaluate_group قبلاً
# یک تابع یک‌تکه بود که هم گراف/ترکیبات را می‌ساخت، هم هر ترکیب را
# امتیازدهی می‌کرد، هم نرمال‌سازی/انتخاب نهایی سراسری را انجام می‌داد.
# اکنون به سه بخش مجزا افراز شده: evaluate_group_build (باید یک‌جا و
# سراسری اجرا شود — نیازمند دیدن همه‌ی کاندیدها همزمان)، score_combo (کاملاً
# مستقل به‌ازای هر ترکیب — قابل توزیع بین matrix jobها) و
# evaluate_group_finalize (نرمال‌سازیِ percentile_rank/امتیاز/انتخاب
# قطعی‌یا-fallback — این هم ذاتاً سراسری است، چون percentile نسبت به کل
# مجموعه‌ی سبدهای یافت‌شده معنا دارد، نه یک زیرمجموعه‌ی chunk). evaluate_group
# اکنون فقط این سه را پشت سر هم صدا می‌زند؛ منطق محاسباتی هیچ‌کدام تغییر
# نکرده — فقط جابه‌جا شده — پس رفتار run()/run_timeline() که evaluate_group
# را صدا می‌زنند بدون تغییر می‌ماند. ==========

def evaluate_group_build(group: pd.DataFrame, attach_periods: bool = False):
    """بخش سراسریِ evaluate_group: باید یک‌جا اجرا شود چون تشخیصِ این‌که کدام
    جفت استراتژی اصلاً یک لبه‌ی معتبر (MIN_PAIR_OVERLAP) هستند و کدام
    استراتژی‌ها داخل سقف MAX_GLOBAL_CANDIDATES باقی می‌مانند، فقط با دیدن
    همه‌ی کاندیدها همزمان ممکن است. خروجی None یعنی هیچ ترکیبی قابل‌ساخت
    نیست (دقیقاً معادل موارد قبلی که evaluate_group مستقیماً ([], 0)
    برمی‌گرداند)."""
    group = build_release_dates(group)
    period_bounds = _period_bounds_by_date(group) if attach_periods else {}

    corr_df, monthly, exact_valid_periods = compute_monthly_correlation_matrix(group)
    if corr_df.empty:
        return None

    kept_pairs, _threshold = filter_pairs_by_correlation(corr_df)
    if kept_pairs.empty:
        return None

    corr_lookup = {(r.a, r.b): r.correlation for r in kept_pairs.itertuples()}
    candidate_strategies = sorted(set(kept_pairs["a"]) | set(kept_pairs["b"]))
    if len(candidate_strategies) < 2:
        return None

    # ========== محافظ کارایی برای رفع باگ ۱ (حذف قید هم‌گروهی) ==========
    #
    # ========== باگ ۸ رفع شد ==========
    # قبلاً این محدودسازی فقط بر پایه‌ی یک معیار بود: میانگین بازده ماهانه‌ی
    # هر strategy_id به‌تنهایی. یعنی استراتژی‌هایی با نرخ بقای فردی بسیار
    # بالا ولی بازده متوسط، همیشه اولین قربانی‌های این cutoff بودند — حتی
    # اگر هدف اصلی (طبق درخواست کاربر) پیدا کردن بالاترین survival_rate هم
    # باشد. حالا این cutoff دو دسته را همیشه تضمین‌شده نگه می‌دارد: ۱۰ تای
    # برتر بر اساس نرخ بقای فردی + ۱۰ تای برتر بر اساس بازده فردی؛ باقی
    # ظرفیت (تا سقف MAX_GLOBAL_CANDIDATES) طبق روال قبلی با بازده پر می‌شود.
    # توجه: این هنوز تضمین نمی‌کند بهترین *ترکیب* (سبد) لزوماً از استراتژی‌های
    # با معیار فردیِ بالا ساخته می‌شود — دو عضو با معیار فردیِ متوسط اما
    # هم‌بستگیِ خیلی پایین می‌توانند سبدی برتر از هرکدام به‌تنهایی بسازند؛
    # این محدودیت ذاتیِ هر پیش‌فیلترِ مبتنی‌بر-معیارِ-فردی است، نه چیزی که
    # صرفاً با افزودن یک معیار دوم کامل رفع شود.
    TOP_GUARANTEED_PER_METRIC = 10
    if MAX_GLOBAL_CANDIDATES and len(candidate_strategies) > MAX_GLOBAL_CANDIDATES:
        indiv_return = monthly[candidate_strategies].mean(skipna=True)
        indiv_survival = (monthly[candidate_strategies] > 0).mean(skipna=True) * 100.0

        top_survival = set(
            indiv_survival.sort_values(ascending=False).head(TOP_GUARANTEED_PER_METRIC).index
        )
        top_return = set(
            indiv_return.sort_values(ascending=False).head(TOP_GUARANTEED_PER_METRIC).index
        )
        guaranteed = top_survival | top_return

        remaining_slots = max(MAX_GLOBAL_CANDIDATES - len(guaranteed), 0)
        fill_pool = indiv_return.drop(index=guaranteed, errors="ignore")
        fill = set(fill_pool.sort_values(ascending=False).head(remaining_slots).index)

        candidate_strategies = sorted(guaranteed | fill)
        cs_set = set(candidate_strategies)
        kept_pairs = kept_pairs[kept_pairs["a"].isin(cs_set) & kept_pairs["b"].isin(cs_set)]
        corr_lookup = {(r.a, r.b): r.correlation for r in kept_pairs.itertuples()}
        if len(candidate_strategies) < 2 or kept_pairs.empty:
            return None

    adj = _build_adjacency(candidate_strategies, kept_pairs)
    clique_members = _enumerate_clique_members(candidate_strategies, adj, PORTFOLIO_SIZES)

    # اطلاعات هر عضو (کوین/امضا) — چون اعضای یک سبد دیگر لزوماً هم‌کوین/
    # هم‌امضا نیستند، این‌ها را به‌ازای هر عضو (نه هر سبد) نگه می‌داریم.
    #
    # ========== فیکس معماری (member_id به‌جای strategy_id) ==========
    # قبلاً اینجا روی strategy_id گروه‌بندی می‌شد و __sig همه‌ی signatureهای
    # متمایزی که آن strategy_id در کل استخر داشت را در یک لیست جمع می‌کرد
    # (می‌توانست ده‌ها/صدها مورد باشد چون یک استراتژی با فیلترهای زمانیِ
    # مختلف/pre-post رویداد/سشن/رژیم چند signature دارد) — یعنی
    # member_signatures در خروجی نهایی انفجاری و بی‌معنی می‌شد (لیستی از
    # signatureهای نامرتبط به همین سبد خاص). حالا که واحد گروه‌بندی
    # member_id است (که خودش دقیقاً یک strategy_id + یک signature را کد
    # می‌کند)، هر گروه ذاتاً فقط یک مقدار signature دارد؛ پس __sig یک رشته‌ی
    # تکی است (نه لیست) و «هر عضو دقیقاً یک امضا» تضمین می‌شود.
    strategy_meta = group.groupby("member_id").agg(
        __coin=("coin_composition", "first"),
        __sig=("signature", "first"),
    )

    # [فیکس ۱۳] برای هر دوره (release_date)، طول واقعی آن دوره (period_length_days)
    # را نگه می‌داریم تا بعداً بشود «تعداد_روز_فعال» یک سبد را (مجموع طول
    # روزهای واقعیِ فعال‌بودن اعضا) حساب کرد. اگر این ستون در داده موجود
    # نباشد (نسخه‌ی قدیمی JSONL)، به تعداد دوره‌ها (نه روز) بازمی‌گردیم.
    period_length_by_date = {}
    if "period_length_days" in group.columns:
        period_length_by_date = (
            group.groupby("release_date")["period_length_days"].max().to_dict()
        )

    return (candidate_strategies, clique_members, monthly, corr_lookup, strategy_meta,
            period_length_by_date, exact_valid_periods, period_bounds)


def score_combo(
    members: tuple,
    monthly: pd.DataFrame,
    corr_lookup: dict,
    strategy_meta: pd.DataFrame,
    period_length_by_date: dict,
    exact_valid_periods: dict,
    coin_composition: str,
    signature: str,
    attach_periods: bool = False,
    period_bounds: Optional[dict] = None,
) -> Optional[dict]:
    """امتیازدهی خامِ یک ترکیب (clique) — کاملاً مستقل از بقیه‌ی ترکیبات؛
    فقط به داده‌ی خودِ اعضای همین ترکیب نیاز دارد (monthly/corr_lookup که
    evaluate_group_build یک‌بار برای کل استخر ساخته). دقیقاً همان منطق
    قبلیِ بدنه‌ی حلقه‌ی evaluate_group است — فقط استخراج‌شده تا هر matrix
    job بتواند این تابع را مستقل برای برشی از صف ترکیبات صدا بزند. خروجی
    None یعنی این ترکیب رد می‌شود (دقیقاً معادل continue قبلی). توجه:
    نرمال‌سازی percentile_rank/امتیاز نهایی این‌جا انجام نمی‌شود — آن فقط
    در evaluate_group_finalize و روی کل مجموعه‌ی ادغام‌شده معنا دارد."""
    pairs = list(itertools.combinations(sorted(members), 2))
    if not all(p in corr_lookup for p in pairs):
        return None  # عملاً هیچ‌وقت نباید اجرا شود (ساخت clique تضمینش می‌کند)؛ فقط برای اطمینان

    # ========== رفع باگ ۲: Union ماه‌های فعال (نه Intersection) ==========
    # ماهی که یک عضو معامله نداشته، از سبد حذف نمی‌شود — سهم آن عضو
    # در آن ماه صفر در نظر گرفته می‌شود («این ماه معامله‌ای نداشت»،
    # نه «این ترکیب رد است»).
    active_months: set = set()
    for m in members:
        active_months |= set(monthly[m].dropna().index)
    if len(active_months) < MIN_PORTFOLIO_SAMPLES:
        return None

    sorted_months = sorted(active_months)
    returns = monthly.loc[sorted_months, list(members)].fillna(0.0)

    sr = survival_rate(returns)
    comp = compensation_ratio(returns)
    ar = avg_return(returns)
    ac = avg_correlation(members, corr_lookup)

    # گام ۸: فیلتر مطلق دیگر چیزی را رد نمی‌کند — فقط برچسب می‌زند.
    # [فیکس درخواستی کاربر] به‌جای continue/حذف، همیشه نگه داشته
    # می‌شود؛ تصمیم «رد شدن یا نه» بعداً و به‌صورت سراسری (بر اساس
    # تعداد کل واجدشرایط‌ها) گرفته می‌شود — نگاه کنید MIN_QUALIFIED_ROWS.
    passes_abs = (
        sr >= ABS_MIN_SURVIVAL_RATE
        and ar >= ABS_MIN_AVG_RETURN
    )

    # بازه‌ی روزانه‌ی واقعیِ فعال‌بودن سبد: از Union دقیق release_date
    # همه‌ی اعضا (نه از ماه‌ها) — برای اینکه ستون‌های روزی و بازسازی
    # _intervals (زمان‌بندی اجرای واقعی) دقیق بمانند.
    exact_union: set = set()
    for m in members:
        exact_union |= exact_valid_periods.get(m, set())
    sorted_exact = sorted(exact_union)
    if sorted_exact:
        بازه_شروع = sorted_exact[0]
        بازه_پایان = sorted_exact[-1]
    else:
        بازه_شروع = sorted_months[0].to_timestamp()
        بازه_پایان = sorted_months[-1].to_timestamp()
    if period_length_by_date and sorted_exact:
        روز_فعال = int(sum(period_length_by_date.get(d, 0) for d in sorted_exact))
    else:
        # داده‌ی قدیمی بدون period_length_days: به تعداد دوره (نه
        # روز) برمی‌گردیم — این تخمین کمینه است، نه دقیق.
        روز_فعال = len(sorted_exact) if sorted_exact else len(sorted_months)

    # [افزوده] ۱۶ ستون آماری ماهانه/افت‌سرمایه/ریسک‌به‌ریوارد: از روی
    # بازده‌ی سبد در هر ماه فعال.
    #
    # ========== باگ ۵ رفع شد ==========
    # قبلاً این‌جا از period_sums خام (مجموع اعضا، returns.sum(axis=1))
    # استفاده می‌شد که همان سوگیریِ به‌نفع سبدهای چندعضوی را دارد که
    # در بالای survival_rate/avg_return توضیح داده شده (کامنت «باگ ۲
    # رفع شد»). آن‌جا برای رفعش avg_return و survival_rate را با
    # میانگین (sum+mean)/2 متعادل کردند، اما همین تصحیح برای این ۱۶
    # ستون ماهانه اعمال نشده بود؛ در نتیجه اعدادی مثل
    # میانگین_سود_ماهانه/بهترین_ماه_درصد نسبت به avg_return واقعیِ
    # همان سبد ~۱.۳ تا ۱.۶ برابر تورم داشتند (برای سبدهای ۲ و ۳
    # عضوی). این‌جا هم از همان معیار متعادل‌شده استفاده می‌کنیم تا با
    # avg_return/survival_rate سازگار بماند.
    period_sums = returns.sum(axis=1)
    period_means = returns.mean(axis=1)
    balanced_returns = (period_sums + period_means) / 2.0
    dated_values = [(idx, float(v)) for idx, v in balanced_returns.items()]
    ext16 = _period_monthly_stats(dated_values)

    member_coins = [
        strategy_meta.loc[m, "__coin"] if m in strategy_meta.index else "" for m in members
    ]
    # هر عضو (member_id) دقیقاً یک امضا دارد — این لیست حالا طولش همیشه با
    # len(members) برابر است (نه یک لیست‌تودرلیست از ده‌ها امضای نامرتبط).
    member_sigs = [
        strategy_meta.loc[m, "__sig"] if m in strategy_meta.index else "" for m in members
    ]

    record = {
        "coin_composition": coin_composition,
        "signature": signature,
        "member_coin_compositions": member_coins,
        "member_signatures": member_sigs,
        "members": list(members),
        "survival_rate": sr,
        "compensation_ratio": comp,
        "avg_return": ar,
        "avg_correlation": ac,
        # ========== رفع باگ ۴: تعداد ماه‌هایی که حداقل یک عضو فعال
        # بوده (Union) — نه تعداد دوره‌های مشترک (Intersection) ==========
        "sample_count": len(active_months),
        "بازه_زمانی_شروع": pd.Timestamp(بازه_شروع).date().isoformat(),
        "بازه_زمانی_پایان": pd.Timestamp(بازه_پایان).date().isoformat(),
        "تعداد_روز_فعال": روز_فعال,
        "تعداد_روز_کل_بازه": (pd.Timestamp(بازه_پایان) - pd.Timestamp(بازه_شروع)).days + 1,
        **ext16,
    }
    record["_passes_abs"] = passes_abs
    if attach_periods:
        raw_intervals = [period_bounds[d] for d in sorted_exact if d in (period_bounds or {})]
        record["_intervals"] = _merge_intervals(raw_intervals)
    return record


def evaluate_group_finalize(
    portfolios: list[dict],
    raw_candidate_count: int,
    top_n: Optional[int],
    abs_filters: bool = True,
) -> tuple[list[dict], int]:
    """بخش سراسریِ دیگرِ evaluate_group: نرمال‌سازی percentile_rank و انتخاب
    نهایی. ذاتاً سراسری است چون percentile_rank هر سبد را نسبت به کل
    مجموعه‌ی سبدهای یافت‌شده رتبه‌بندی می‌کند — پس فقط باید یک‌بار، روی کل
    `portfolios` (که می‌تواند حاصل ادغام چند matrix job باشد)، صدا زده
    شود؛ هرگز روی زیرمجموعه‌ای از آن (وگرنه رتبه‌بندی نسبی غلط می‌شود)."""
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

    # ========== باگ ۱ رفع شد + فیکس درخواستی کاربر: به‌جای رد کردن سخت‌گیرانه،
    # این‌جا تصمیم می‌گیریم. اگر تعداد کاندیدهای واجدشرایط (survival_rate/
    # avg_return) به MIN_QUALIFIED_ROWS برسد، همان‌ها را (بر اساس score)
    # برمی‌گردانیم؛ در غیر این صورت (خروجی خیلی کم یا خالی می‌شد)، به‌جای
    # خالی ماندن، بهترین‌های *موجود* را بر اساس survival_rate (نه score) تا
    # سقف FALLBACK_TOP_N برمی‌گردانیم — این‌طور فیلترها دیگر چیزی را واقعاً
    # حذف نمی‌کنند، فقط بین دو حالت انتخاب می‌کنند. ==========
    if abs_filters:
        qualified_df = pf_df[pf_df["_passes_abs"]]
        if len(qualified_df) >= MIN_QUALIFIED_ROWS:
            pf_df = qualified_df.sort_values("score", ascending=False)
            if top_n is not None and top_n > 0:
                pf_df = pf_df.head(top_n)
        else:
            log.warning(
                "فقط %d سبد آستانه‌های survival_rate>=%.1f/avg_return>=%.2f را رعایت کردند "
                "(کمتر از حداقل %d) — به‌جای خروجی تقریباً خالی، بهترین %d سبد موجود بر اساس "
                "نرخ بقا (بدون فیلتر مطلق) برگردانده می‌شود.",
                len(qualified_df), ABS_MIN_SURVIVAL_RATE, ABS_MIN_AVG_RETURN,
                MIN_QUALIFIED_ROWS, FALLBACK_TOP_N,
            )
            pf_df = pf_df.sort_values("survival_rate", ascending=False).head(FALLBACK_TOP_N)
        pf_df = pf_df.drop(columns=["_passes_abs"])
    else:
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

    # ========== رفع باگ ۱ (حذف قید هم‌گروهی) — تغییر معماری گام‌های ۴-۶ ==========
    # قبلاً هر (coin_composition, signature) به‌طور مستقل و محدود به اعضای
    # خودش evaluate_group می‌شد (تا بشود پردازش را chunk/resume کرد). این
    # دقیقاً همان قید نادرستی بود که سبدها را به «هم‌کوین و هم‌امضا» محدود
    # می‌کرد. حالا correlation/ساخت سبد باید روی *کل* استخر Golden-qualified
    # یک‌جا انجام شود (فقط این‌طور می‌شود استراتژی‌های با کوین/شاخص خبری
    # متفاوت ولی همبستگی پایین را کنار هم گذاشت) — پس دیگر معنایی ندارد که
    # این محاسبه را به‌ازای هر گروه جداگانه chunk کنیم. علاوه بر این، محاسبه‌ی
    # همبستگی حالا برداری (pandas.DataFrame.corr) و برشمردنِ ترکیب‌ها graph/
    # clique-based است (نه itertools.combinations خام)، پس حتی روی کل استخر
    # هم به‌مراتب سریع‌تر از حلقه‌ی قبلیِ پایتونیِ per-group است.
    #
    # chunk_size/--resume برای این بخش دیگر به‌معنای «ادامه از گروه بعدی»
    # نیستند: چون محاسبه اکنون یک عملیات سراسری و اتمی است، --resume فقط به
    # این معناست که اگر اجرای قبلی با موفقیت کامل شده، دوباره محاسبه نشود.
    # (فایل‌های صف/وضعیت processed_signature_* که سیستم‌های بیرونی/CI برای
    # پاک‌سازی صف می‌خوانند، دقیقاً مثل قبل نوشته می‌شوند — فقط اکنون همه‌ی
    # all_sig_keys یک‌جا، در پایانِ یک اجرای موفق، processed علامت می‌خورند،
    # چون صحت سبدهای سراسری ذاتاً به دیدن کل استخر با هم نیاز دارد و پردازش
    # جزئی/ناقص یک زیرمجموعه دیگر معنای درستی ندارد.)
    if resume and status.get("status") == "completed":
        existing = output_dir / "portfolios.csv"
        if not existing.exists():
            existing = output_dir / "portfolios.parquet"
        if existing.exists():
            log.info("اجرای قبلی کامل بود — از --resume صرف‌نظر و فایل موجود بازگردانده شد: %s", existing)
            return existing

    if check_interrupt_flag(output_dir, interrupt_flag):
        log.warning("interrupt.flag شناسایی شد — بدون شروع محاسبه‌ی سراسری متوقف شد.")
        status["status"] = "interrupted"
        save_status(status_file, status)
        return output_dir / "portfolios.csv"

    status["status"] = "running"
    status["chunk_size"] = chunk_size
    save_status(status_file, status)

    all_portfolios: list[dict] = []
    total_raw_portfolios = 0

    if candidates.empty or candidates["member_id"].nunique() < 2:
        log.warning("کمتر از ۲ عضو (strategy_id+signature) واجد شرایط Golden یافت شد — سبدی ساخته نمی‌شود.")
    else:
        log.info(
            "ساخت سبد به‌صورت سراسری روی %d عضو (strategy_id+signature) واجد شرایط Golden (فارغ از کوین)...",
            candidates["member_id"].nunique(),
        )
        all_portfolios, total_raw_portfolios = evaluate_group(
            coin_composition="", signature="", group=candidates, top_n=top_n,
        )

    # همه‌ی گروه‌های بارگذاری‌شده در این اجرا در محاسبه‌ی سراسری بالا لحاظ
    # شدند (چه Golden ردشان کرده باشد چه نه — prefilter_candidates پیش‌تر
    # این تفکیک را انجام داده)، پس همه به‌عنوان processed علامت می‌خورند.
    processed_set.update(all_sig_keys)

    if all_portfolios:
        temp_df = pd.DataFrame(all_portfolios)
        _save_dataframe(temp_df, temp_results_base)

    # ---- گام ۶: پس از اتمام محاسبه‌ی سراسری ----
    # [فیکس] طبق درخواست کاربر، version_id و created_at از خروجی
    # portfolios.csv حذف شدند — این دو ستون صرفاً متادیتای اجرا بودن، نه
    # چیزی که برای تصمیم معاملاتی لازم باشه.
    # ========== رفع باگ ۱: ستون‌های coin_composition/signature سطح‌بالا با
    # member_coin_compositions/member_signatures جایگزین شدند چون اعضای یک
    # سبد دیگر لزوماً هم‌کوین/هم‌امضا نیستند. ==========
    columns = [
        "members", "member_coin_compositions", "member_signatures",
        "survival_rate", "compensation_ratio", "avg_return", "avg_correlation", "score",
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
# حالت «جدول زمانی پیوسته» (Timeline) — ماژول سوم
# -----------------------------------------------------------------------------
# هدف: از بین تمام سبدهای واجد شرایط (خروجی مشابه run_whole_time)، یک جدول
# پیوسته از اولین تا آخرین رخداد واقعی داده می‌سازد که برای هر بازه‌ی واقعی
# می‌گوید دقیقاً چه سبدی (یا ترکیبی از چند سبد هم‌پوشان) باید اجرا شود —
# طوری که حداکثر ممکن از محور زمان پوشش داده شود (نه فقط میانگین ۱۵٪ فعلی).

def _build_global_quality_arrays(pool: list[dict]) -> dict:
    """آرایه‌های مرتب‌شده‌ی هر متریک روی *کل استخر* کاندیدها (همه‌ی گروه‌ها با
    هم) — پایه‌ی صدک‌بندی سراسری quality_score که برخلاف score محلی
    evaluate_group، بین گروه‌های مختلف هم قابل مقایسه است."""
    return {
        "avg_return": np.sort(np.array([p["avg_return"] for p in pool], dtype=float)),
        "compensation_ratio": np.sort(np.array([p["compensation_ratio"] for p in pool], dtype=float)),
        "survival_rate": np.sort(np.array([p["survival_rate"] for p in pool], dtype=float)),
        # کمتر=بهتر برای همبستگی، پس با علامت منفی صدک‌بندی می‌کنیم تا مقیاس
        # «بزرگ‌تر=بهتر» با بقیه‌ی متریک‌ها یکی بماند
        "avg_correlation": np.sort(
            np.array([-p["avg_correlation"] for p in pool if not pd.isna(p["avg_correlation"])], dtype=float)
        ),
    }


def _percentile_of(value: float, sorted_arr: np.ndarray) -> float:
    """رتبه‌ی صدکی value نسبت به آرایه‌ی از قبل مرتب‌شده (بین ۰ و ۱۰۰)."""
    if sorted_arr.size == 0 or value is None or (isinstance(value, float) and math.isnan(value)):
        return 50.0  # داده‌ی ناکافی — نه امتیاز مثبت نه منفی
    idx = int(np.searchsorted(sorted_arr, value, side="right"))
    return 100.0 * idx / sorted_arr.size


def _quality_score(record: dict, global_arrays: dict) -> float:
    """quality_score سراسری و قابل‌مقایسه بین گروه‌ها (همان معیاری که قبلاً
    برای جدول ۸-ردیفی تأیید شد: بازده ۴۰٪ + جبران‌سازی ۳۰٪ + بقا ۲۰٪ +
    همبستگی پایین ۱۰٪)، به‌جای score محلیِ صدکی-درون‌گروهی evaluate_group."""
    corr_val = record.get("avg_correlation")
    corr_val = -corr_val if corr_val is not None and not pd.isna(corr_val) else corr_val
    return (
        QUALITY_WEIGHTS["return"] * _percentile_of(record.get("avg_return"), global_arrays["avg_return"])
        + QUALITY_WEIGHTS["compensation"] * _percentile_of(record.get("compensation_ratio"), global_arrays["compensation_ratio"])
        + QUALITY_WEIGHTS["survival"] * _percentile_of(record.get("survival_rate"), global_arrays["survival_rate"])
        + QUALITY_WEIGHTS["correlation"] * _percentile_of(corr_val, global_arrays["avg_correlation"])
    )


# =============================================================================
# [افزوده] سبد ثابت به‌ازای هر شرط خبری X
# -----------------------------------------------------------------------------
# مشکلی که این بخش حل می‌کند: در run_timeline/_resolve_segment، برای هر
# رخداد واقعیِ یک شرط خبری X (نوع خبر+آستانه+مدل+سشن، فارغ از رژیم بازار)
# برنده‌ی همان رخداد به‌تنهایی انتخاب می‌شد — که باعث می‌شد وقتی شرط X دوباره
# (در تاریخ دیگری) رخ می‌دهد، سبد انتخابی عوض شود (چون کاندیدهای فعال هر
# رخداد ممکن است متفاوت باشند). اینجا برعکس: برای هر شرط X، از میان تمام
# کاندیدهایی که در *کل تاریخچه* برایش دیده شده‌اند (فارغ از کوین)، یک‌بار
# برای همیشه یک سبد ثابت J (شامل کوین) بر اساس quality_score سراسری انتخاب
# می‌شود.
# =============================================================================

def _condition_key(record: dict) -> str:
    """شرط خبری X = signature رکورد (که در pool این ماژول از قبل بدون
    پسوند رژیم بازار است، چون از امضا_بدون_رژیم ساخته شده) منهای پیشوند
    coin_composition. کوین بخشی از خودِ سبد انتخابی J است، نه بخشی از شرط."""
    prefix = record["coin_composition"] + "_"
    sig = record["signature"]
    return sig[len(prefix):] if sig.startswith(prefix) else sig


def build_fixed_portfolio_per_condition(pool: list[dict]) -> list[dict]:
    """برای هر شرط خبری X، دقیقاً یک سبد ثابت J (شامل کوین) از میان همه‌ی
    کاندیدهای واجدشرایط (_passes_abs) کل تاریخچه، بر اساس quality_score
    سراسری (که باید از قبل روی هر رکورد pool محاسبه شده باشد) انتخاب
    می‌کند. خروجی: یک ردیف به‌ازای هر شرط X، به همراه فاصله‌ی امتیاز تا
    نفر دوم (margin) و تعداد کاندیدهای رقیب، برای سنجش قاطعیت انتخاب."""
    qualified = [r for r in pool if r.get("_passes_abs", True)]
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in qualified:
        groups[_condition_key(r)].append(r)

    rows = []
    for condition, candidates in groups.items():
        ranked = sorted(candidates, key=lambda r: r["quality_score"], reverse=True)
        winner = ranked[0]
        n_candidates = len(ranked)
        margin = float(winner["quality_score"] - ranked[1]["quality_score"]) if n_candidates > 1 else float("inf")
        rows.append({
            "شرط_خبری_X": condition,
            "coin_composition_ثابت": winner["coin_composition"],
            "signature_کامل": winner["signature"],
            "members": winner["members"],
            "survival_rate": winner["survival_rate"],
            "compensation_ratio": winner["compensation_ratio"],
            "avg_return": winner["avg_return"],
            "avg_correlation": winner["avg_correlation"],
            "sample_count": winner["sample_count"],
            "quality_score": winner["quality_score"],
            "تعداد_کاندید_رقیب": n_candidates,
            "فاصله_تا_نفر_دوم": margin,
            **{k: winner.get(k, 0.0) for k in EXT_STATS_16_COLUMNS},
        })
    return rows


def _evaluate_merge(
    active_items: list[dict],
    seg_start: pd.Timestamp,
    seg_end: pd.Timestamp,
    returns_lookup: pd.Series,
    global_arrays: dict,
) -> Optional[dict]:
    """اگر ۲+ سبد هم‌زمان در یک بازه‌ی اتمی فعال باشند، اعضای همه را Union
    می‌کند و survival_rate/compensation_ratio/avg_return/avg_correlation را
    دقیقاً با همان فرمول‌های evaluate_group، روی داده‌ی خام واقعیِ محدود به
    این بازه، از نو محاسبه می‌کند. اگر نمونه‌ی مشترک کافی نبود None برمی‌گرداند
    (یعنی: شواهد کافی برای توصیه‌ی ادغام نیست، به بهترین سبد تکی برمی‌گردیم)."""
    members = sorted({m for item in active_items for m in item["members"]})
    if len(members) < 2:
        return None
    try:
        sub = returns_lookup.loc[members]
    except KeyError:
        return None

    df = sub.reset_index()
    df.columns = ["member_id", "release_date", "total_return"]
    df = df[(df["release_date"] >= seg_start) & (df["release_date"] <= seg_end)]
    if df.empty:
        return None

    pivot = df.pivot_table(index="release_date", columns="member_id", values="total_return", aggfunc="mean")
    pivot = pivot.reindex(columns=members)
    pivot = pivot.dropna(how="any")
    if len(pivot) < MIN_PORTFOLIO_SAMPLES:
        return None

    sr = survival_rate(pivot)
    comp = compensation_ratio(pivot)
    ar = avg_return(pivot)

    corr_vals = []
    for a, b in itertools.combinations(members, 2):
        sub_ab = pivot[[a, b]].dropna()
        if len(sub_ab) >= MIN_PAIR_OVERLAP and sub_ab[a].nunique() > 1 and sub_ab[b].nunique() > 1:
            c, _ = spearmanr(sub_ab[a], sub_ab[b])
            if not np.isnan(c):
                corr_vals.append(float(c))
    ac = float(np.mean(corr_vals)) if corr_vals else float("nan")

    coins = sorted({item["coin_composition"] for item in active_items})
    sigs = sorted({item["signature"] for item in active_items})
    merged = {
        "coin_composition": "+".join(coins),
        "signature": " ⊕ ".join(sigs),
        "members": members,
        "survival_rate": sr,
        "compensation_ratio": comp,
        "avg_return": ar,
        "avg_correlation": ac,
        "sample_count": len(pivot),
    }
    merged["quality_score"] = _quality_score(merged, global_arrays)
    return merged


def _resolve_segment(
    seg_start: pd.Timestamp,
    seg_end: pd.Timestamp,
    active_qualified: list[dict],
    active_any: list[dict],
    returns_lookup: pd.Series,
    global_arrays: dict,
) -> dict:
    """تصمیم برای یک بازه‌ی اتمی: تکی / ترکیبی / پرکننده‌ی شکاف / بدون‌پوشش."""
    if active_qualified:
        if len(active_qualified) == 1:
            chosen = active_qualified[0]
            mode = "تکی"
        else:
            merged = _evaluate_merge(active_qualified, seg_start, seg_end, returns_lookup, global_arrays)
            best_single = max(active_qualified, key=lambda r: r["quality_score"])
            if merged is not None and merged["quality_score"] > best_single["quality_score"]:
                chosen = merged
                mode = "ترکیبی"
            else:
                chosen = best_single
                mode = "تکی-برتر (ادغام بهتر نبود)"
    elif active_any:
        chosen = max(active_any, key=lambda r: r["quality_score"])
        mode = "پرکننده‌شکاف (زیر آستانه‌ی فیلتر مطلق)"
    else:
        chosen = None
        mode = "بدون‌پوشش"

    row = {
        "شروع": seg_start.date().isoformat(),
        "پایان": seg_end.date().isoformat(),
        "روز": (seg_end - seg_start).days + 1,
        "حالت": mode,
    }
    if chosen is not None:
        row.update({
            "coin_composition": chosen.get("coin_composition"),
            "signature": chosen.get("signature"),
            "members": chosen.get("members"),
            "avg_return": chosen.get("avg_return"),
            "compensation_ratio": chosen.get("compensation_ratio"),
            "survival_rate": chosen.get("survival_rate"),
            "avg_correlation": chosen.get("avg_correlation"),
            "quality_score": chosen.get("quality_score"),
        })
    return row


def _collapse_adjacent(segments: list[dict]) -> list[dict]:
    """بازه‌های اتمیِ مجاور با تصمیم کاملاً یکسان (همان حالت + همان اعضا +
    همان coin_composition) را برای فشرده‌سازی خروجی با هم ادغام می‌کند."""
    if not segments:
        return []
    collapsed = [dict(segments[0])]
    for seg in segments[1:]:
        last = collapsed[-1]
        same = (
            seg["حالت"] == last["حالت"]
            and seg.get("members") == last.get("members")
            and seg.get("coin_composition") == last.get("coin_composition")
        )
        if same:
            last["پایان"] = seg["پایان"]
            last["روز"] = last["روز"] + seg["روز"]
        else:
            collapsed.append(dict(seg))
    return collapsed


def _build_timeline_segments(pool: list[dict], returns_lookup: pd.Series, global_arrays: dict) -> list[dict]:
    """موتور sweep-line اصلی: از روی بازه‌های واقعی (_intervals) همه‌ی
    کاندیدهای استخر، محور زمان را به بازه‌های اتمی (مجموعه‌ی کاندیدهای فعال
    ثابت) می‌شکند و برای هرکدام با _resolve_segment تصمیم می‌گیرد."""
    events: list[tuple] = []
    for idx, item in enumerate(pool):
        for s, e in item["_intervals"]:
            events.append((s, 1, idx))
            events.append((e + pd.Timedelta(days=1), -1, idx))  # نقطه‌ی پایانِ انحصاری

    if not events:
        return []

    events_by_point: dict = defaultdict(list)
    for date, delta, idx in events:
        events_by_point[date].append((delta, idx))
    unique_points = sorted(events_by_point.keys())

    active_count: dict[int, int] = {}
    segments: list[dict] = []

    for i, point in enumerate(unique_points):
        for delta, idx in events_by_point[point]:
            active_count[idx] = active_count.get(idx, 0) + delta
            if active_count[idx] <= 0:
                active_count.pop(idx, None)

        if i + 1 >= len(unique_points):
            break
        seg_start = point
        seg_end = unique_points[i + 1] - pd.Timedelta(days=1)
        if seg_start > seg_end:
            continue

        # ========== فیکس: بازه‌ی کاملاً بدون‌پوشش (نه حتی یک کاندیدِ زیرِ
        # آستانه) قبلاً به‌طور کامل حذف می‌شد (continue بدون ساخت ردیف) —
        # یعنی خروجی نهایی برای این بازه‌ها هیچ ردیفی نداشت و کاربر نمی‌فهمید
        # این بخش از محور زمان اصلاً پوشش داده نشده. حالا این بازه هم با
        # _resolve_segment (که در نبود هر دو لیست، حالت «بدون‌پوشش» تولید
        # می‌کند) صریحاً به‌عنوان ردیف ثبت می‌شود. ==========
        active_items = [pool[j] for j in active_count.keys()]
        active_qualified = [it for it in active_items if it.get("_passes_abs")]
        segments.append(_resolve_segment(seg_start, seg_end, active_qualified, active_items, returns_lookup, global_arrays))

    return _collapse_adjacent(segments)


def _load_whole_time_pool(
    whole_time_csv_path: Path,
    candidates: pd.DataFrame,
) -> list[dict]:
    """
    ========== باگ ۹ رفع شد (معماری): timeline دیگر استخر خودش را نمی‌سازد ==========
    قبلاً run_timeline با evaluate_group روی گروه‌های (coin, امضای بدون
    رژیم) از صفر یک استخر کاندیدِ مستقل می‌ساخت — با فیلتر همبستگیِ خودش،
    survival_rate/avg_return خودش، بدون هیچ ارتباطی به portfolios_whole_time.csv.
    نتیجه: همان جفت استراتژی می‌توانست در یکی قبول و در دیگری رد شود (چون
    آستانه‌ها نسبت‌به‌گروه بودند)، و survival_rate بین دو فایل ناسازگار
    می‌شد (طبق بحث قبلی: یک سبد survival_rate=70.3 در timeline که اصلاً در
    whole_time دیده نمی‌شد).

    حالا: تنها منبع کاندید برای جدول زمانی، سبدهایی هستند که از قبل در
    portfolios_whole_time.csv واجد شرایط شده‌اند (survival_rate/avg_return
    دقیقاً همان‌هایی که در آن‌جا محاسبه شده، نه از نو). این تابع برای هر
    ردیفِ whole_time، بازه‌های زمانیِ واقعیِ فعال‌بودن (_intervals) را از
    روی داده‌ی خامِ همین اجرا بازسازی می‌کند (چون whole_time.csv فقط بازه‌ی
    تجمیعیِ شروع/پایان را دارد، نه پنجره‌های روز‌به‌روز که موتور sweep-line
    نیاز دارد) — بدون این‌که هیچ فیلتر همبستگی یا آستانه‌ی جدیدی اعمال کند.

    اگر portfolios_whole_time.csv خالی/موجود نباشد، استخر خالی برمی‌گردد —
    طبق تصمیم صریح کاربر: «وگرنه اصلاً نباید کاری کند» (بدون fallback به
    ساخت مستقل کاندید).
    """
    if whole_time_csv_path is None or not Path(whole_time_csv_path).exists():
        log.warning(
            "فایل portfolios_whole_time.csv یافت نشد (%s) — طبق طراحیِ جدید، "
            "timeline بدون آن هیچ سبدی نمی‌سازد و جدول زمانی خالی خواهد بود.",
            whole_time_csv_path,
        )
        return []

    wt = pd.read_csv(whole_time_csv_path)
    if wt.empty:
        log.warning("portfolios_whole_time.csv خالی است — جدول زمانی خالی خواهد بود.")
        return []

    # members داخل portfolios_whole_time.csv اکنون مقادیر member_id هستند
    # (strategy_id+signature)، پس این دیکشنری هم باید با همان کلید ساخته شود.
    exact_valid_periods = {
        strat: set(sub["release_date"]) for strat, sub in candidates.groupby("member_id")
    }
    period_bounds = _period_bounds_by_date(candidates)

    pool: list[dict] = []
    skipped_no_timing = 0
    for _, row in wt.iterrows():
        try:
            members = tuple(ast.literal_eval(row["members"]))
        except Exception:
            continue
        if len(members) < 2:
            continue

        try:
            member_coins = ast.literal_eval(row.get("member_coin_compositions", "[]"))
        except Exception:
            member_coins = []

        exact_union: set = set()
        for m in members:
            exact_union |= exact_valid_periods.get(m, set())
        sorted_exact = sorted(exact_union)
        raw_intervals = [period_bounds[d] for d in sorted_exact if d in period_bounds]
        intervals = _merge_intervals(raw_intervals)
        if not intervals:
            # این سبد در whole_time واجد شرایط بوده، ولی در داده‌ی خامِ این
            # اجرای خاص (مثلاً یک بازه‌ی زمانیِ متفاوت/جدیدتر) رد پایی از
            # بازه‌ی فعال‌بودنش پیدا نشد — رد می‌شود، نه این‌که با مقدار
            # تخمینی جایگزین شود.
            skipped_no_timing += 1
            continue

        pool.append({
            "coin_composition": "+".join(sorted(set(member_coins))) if member_coins else "",
            "signature": "+".join(members),
            "members": list(members),
            "survival_rate": float(row["survival_rate"]),
            "compensation_ratio": float(row["compensation_ratio"]),
            "avg_return": float(row["avg_return"]),
            "avg_correlation": float(row["avg_correlation"]),
            "sample_count": int(row["sample_count"]),
            "_intervals": intervals,
            "_passes_abs": True,
        })

    if skipped_no_timing:
        log.info(
            "%d سبد از whole_time به‌خاطر نبود بازه‌ی زمانیِ قابل‌بازسازی در داده‌ی خام این اجرا رد شدند.",
            skipped_no_timing,
        )
    return pool


def run_timeline(
    signatures_dir: Path,
    golden_scores_path: Optional[Path],
    version_schema_path: Optional[Path],
    output_dir: Path,
    whole_time_csv: Path,
    signatures_filter: Optional[Path] = None,
) -> Path:
    """
    ماژول سوم: یک جدول زمانی پیوسته از اولین تا آخرین رخداد واقعی می‌سازد که
    برای هر بازه می‌گوید دقیقاً چه سبدی (یا ترکیبی) باید اجرا شود.

    ========== باگ ۹ رفع شد (معماری) ==========
    این ماژول دیگر کاندید خودش را نمی‌سازد؛ فقط از میان سبدهایی که
    portfolios_whole_time.csv از قبل واجد شرایط کرده انتخاب می‌کند (نگاه
    کنید _load_whole_time_pool). اگر برای یک بازه هیچ سبد واجدشرایطی
    هم‌پوشانی نداشته باشد، آن بازه «بدون‌پوشش» علامت می‌خورد — دیگر
    «پرکننده‌ی شکاف با کاندید زیر آستانه» ساخته نمی‌شود، چون آن استخرِ
    زیرآستانه دیگر در این‌جا وجود ندارد (طبق تصمیم صریح کاربر).

    به همین دلیل build_fixed_portfolio_per_condition (که مفهوم «شرط خبری
    X» را بر پایه‌ی یک coin_composition/signature واحد به‌ازای هر کاندید
    فرض می‌کرد) دیگر این‌جا صدا زده نمی‌شود: سبدهای whole_time می‌توانند
    اعضایی از چند کوین/امضای متفاوت هم‌زمان داشته باشند (دقیقاً همان قیدی
    که در run_whole_time هم عمداً حذف شد)، پس دیگر یک «شرط خبری واحد»ی
    که بشود کاندیدها را بر اساسش گروه‌بندی کرد، معنادار نیست.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    signatures = load_signatures(signatures_dir, signatures_filter)
    load_version_schema(version_schema_path)

    if golden_scores_path is not None:
        golden = load_golden_scores(golden_scores_path)
        candidates = prefilter_candidates(signatures, golden)
    else:
        log.warning("golden_scores ارائه نشده — پیش‌فیلتر Golden رد می‌شود.")
        candidates = signatures

    columns = [
        "شروع", "پایان", "روز", "حالت", "coin_composition", "signature",
        "members", "avg_return", "compensation_ratio", "survival_rate",
        "avg_correlation", "quality_score",
    ]

    if candidates.empty:
        log.warning("هیچ رکورد کاندیدی برای ساخت جدول زمانی یافت نشد.")
        out_df = pd.DataFrame(columns=columns)
        return _save_dataframe(out_df, output_dir / "portfolios_timeline")

    candidates = build_release_dates(candidates.copy())

    pool = _load_whole_time_pool(whole_time_csv, candidates)
    log.info("حالت جدول زمانی: %d سبد از portfolios_whole_time.csv برای پوشش محور زمان بارگذاری شد.", len(pool))

    if not pool:
        log.warning("هیچ سبد قابل‌استفاده‌ای از whole_time یافت نشد — جدول زمانی خالی خواهد بود.")
        out_df = pd.DataFrame(columns=columns)
        return _save_dataframe(out_df, output_dir / "portfolios_timeline")

    global_arrays = _build_global_quality_arrays(pool)
    for record in pool:
        record["quality_score"] = _quality_score(record, global_arrays)

    # ---- نمایه‌ی بازده‌ی خام هر عضو (member_id) در هر release_date، برای محاسبه‌ی دقیق سبدهای ادغام‌شده ----
    returns_lookup = (
        candidates.groupby(["member_id", "release_date"])["total_return"].mean()
    )

    segments = _build_timeline_segments(pool, returns_lookup, global_arrays)

    out_df = pd.DataFrame(segments)
    if not out_df.empty:
        out_df = out_df[[c for c in columns if c in out_df.columns]]

    output_path = output_dir / "portfolios_timeline"
    final_path = _save_dataframe(out_df, output_path)
    log.info("ذخیره شد: %s (%d بازه‌ی نهایی پس از فشرده‌سازی)", final_path, len(out_df))
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
    نسخه‌ی «کل بازه‌ی زمانی»: مثل run()، دیگر قید «هم‌کوین/هم‌امضا» روی
    عضویت در سبد وجود ندارد — این قید فقط برای ساده‌سازی محاسبه‌ی همبستگیِ
    شرطی در کد قدیمی آمده بود، نه یک الزام تجاری واقعی (نگاه کنید به توضیح
    رفع باگ ۱ در run()). حالا مثل run()، evaluate_group یک‌بار روی *کل*
    استخر Golden-qualified صدا زده می‌شود؛ هر استراتژی (با هر کوین/شاخص
    خبری) اگر همبستگی بازدهی‌اش با بقیه‌ی اعضا پایین باشد می‌تواند در یک
    سبد قرار بگیرد. تفاوتش با run() فقط در این است که run() به‌ازای هر
    اجرا محدود به رکوردهای همان اجرا (chunk) است، در حالی که این تابع طبق
    طراحی همیشه روی کل تاریخچه (بدون قید رویداد خبری) کار می‌کند.

    همان evaluate_group (همان فرمول‌های survival_rate/compensation_ratio/
    avg_return/avg_correlation/score) عیناً استفاده می‌شود؛ این‌طور تضمین
    می‌شود که همان محافظت در برابر «سود و ضرر خنثی‌کننده‌ی هم» که برای حالت
    خبری طراحی شده، اینجا هم برقرار است.

    این تابع chunk/resume/interrupt جداگانه ندارد — چون محاسبه (دقیقاً مثل
    run() پس از رفع باگ ۱) یک عملیات سراسری و اتمی روی کل استخر است، نه
    چیزی که با معنای درستی بشود به‌ازای هر گروه chunk کرد.
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

    # ========== رفع باگ ۱ (همان فیکس run()، اینجا هم اعمال شد) ==========
    # نسخه‌ی قبلی همچنان روی (coin_composition, امضای بدون رژیم) گروه‌بندی
    # می‌کرد — یعنی دقیقاً همان قید نادرست «هم‌کوین و هم‌امضا»یی که در run()
    # حذف شد، اینجا فقط با نام «امضای بدون رژیم» باقی مانده بود. این قید،
    # درست مثل قبل، صرفاً برای ساده‌سازی محاسبه‌ی همبستگیِ شرطی آمده بود؛
    # منطق تجاری «سبد چند استراتژی با همبستگی پایین» به کوین یا شاخص خبری
    # هیچ‌کدام کاری ندارد. حالا evaluate_group یک‌بار روی کل استخر
    # Golden-qualified صدا زده می‌شود، بدون هیچ گروه‌بندی‌ای.
    if candidates.empty or candidates["member_id"].nunique() < 2:
        log.warning("کمتر از ۲ عضو (strategy_id+signature) واجد شرایط Golden یافت شد — سبدی ساخته نمی‌شود.")
        all_portfolios, total_raw = [], 0
    else:
        log.info(
            "حالت کل بازه‌ی زمانی: ساخت سبد به‌صورت سراسری روی %d عضو (strategy_id+signature) واجد شرایط Golden (فارغ از کوین)...",
            candidates["member_id"].nunique(),
        )
        all_portfolios, total_raw = evaluate_group(
            coin_composition="", signature="", group=candidates, top_n=top_n,
        )
    log.info("حالت کل بازه‌ی زمانی: %d سبد یافت شد (از %d کاندید خام).", len(all_portfolios), total_raw)

    # [فیکس] طبق درخواست کاربر، version_id و created_at از خروجی
    # portfolios_whole_time.csv هم حذف شدند.
    # ========== رفع باگ ۱: ستون‌های coin_composition/signature سطح‌بالا با
    # member_coin_compositions/member_signatures جایگزین شدند — عیناً مثل
    # run()، چون اعضای یک سبد دیگر لزوماً هم‌کوین/هم‌امضا نیستند. ==========
    columns = [
        "members", "member_coin_compositions", "member_signatures",
        "survival_rate", "compensation_ratio", "avg_return", "avg_correlation", "score",
        "sample_count",
        # [فیکس ۱۳] بازه‌ی زمانی بک‌تست این سبد
        "بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال", "تعداد_روز_کل_بازه",
    ] + EXT_STATS_16_COLUMNS

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
# حالت matrix-پذیرِ «کل بازه‌ی زمانی» — سه فاز build / score / merge
#
# طبق تحلیل بالا (evaluate_group_build/score_combo/evaluate_group_finalize):
# تنها بخشی که واقعاً به‌ازای هر ترکیب مستقل و سبک است، امتیازدهیِ خودِ
# ترکیب‌هاست (score_combo). ساختِ گراف/لیستِ ترکیبات (evaluate_group_build)
# و نرمال‌سازیِ percentile نهایی (evaluate_group_finalize) هر دو ذاتاً
# سراسری‌اند و فقط یک‌بار انجام می‌شوند. این سه تابع دقیقاً همین سه‌فازی را
# با فایل روی دیسک پیاده می‌کنند تا فاز ۲ بشود یک matrix job روی GitHub
# Actions:
#   ۱) run_whole_time_build   → یک‌بار: دانلود/بارگذاری کامل شده، صف
#      ترکیبات (بدون هیچ سقفی روی تعدادشان) + داده‌ی مشترک سبک را می‌سازد.
#   ۲) run_whole_time_score   → به‌ازای هر matrix job: فقط برشی از صف را
#      امتیازدهی می‌کند؛ هیچ دانلود/محاسبه‌ی تکراری‌ای ندارد.
#   ۳) run_whole_time_merge   → یک‌بار: همه‌ی خروجی‌های جزئی را ادغام،
#      نرمال‌سازی/انتخاب نهایی (که ذاتاً سراسری است) را انجام می‌دهد.
# -----------------------------------------------------------------------------

def _flatten_combo_queue(clique_members: dict) -> list[dict]:
    """لیست تخت همه‌ی ترکیبات (هر اندازه‌ای که در PORTFOLIO_SIZES باشد) —
    این همان صفی است که بین matrix jobها تقسیم می‌شود. عمداً هیچ سقفی روی
    طولش اعمال نمی‌شود: بر خلاف صف archive/signature در analysis_portfolios.yml
    (که می‌تواند مازادش را برای اجرای بعدی نگه دارد)، این‌جا همه‌ی ترکیبات
    باید در همین اجرا امتیازدهی شوند — چون evaluate_group_finalize باید کل
    مجموعه‌ی سبدهای یافت‌شده را ببیند تا percentile_rank/انتخاب نهایی درست
    باشد؛ نادیده گرفتن بخشی از ترکیبات یعنی رتبه‌بندی نهایی ناقص/غلط."""
    queue: list[dict] = []
    for size in sorted(clique_members.keys()):
        for members in clique_members[size]:
            queue.append({"size": size, "members": list(members)})
    return queue


def run_whole_time_build(
    signatures_dir: Path,
    golden_scores_path: Optional[Path],
    version_schema_path: Optional[Path],
    output_dir: Path,
    signatures_filter: Optional[Path] = None,
) -> Path:
    """فاز ۱ از ۳: تنها فازی که به دانلود/بارگذاری کامل تمام آرشیوها نیاز
    دارد (چون تعیین کاندیدهای نهایی و اعتبارِ هر جفت (MIN_PAIR_OVERLAP) فقط
    با دیدن همزمان همه‌ی داده ممکن است). خروجی این فاز: صف کامل ترکیبات
    (combos.json) + جدول‌های سبکِ لازم برای امتیازدهی مستقل هر ترکیب در فاز
    ۲ (بدون نیاز به دانلود مجدد هیچ آرشیوی)."""
    output_dir.mkdir(parents=True, exist_ok=True)

    def _write_empty() -> Path:
        (output_dir / "combos.json").write_text("[]", encoding="utf-8")
        (output_dir / "empty.flag").write_text("1", encoding="utf-8")
        return output_dir / "combos.json"

    signatures = load_signatures(signatures_dir, signatures_filter)
    load_version_schema(version_schema_path)

    if golden_scores_path is not None:
        golden = load_golden_scores(golden_scores_path)
        candidates = prefilter_candidates(signatures, golden)
    else:
        log.warning("golden_scores ارائه نشده — پیش‌فیلتر Golden رد می‌شود.")
        candidates = signatures

    if candidates.empty or candidates["member_id"].nunique() < 2:
        log.warning("کمتر از ۲ عضو (strategy_id+signature) واجد شرایط Golden یافت شد — صف ترکیبات خالی خواهد بود.")
        return _write_empty()

    built = evaluate_group_build(candidates, attach_periods=False)
    if built is None:
        log.warning("پس از فیلتر هم‌پوشانی/سقف کاندیدها، هیچ ترکیبی باقی نماند.")
        return _write_empty()

    (_candidate_strategies, clique_members, monthly, corr_lookup, strategy_meta,
     period_length_by_date, exact_valid_periods, _period_bounds) = built

    queue = _flatten_combo_queue(clique_members)
    log.info("تعداد کل ترکیبات کاندید (بدون هیچ سقفی): %d", len(queue))

    monthly_out = monthly.copy()
    monthly_out.index = monthly_out.index.astype(str)  # PeriodIndex → رشته برای parquet
    monthly_out.to_parquet(output_dir / "monthly_returns.parquet")

    corr_out = pd.DataFrame(
        [{"a": a, "b": b, "correlation": c} for (a, b), c in corr_lookup.items()],
        columns=["a", "b", "correlation"],
    )
    corr_out.to_parquet(output_dir / "corr_lookup.parquet")

    meta_out = strategy_meta.copy()
    meta_out["__sig"] = meta_out["__sig"].apply(json.dumps)
    meta_out.to_parquet(output_dir / "strategy_meta.parquet")

    evp_rows = [
        {"member_id": s, "release_date": d}
        for s, dates in exact_valid_periods.items() for d in dates
    ]
    pd.DataFrame(evp_rows, columns=["member_id", "release_date"]).to_parquet(
        output_dir / "exact_valid_periods.parquet"
    )

    pld_rows = [
        {"release_date": d, "period_length_days": v}
        for d, v in period_length_by_date.items()
    ]
    pd.DataFrame(pld_rows, columns=["release_date", "period_length_days"]).to_parquet(
        output_dir / "period_length_by_date.parquet"
    )

    combos_path = output_dir / "combos.json"
    combos_path.write_text(json.dumps(queue, ensure_ascii=False), encoding="utf-8")
    (output_dir / "empty.flag").write_text("0", encoding="utf-8")
    log.info(
        "✅ فاز build کامل شد: %d ترکیب در combos.json، داده‌ی مشترک در %s.",
        len(queue), output_dir,
    )
    return combos_path


def run_whole_time_score(shared_dir: Path, combos_file: Path, output_file: Path) -> Path:
    """فاز ۲ از ۳ (matrix، به‌ازای هر chunk یک اجرا): فقط برشی از combos.json
    را که فاز build ساخته امتیازدهی می‌کند. هیچ آرشیوی دوباره دانلود
    نمی‌شود — فقط جدول‌های سبکِ فاز build خوانده می‌شوند. عمداً percentile_rank/
    امتیاز نهایی این‌جا محاسبه نمی‌شود (نگاه کنید evaluate_group_finalize)."""
    shared_dir = Path(shared_dir)

    monthly = pd.read_parquet(shared_dir / "monthly_returns.parquet")
    monthly.index = pd.PeriodIndex(monthly.index, freq="M")

    corr_df = pd.read_parquet(shared_dir / "corr_lookup.parquet")
    corr_lookup = {(r.a, r.b): r.correlation for r in corr_df.itertuples()}

    strategy_meta = pd.read_parquet(shared_dir / "strategy_meta.parquet")
    strategy_meta["__sig"] = strategy_meta["__sig"].apply(json.loads)

    evp_df = pd.read_parquet(shared_dir / "exact_valid_periods.parquet")
    evp_df["release_date"] = pd.to_datetime(evp_df["release_date"])
    exact_valid_periods = {
        s: set(sub["release_date"]) for s, sub in evp_df.groupby("member_id")
    }

    pld_df = pd.read_parquet(shared_dir / "period_length_by_date.parquet")
    pld_df["release_date"] = pd.to_datetime(pld_df["release_date"])
    period_length_by_date = dict(zip(pld_df["release_date"], pld_df["period_length_days"]))

    combos = json.loads(Path(combos_file).read_text(encoding="utf-8"))

    records = []
    for item in combos:
        rec = score_combo(
            tuple(item["members"]), monthly, corr_lookup, strategy_meta,
            period_length_by_date, exact_valid_periods,
            coin_composition="", signature="",
        )
        if rec is not None:
            records.append(rec)

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(records)
    if not out_df.empty:
        # لیست‌ها (members/member_coin_compositions/member_signatures) برای
        # ذخیره‌ی parquet باید به رشته‌ی JSON تبدیل شوند؛ در فاز merge برگردانده می‌شوند.
        for col in ("members", "member_coin_compositions", "member_signatures"):
            out_df[col] = out_df[col].apply(json.dumps)
    out_df.to_parquet(output_file)
    log.info(
        "✅ این chunk: %d سبد معتبر از %d ترکیب امتیازدهی شد → %s",
        len(out_df), len(combos), output_file,
    )
    return output_file


_WT_MERGE_HEAVY_COLUMNS = ["members", "member_coin_compositions", "member_signatures"]
_WT_MERGE_OUTPUT_COLUMNS = [
    "members", "member_coin_compositions", "member_signatures",
    "survival_rate", "compensation_ratio", "avg_return", "avg_correlation", "score",
    "sample_count",
    "بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال", "تعداد_روز_کل_بازه",
] + EXT_STATS_16_COLUMNS
_WT_MERGE_LIGHT_COLUMNS = [
    c for c in _WT_MERGE_OUTPUT_COLUMNS if c not in _WT_MERGE_HEAVY_COLUMNS
] + ["_passes_abs"]


def _wt_merge_check_parts(parts_dir: Path) -> list[Path]:
    """[دیباگ کد ۱۴۳ / سرنخ ۱] چک می‌کند فایل‌های part واقعاً رسیده‌اند، حجم
    هرکدام را لاگ می‌کند، و لیست مرتب‌شده (بر اساس عدد chunk، نه رشته) را
    برمی‌گرداند."""
    all_glob_matches = sorted(Path(parts_dir).glob("part_*.parquet"))
    log.info(
        "[CHECK] پوشه‌ی parts-dir=%s | تعداد فایل part_*.parquet پیدا شده=%d",
        parts_dir, len(all_glob_matches),
    )
    total_bytes_on_disk = 0
    for pf in all_glob_matches:
        try:
            sz = pf.stat().st_size
        except OSError as e:
            log.warning("[CHECK] فایل %s قابل‌ استت نیست: %s", pf, e)
            continue
        total_bytes_on_disk += sz
        log.info("[CHECK]   %s -> %.1fMB روی دیسک", pf.name, sz / (1024 * 1024))
    log.info(
        "[CHECK] مجموع حجم همه‌ی part ها روی دیسک: %.1fMB",
        total_bytes_on_disk / (1024 * 1024),
    )
    # ========== باگ: sorted() روی نام فایل رشته‌ای است، نه عدد chunk — یعنی
    # part_10 قبل از part_2 می‌آید. چون انتخاب نهایی (FALLBACK_TOP_N/top_n)
    # روی تساوی‌های survival_rate/score با sort_values (ناپایدار در برابر
    # ترتیب ورودی برای مقادیر مساوی) تصمیم می‌گیرد، ترتیب غلط ادغام می‌تواند
    # باعث شود مجموعه‌ی سبدهای انتخاب‌شده به‌طور غیرقطعی تغییر کند. ==========
    return sorted(all_glob_matches, key=lambda p: int(p.stem.split("_")[-1]))


def _wt_merge_finalize_and_save(
    all_portfolios: list[dict], total_raw: int, output_dir: Path, n_parts: int,
) -> Path:
    if not all_portfolios:
        out_df = pd.DataFrame(columns=_WT_MERGE_OUTPUT_COLUMNS)
    else:
        out_df = pd.DataFrame(all_portfolios)
        out_df = out_df[[c for c in _WT_MERGE_OUTPUT_COLUMNS if c in out_df.columns]]
    output_path = Path(output_dir) / "portfolios_whole_time"
    final_path = _save_dataframe(out_df, output_path)
    _log_mem("بعد از ذخیره‌سازی خروجی نهایی")
    log.info(
        "ذخیره شد: %s (%d سبد نهایی از %d سبد خامِ ادغام‌شده‌ی %d part)",
        final_path, len(out_df), total_raw, n_parts,
    )
    return final_path


def _merge_strategy_full(part_files: list[Path], output_dir: Path, top_n: Optional[int]) -> Path:
    """راهبرد ۱ (پیش‌فرض، پایه): دقیقاً همان منطق قبلی — همه‌ی ستون‌ها
    (شامل members و بقیه‌ی ستون‌های تودرتو) برای *همه‌ی* ردیف‌ها بلافاصله
    با json.loads باز می‌شوند، concat می‌شوند، و یک‌جا به evaluate_group_finalize
    داده می‌شوند. بیشترین مصرف حافظه‌ی peak را دارد؛ اگر داده به اندازه‌ی
    کافی کوچک باشد سریع‌ترین و ساده‌ترین حالت است."""
    dfs = []
    total_rows = 0
    for pf in part_files:
        df = pd.read_parquet(pf)
        _log_mem(f"[full] بعد از read_parquet({pf.name}) -> {len(df)} ردیف")
        if df.empty:
            continue
        for col in _WT_MERGE_HEAVY_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(json.loads)
        _log_mem(f"[full] بعد از json.loads ستون‌های تودرتوی {pf.name}")
        total_rows += len(df)
        dfs.append(df)

    log.info("[CHECK] مجموع ردیف‌های همه‌ی %d part پیش از concat: %d", len(dfs), total_rows)

    if not dfs:
        all_portfolios, total_raw = [], 0
    else:
        merged = pd.concat(dfs, ignore_index=True)
        dfs = None
        _log_mem(f"[full] بعد از pd.concat -> shape={merged.shape}")
        records = merged.to_dict("records")
        _log_mem(f"[full] بعد از to_dict('records') -> {len(records)} رکورد")
        merged = None
        all_portfolios, total_raw = evaluate_group_finalize(
            records, total_rows, top_n, abs_filters=True,
        )
        _log_mem("[full] بعد از evaluate_group_finalize")

    return _wt_merge_finalize_and_save(all_portfolios, total_raw, output_dir, len(part_files))


def _merge_strategy_lazy_json(
    part_files: list[Path], output_dir: Path, top_n: Optional[int],
) -> Path:
    """راهبرد ۲ (فالبک اول): همان مسیر قبلی، با یک تفاوت: رشته‌های JSON سه
    ستون تودرتو (members/member_coin_compositions/member_signatures) تا
    *بعد* از فیلتر/انتخاب نهایی هیچ‌وقت decode نمی‌شوند — یعنی در تمام مسیر
    concat/to_dict/evaluate_group_finalize این ستون‌ها همچنان رشته‌ی فشرده‌ی
    JSON هستند (بسیار کوچک‌تر از لیست/دیکشنری پایتونیِ expand‌شده). فقط
    ردیف‌های نهایی برنده (top_n) در آخر decode می‌شوند. منطق/خروجی دقیقاً
    همان full است؛ فقط peak memory کمتر است."""
    dfs = []
    total_rows = 0
    for pf in part_files:
        df = pd.read_parquet(pf)
        _log_mem(f"[lazy-json] بعد از read_parquet({pf.name}) -> {len(df)} ردیف (بدون decode)")
        if df.empty:
            continue
        total_rows += len(df)
        dfs.append(df)

    log.info("[CHECK] مجموع ردیف‌های همه‌ی %d part پیش از concat: %d", len(dfs), total_rows)

    if not dfs:
        all_portfolios, total_raw = [], 0
    else:
        merged = pd.concat(dfs, ignore_index=True)
        dfs = None
        _log_mem(f"[lazy-json] بعد از pd.concat -> shape={merged.shape}")
        records = merged.to_dict("records")
        _log_mem(f"[lazy-json] بعد از to_dict('records') -> {len(records)} رکورد")
        merged = None
        all_portfolios, total_raw = evaluate_group_finalize(
            records, total_rows, top_n, abs_filters=True,
        )
        _log_mem(f"[lazy-json] بعد از evaluate_group_finalize -> {len(all_portfolios)} برنده")
        # فقط همین چند ردیف برنده را decode می‌کنیم
        for rec in all_portfolios:
            for col in _WT_MERGE_HEAVY_COLUMNS:
                if col in rec and isinstance(rec[col], str):
                    rec[col] = json.loads(rec[col])
        _log_mem("[lazy-json] بعد از decode برندگان")

    return _wt_merge_finalize_and_save(all_portfolios, total_raw, output_dir, len(part_files))


def _read_heavy_rows_streaming(
    pf: Path, row_idxs: list[int], batch_size: int = 20_000,
) -> dict[int, dict]:
    """فقط ستون‌های سنگین (_WT_MERGE_HEAVY_COLUMNS) همون چند ردیف
    row_idxs رو از فایل pf می‌خونه — بدون اینکه کل فایل یا حتی یک
    row-group کامل یک‌جا در حافظه بیاد. با iter_batches جلو می‌ریم؛ هر
    batch بعد از پردازش (و پیدا کردن تقاطعش با row_idxs) دور ریخته
    می‌شه، پس peak memory این تابع مستقل از تعداد کل ردیف‌های part و
    فقط تابعی از batch_size است. کلید dict خروجی همون __row_idx اصلیه."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    wanted = set(row_idxs)
    result: dict[int, dict] = {}
    pf_obj = pq.ParquetFile(pf)
    offset = 0
    for batch in pf_obj.iter_batches(batch_size=batch_size, columns=_WT_MERGE_HEAVY_COLUMNS):
        batch_len = batch.num_rows
        local_idxs = sorted(i - offset for i in wanted if offset <= i < offset + batch_len)
        if local_idxs:
            sub = pc.take(batch, pa.array(local_idxs, type=pa.int64())).to_pandas()
            for local_i, (_, row) in zip(local_idxs, sub.iterrows()):
                result[local_i + offset] = {col: row[col] for col in _WT_MERGE_HEAVY_COLUMNS}
        offset += batch_len
        if len(result) == len(wanted):
            break  # همه‌ی ردیف‌های موردنیاز این part پیدا شدن، ادامه نده
    return result


def _merge_strategy_columnar(
    part_files: list[Path], output_dir: Path, top_n: Optional[int],
) -> Path:
    """راهبرد ۳ (فالبک دوم، سنگین‌ترین سناریو): سه ستون تودرتو اصلاً در
    مرحله‌ی امتیازدهی/انتخاب خوانده نمی‌شوند — با column-projection پارکت
    فقط ستون‌های سبک (اسکالر) برای *همه*‌ی part ها خوانده و concat می‌شوند
    (حافظه‌ی این مرحله چند برابر کوچک‌تر از دو راهبرد قبلی است چون حتی
    رشته‌ی JSON فشرده هم برای ردیف‌های غیربرنده هرگز به حافظه نمی‌آید).
    evaluate_group_finalize روی همین ستون‌های سبک (که همان ستون‌های واقعی
    مورد نیازش هستند: survival_rate/compensation_ratio/avg_return/
    avg_correlation/_passes_abs) دقیقاً همان برندگان را انتخاب می‌کند.
    فقط برای همان چند ردیف برنده، ستون‌های سنگین از فایل part اصلی‌شان
    (با شناسه‌ی __part_idx/__row_idx که در پاس اول اضافه شده) مجدداً و
    این‌بار *فقط برای آن ردیف‌ها* خوانده و decode می‌شوند."""
    light_dfs = []
    total_rows = 0
    for part_idx, pf in enumerate(part_files):
        try:
            import pyarrow.parquet as pq
            available = set(pq.ParquetFile(pf).schema.names)
        except Exception:
            available = None
        cols = _WT_MERGE_LIGHT_COLUMNS
        if available is not None:
            cols = [c for c in _WT_MERGE_LIGHT_COLUMNS if c in available]
        df = pd.read_parquet(pf, columns=cols)
        _log_mem(f"[columnar] بعد از خواندن ستون‌های سبک {pf.name} -> {len(df)} ردیف")
        if df.empty:
            continue
        df["__part_idx"] = part_idx
        df["__row_idx"] = np.arange(len(df))
        total_rows += len(df)
        light_dfs.append(df)

    log.info("[CHECK] مجموع ردیف‌های همه‌ی %d part پیش از concat (سبک): %d", len(light_dfs), total_rows)

    if not light_dfs:
        return _wt_merge_finalize_and_save([], 0, output_dir, len(part_files))

    merged_light = pd.concat(light_dfs, ignore_index=True)
    light_dfs = None
    _log_mem(f"[columnar] بعد از concat سبک -> shape={merged_light.shape}")
    light_records = merged_light.to_dict("records")
    merged_light = None
    _log_mem(f"[columnar] بعد از to_dict سبک -> {len(light_records)} رکورد")

    winners, total_raw = evaluate_group_finalize(light_records, total_rows, top_n, abs_filters=True)
    _log_mem(f"[columnar] بعد از evaluate_group_finalize -> {len(winners)} برنده")

    # پاس دوم: فقط برای برندگان، ستون‌های سنگین را از part اصلی‌شان می‌خوانیم
    winners_by_part: dict[int, list[dict]] = defaultdict(list)
    for w in winners:
        winners_by_part[w["__part_idx"]].append(w)

    for part_idx, part_winners in winners_by_part.items():
        pf = part_files[part_idx]
        row_idxs = [w["__row_idx"] for w in part_winners]
        # [فیکس ریشه‌ای OOM] چون part_*.parquet با تنظیمات پیش‌فرض pandas
        # نوشته شده، هر part فقط یک row-group داره — یعنی حتی
        # pq.ParquetFile(pf).read(columns=...).take(row_idxs) هم قبل از
        # take() مجبوره کل ستون سنگین رو برای هر ۱۰۸هزار ردیف decode کنه
        # (فقط جلوی تبدیل pandas/Python object رو می‌گیره، نه خودِ اصل
        # مشکل رو). راه‌حل قطعی: با iter_batches به‌صورت stream و batch به
        # batch جلو می‌ریم؛ در هر لحظه فقط یک batch (نه کل part) در حافظه‌ست،
        # پس مصرف حافظه‌ی این مرحله دیگر به تعداد ردیف‌های part بستگی ندارد
        # و فقط به batch_size (اینجا ۲۰هزار ردیف) محدود می‌شود.
        heavy_by_row = _read_heavy_rows_streaming(pf, row_idxs)
        for w in part_winners:
            hrow = heavy_by_row[w["__row_idx"]]
            for col in _WT_MERGE_HEAVY_COLUMNS:
                val = hrow[col]
                w[col] = json.loads(val) if isinstance(val, str) else val
        heavy_by_row = None
        _log_mem(f"[columnar] بعد از خواندنِ هدفمندِ ستون‌های سنگین از {pf.name} برای {len(part_winners)} برنده")

    for w in winners:
        w.pop("__part_idx", None)
        w.pop("__row_idx", None)

    return _wt_merge_finalize_and_save(winners, total_raw, output_dir, len(part_files))


def run_whole_time_merge(
    parts_dir: Path, output_dir: Path, top_n: Optional[int], strategy: str = "full",
) -> Path:
    """فاز ۳ از ۳ (یک‌بار): همه‌ی part_*.parquet فاز ۲ را ادغام می‌کند و
    دقیقاً همان evaluate_group_finalize (که evaluate_group یک‌شکلِ خودش هم
    استفاده می‌کند) را یک‌بار روی کل مجموعه‌ی ادغام‌شده اجرا می‌کند — این‌طور
    percentile_rank/انتخاب قطعی‌یا-fallback دقیقاً همان نتیجه‌ی حالت
    تک‌جابی evaluate_group را می‌دهد، فقط ورودی‌اش از چند matrix job جمع
    شده است.

    [دیباگ کد ۱۴۳ / ایده‌ی همکار] سه راهبرد فالبک، از سبک‌ترین کد تا
    کم‌مصرف‌ترین حافظه — هر سه دقیقاً همان evaluate_group_finalize را روی
    همان مجموعه‌ی کامل صدا می‌زنند، پس خروجی هر سه از نظر ریاضی یکسان است؛
    فقط الگوی مصرف I/O و حافظه فرق می‌کند:
      - "full":       راهبرد قبلی/پیش‌فرض (بیشترین مصرف حافظه).
      - "lazy-json":  decode ستون‌های تودرتو را تا بعد از انتخاب نهایی
                       عقب می‌اندازد (مصرف حافظه‌ی خیلی کمتر، هزینه‌ی
                       اضافه‌ی ناچیز).
      - "columnar":   ستون‌های تودرتو را اصلاً برای ردیف‌های غیربرنده در
                       حافظه نمی‌آورد (کمترین مصرف حافظه، دو پاس I/O).
    اگر یک راهبرد به هر دلیلی (از جمله OOM/kill خودِ runner) کل جاب را
    ببرد، در سطح workflow جاب بعدی با راهبرد بعدی دوباره تلاش می‌کند —
    چون یک SIGTERM/SIGKILL واقعی از سمت سیستم‌عامل/زیرساخت runner را
    نمی‌شود از *داخل* همان پروسسِ در حال مرگ گرفت و ادامه داد؛ فقط جاب/
    پروسس بعدی می‌تواند دوباره تلاش کند."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _log_mem(f"شروع run_whole_time_merge (strategy={strategy})")

    part_files = _wt_merge_check_parts(parts_dir)

    strategies = {
        "full": _merge_strategy_full,
        "lazy-json": _merge_strategy_lazy_json,
        "columnar": _merge_strategy_columnar,
    }
    if strategy not in strategies:
        raise ValueError(
            f"strategy نامعتبر: {strategy!r} — باید یکی از {sorted(strategies)} باشد."
        )
    return strategies[strategy](part_files, output_dir, top_n)


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
        "--top-n", required=False, type=int, default=None,
        help="سقف تعداد سبدهای برتر روی کل استخر (سراسری، نه به‌ازای هر کوین/امضا). "
             "پیش‌فرض: بدون سقف — همه‌ی سبدهای واجدشرایط برگردانده می‌شوند.",
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
             "(بدون قید رویداد خبری، و بدون قید هم‌کوین/هم‌امضا — فقط بر اساس "
             "همبستگی پایین بازدهی) می‌سازد. خروجی در فایل جدای "
             "portfolios_whole_time.* ذخیره می‌شود.",
    )
    parser.add_argument(
        "--timeline", action="store_true", default=False,
        help="ماژول سوم: یک جدول زمانی پیوسته (از اولین تا آخرین رخداد "
             "واقعی) می‌سازد که برای هر بازه می‌گوید دقیقاً چه سبدی (یا "
             "ترکیبی از چند سبد هم‌پوشان) باید اجرا شود. [فیکس معماری] "
             "کاندیدهای این حالت دیگر از صفر ساخته نمی‌شوند؛ فقط از میان "
             "سبدهایی که portfolios_whole_time.csv از قبل واجد شرایط کرده "
             "انتخاب می‌شوند — پس --whole-time-csv الزامی است. خروجی در "
             "portfolios_timeline.* ذخیره می‌شود. با --whole-time قابل "
             "ترکیب نیست.",
    )
    parser.add_argument(
        "--whole-time-csv", required=False, type=Path, default=None,
        help="مسیر فایل portfolios_whole_time.csv (خروجیِ از پیش تولیدشده‌ی "
             "--whole-time). فقط با --timeline استفاده می‌شود و برای آن "
             "الزامی است — timeline دیگر کاندید مستقل نمی‌سازد.",
    )
    parser.add_argument(
        "--whole-time-build", action="store_true", default=False,
        help="فاز ۱ از ۳ حالت matrix-پذیرِ whole-time: تمام آرشیوها را "
             "می‌خواند و صف کامل ترکیبات (combos.json، بدون هیچ سقفی روی "
             "تعدادشان) + داده‌ی مشترک لازم برای فاز ۲ را در --output-dir "
             "می‌سازد.",
    )
    parser.add_argument(
        "--whole-time-score", action="store_true", default=False,
        help="فاز ۲ از ۳ (matrix): فقط یک برش از combos.json را امتیازدهی "
             "می‌کند. نیازمند --shared-dir (خروجی فاز ۱)، --combos-file "
             "(فایل chunk این job) و --output-file (مسیر part خروجی).",
    )
    parser.add_argument(
        "--whole-time-merge", action="store_true", default=False,
        help="فاز ۳ از ۳ (یک‌بار): همه‌ی part_*.parquet فاز ۲ را (از "
             "--parts-dir) ادغام و نرمال‌سازی/انتخاب نهایی سراسری را انجام "
             "می‌دهد؛ خروجی نهایی portfolios_whole_time.* در --output-dir.",
    )
    parser.add_argument(
        "--shared-dir", required=False, type=Path, default=None,
        help="مسیر خروجی فاز --whole-time-build (فقط برای --whole-time-score لازم است).",
    )
    parser.add_argument(
        "--combos-file", required=False, type=Path, default=None,
        help="مسیر فایل JSON شامل برشی از combos.json برای این matrix job "
             "(فقط برای --whole-time-score لازم است).",
    )
    parser.add_argument(
        "--output-file", required=False, type=Path, default=None,
        help="مسیر فایل part خروجی این matrix job (فقط برای --whole-time-score لازم است).",
    )
    parser.add_argument(
        "--parts-dir", required=False, type=Path, default=None,
        help="مسیر پوشه‌ی حاوی part_*.parquet فاز ۲ (فقط برای --whole-time-merge لازم است).",
    )
    parser.add_argument(
        "--whole-time-merge-strategy", required=False, type=str, default="full",
        choices=["full", "lazy-json", "columnar"],
        help="[دیباگ کد ۱۴۳ / فالبک] راهبرد ادغام فاز ۳: full (پیش‌فرض قبلی)، "
             "lazy-json (decode ستون‌های تودرتو را تا بعد از انتخاب نهایی "
             "عقب می‌اندازد)، یا columnar (ستون‌های تودرتو را اصلاً برای "
             "ردیف‌های غیربرنده نمی‌خواند). هر سه از نظر ریاضی خروجی یکسان "
             "تولید می‌کنند؛ فقط مصرف حافظه فرق می‌کند. فقط برای "
             "--whole-time-merge کاربرد دارد.",
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
        if args.whole_time_build:
            run_whole_time_build(
                signatures_dir=args.signatures_dir,
                golden_scores_path=args.golden_scores,
                version_schema_path=args.version_schema,
                output_dir=output_dir,
                signatures_filter=args.signatures_filter,
            )
        elif args.whole_time_score:
            if args.shared_dir is None or args.combos_file is None or args.output_file is None:
                log.error(
                    "--whole-time-score نیازمند هر سه‌ی --shared-dir، --combos-file "
                    "و --output-file است."
                )
                return 1
            run_whole_time_score(
                shared_dir=args.shared_dir,
                combos_file=args.combos_file,
                output_file=args.output_file,
            )
        elif args.whole_time_merge:
            if args.parts_dir is None:
                log.error("--whole-time-merge نیازمند --parts-dir است.")
                return 1
            run_whole_time_merge(
                parts_dir=args.parts_dir,
                output_dir=output_dir,
                top_n=args.top_n,
                strategy=args.whole_time_merge_strategy,
            )
        elif args.timeline:
            if args.whole_time_csv is None:
                log.error(
                    "--timeline بدون --whole-time-csv اجرا نمی‌شود (طبق طراحیِ "
                    "جدید، timeline کاندید مستقل نمی‌سازد و باید از "
                    "portfolios_whole_time.csv از پیش‌تولیدشده بخواند)."
                )
                return 1
            run_timeline(
                signatures_dir=args.signatures_dir,
                golden_scores_path=args.golden_scores,
                version_schema_path=args.version_schema,
                output_dir=output_dir,
                whole_time_csv=args.whole_time_csv,
                signatures_filter=args.signatures_filter,
            )
        elif args.whole_time:
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
