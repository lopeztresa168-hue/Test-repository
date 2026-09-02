"""
golden.py – ماژول ترکیب طلایی (Golden)
شناسایی بهترین استراتژی‌ها برای هر شرایط خبری خاص
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

def _save(df: pd.DataFrame, path: Path) -> Path:
    """ذخیره DataFrame به CSV."""
    csv_path = path.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    return csv_path


def _load(path: Path) -> pd.DataFrame:
    """بارگذاری CSV."""
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(csv_path)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ثابت‌ها
# ---------------------------------------------------------------------------
DEFAULT_VERSION_ID = "v1.0.0"
DEFAULT_ALPHA = 0.1
BTC_MIN_SAMPLES = 10
ALT_MIN_SAMPLES = 5
BTC_DATA_YEARS = 4  # آستانه برای تشخیص BTC/Altcoin
DEFAULT_CHUNK_SIZE = 50  # تعداد امضا در هر قطعه (chunk)


# ---------------------------------------------------------------------------
# گام ۱: بارگذاری داده‌ها
# ---------------------------------------------------------------------------

def load_signatures(signatures_dir: str) -> pd.DataFrame:
    """بارگذاری تمام فایل‌های .jsonl از دایرکتوری مشخص.

    باگ dedup fix: هر ردیف با مسیر نسبیِ فایل مبدأش (source_file) برچسب می‌خورد.
    این مسیر همان چیزی است که در completed_golden.json / all_combinations_golden.json
    استفاده می‌شود (نه «امضای خبری» سطح ردیف که در build_signature ساخته می‌شود).
    بدون این، dedup در سطح YAML هیچ‌وقت کار نمی‌کرد چون دو نوع «امضا»
    در دو سطح متفاوت (فایل در برابر رکورد) با هم مقایسه می‌شدند.
    """
    path = Path(signatures_dir)
    files = list(path.rglob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"هیچ فایل .jsonl در {signatures_dir} یافت نشد.")

    frames = []
    for f in files:
        log.info(f"بارگذاری: {f.name}")
        df = pd.read_json(f, lines=True)
        rel_path = f.relative_to(path).as_posix()
        df["source_file"] = rel_path
        frames.append(df)

    data = pd.concat(frames, ignore_index=True)
    log.info(f"تعداد کل رکوردها: {len(data):,}")
    return data


def _derive_strategy_folder_from_source(source_file) -> str:
    """بازیابی اسم پوشه استراتژی از مسیر نسبی source_file.

    ساختار مسیر (طبق analysis_fixed_batch.yml / analysis_golden.yml) همیشه:
        {MODULE}/{STRAT_I}/{SAFE_COIN}/{INTERVAL}_{MODEL}.jsonl
    است، یعنی قطعه‌ی دوم مسیر (اندیس ۱) همان اسم پوشه‌ی استراتژی است.
    فقط وقتی فراخوانی می‌شود که فیلد strategy_folder داخل خود رکورد jsonl
    خالی/موجود نباشد (مثلاً باگ قدیمی combo_10day.py که strategy_folder
    را همیشه "" ثبت می‌کرد).
    """
    if not isinstance(source_file, str) or not source_file:
        return ""
    parts = Path(source_file).as_posix().split("/")
    return parts[1] if len(parts) >= 2 else ""


def fill_missing_strategy_folder(sig_df: pd.DataFrame) -> pd.DataFrame:
    """پر کردن مقادیر خالی strategy_folder از روی source_file (در صورت وجود).

    باگ قدیمی: combo_10day.py مقدار strategy_folder را همیشه "" ثبت می‌کرد
    (پارامتر --strategy-folder هیچ‌وقت به رکوردهای JSONL منتقل نمی‌شد).
    این تابع بدون نیاز به rerun کردن backtestها یا حذف امضاها، اسم پوشه
    استراتژی را از مسیر نسبی فایل (source_file) بازسازی می‌کند.
    """
    if "strategy_folder" not in sig_df.columns or "source_file" not in sig_df.columns:
        return sig_df

    empty_mask = sig_df["strategy_folder"].isna() | (
        sig_df["strategy_folder"].astype(str).str.strip() == ""
    )
    n_empty = int(empty_mask.sum())
    if n_empty:
        derived = sig_df.loc[empty_mask, "source_file"].apply(_derive_strategy_folder_from_source)
        sig_df.loc[empty_mask, "strategy_folder"] = derived
        still_empty = int((sig_df.loc[empty_mask, "strategy_folder"].astype(str).str.strip() == "").sum())
        log.info(
            f"[FIX] {n_empty:,} رکورد با strategy_folder خالی از روی source_file بازیابی شد "
            f"({n_empty - still_empty:,} موفق، {still_empty:,} ناموفق به‌دلیل مسیر نامعتبر)."
        )
    return sig_df


def load_strategies(strategies_json: str | None) -> dict:
    """بارگذاری metadata استراتژی‌ها (اختیاری — این فایل در هیچ‌جا استفاده نمی‌شود)."""
    if not strategies_json or not Path(strategies_json).exists():
        return {}
    with open(strategies_json, encoding="utf-8") as fh:
        return json.load(fh)


def load_patterns(patterns_dir: str | None) -> pd.DataFrame:
    """بارگذاری اختیاری CSVهای آستانه/نسبت_شانس (خروجی combo_10day/combo_monthly
    که در pattern_archives/ ذخیره و استخراج شده‌اند).

    ساختار مسیر دقیقاً مثل signatures/ است: {module}/{strategy}/{coin}/{interval}_{model}.csv
    (coin با '+' جایگزین‌شده به '_' طبق SAFE_COIN در analysis_fixed_batch.yml).
    خود فایل‌های CSV هیچ ستون coin/strategy/module ندارند — این‌ها از مسیر
    فایل استخراج می‌شوند.

    فقط ردیف‌های تک‌شاخصی و دوشاخصی (تعداد_شاخص‌ها <= 2) نگه داشته می‌شوند —
    طبق تصمیم فعلی، ترکیب ۳ شاخص یا بیشتر استفاده نمی‌شود.

    اگر مسیر داده نشود یا خالی/ناموجود باشد، DataFrame خالی برمی‌گردد (بدون
    خطا) — این قابلیت کاملاً اختیاری و مکمل جریان اصلی golden است.
    """
    if not patterns_dir:
        log.info("[PATTERNS] --patterns-dir داده نشده — ستون‌های آستانه در خروجی نخواهند بود.")
        return pd.DataFrame()
    pdir = Path(patterns_dir)
    if not pdir.exists():
        log.warning(f"[PATTERNS] پوشه‌ی {pdir} یافت نشد — ستون‌های آستانه در خروجی نخواهند بود.")
        return pd.DataFrame()

    rows = []
    csv_files = list(pdir.rglob("*.csv"))
    for csv_path in csv_files:
        try:
            rel = csv_path.relative_to(pdir)
            parts = rel.parts  # (module, strategy, safe_coin, "interval_model.csv")
            if len(parts) < 4:
                continue
            module, strategy, safe_coin = parts[0], parts[1], parts[2]
            df = pd.read_csv(csv_path)
            if "تعداد_شاخص‌ها" not in df.columns:
                continue
            df = df[df["تعداد_شاخص‌ها"] <= 2].copy()
            if df.empty:
                continue
            df["module"] = module
            df["strategy_folder"] = strategy
            df["safe_coin"] = safe_coin
            # تک‌شاخصی: یک نام (مثلاً "CPI m/m"). دوشاخصی: دو نام با "|" به‌هم
            # چسبیده (مثلاً "CPI m/m|FOMC") — همان فرمتی که combo_monthly.py/
            # combo_10day.py با '|'.join(subset) تولید می‌کنند.
            df["indicator"] = df["لیست_شاخص‌ها"]
            rows.append(df)
        except Exception as e:
            log.warning(f"[PATTERNS] خطا در خواندن {csv_path}: {e} — رد می‌شود.")
            continue

    if not rows:
        log.info(f"[PATTERNS] هیچ ردیف تک/دوشاخصی معتبری در {pdir} یافت نشد.")
        return pd.DataFrame()

    result = pd.concat(rows, ignore_index=True)
    log.info(f"[PATTERNS] {len(result):,} ردیف تک/دوشاخصی از {len(csv_files)} فایل CSV بارگذاری شد.")
    return result


def load_archive_index(archive_index_path: str | None) -> dict:
    """بارگذاری نگاشت signature_path -> archive_path از فایل ایندکس (اختیاری).

    فرمت فایل باید همان signature_archive_index.json باشد که در مخزن سوم
    نگهداری می‌شود: یک شیء JSON ساده {signature_path: archive_path, ...}.
    اگر مسیر داده نشود یا فایل موجود نباشد، نگاشت خالی برگردانده می‌شود و
    ستون archive_path در خروجی همه‌جا None خواهد بود (بدون خطا).
    """
    if not archive_index_path:
        log.info("[ARCHIVE-INDEX] --archive-index داده نشده — ستون archive_path خالی (None) خواهد بود.")
        return {}
    idx_path = Path(archive_index_path)
    if not idx_path.exists():
        log.warning(f"[ARCHIVE-INDEX] فایل {idx_path} یافت نشد — ستون archive_path خالی (None) خواهد بود.")
        return {}
    try:
        with open(idx_path, encoding="utf-8") as fh:
            mapping = json.load(fh)
        if not isinstance(mapping, dict):
            log.warning(f"[ARCHIVE-INDEX] محتوای {idx_path} یک dict نیست — نادیده گرفته شد.")
            return {}
        log.info(f"[ARCHIVE-INDEX] {len(mapping):,} نگاشت signature_path -> archive_path از {idx_path} بارگذاری شد.")
        return mapping
    except Exception as e:
        log.warning(f"[ARCHIVE-INDEX] خطا در خواندن {idx_path}: {e} — ستون archive_path خالی (None) خواهد بود.")
        return {}


def load_version_schema(version_schema: str | None) -> str:
    """استخراج version_id از فایل schema یا مقدار پیش‌فرض."""
    if not version_schema or not Path(version_schema).exists():
        return DEFAULT_VERSION_ID
    with open(version_schema, encoding="utf-8") as fh:
        schema = json.load(fh)
    return schema.get("version_id", DEFAULT_VERSION_ID)




# ---------------------------------------------------------------------------
# ساخت امضای خبری
# ---------------------------------------------------------------------------

def build_signature(row: pd.Series) -> str:
    """ساخت امضای خبری از فیلدهای یک رکورد."""
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
    # [فیکس ۹] سشن معاملاتی (مثل london) باید بخشی از امضا باشد، وگرنه
    # «استراتژی A همیشه» و «استراتژی A فقط سشن لندن» یک ترکیب یکسان حساب
    # می‌شوند و در golden.py با هم قاطی/میانگین‌گیری می‌شوند.
    session = row.get("session")
    if session is None or (isinstance(session, float) and pd.isna(session)) or session == "":
        session = "none"
    return f"{coin}_{indicator}_{position}_{distance}_{model}_{session}_{regime}"


# ---------------------------------------------------------------------------
# گام ۲: محاسبه معیارهای خام
# ---------------------------------------------------------------------------

def _representative_value(series: pd.Series):
    """انتخاب رایج‌ترین مقدار یک سری (در صورت تساوی، اولین مقدار).

    برای انتخاب signature_path نماینده از بین چند source_file که ممکن است
    به یک signature یکسان ختم شوند، استفاده می‌شود.
    """
    clean = series.dropna()
    if clean.empty:
        return None
    counts = clean.value_counts()
    return counts.index[0]


def consecutive_losses(returns: list[float]) -> int:
    """محاسبه حداکثر تعداد ضررهای متوالی."""
    max_consec = 0
    current = 0
    for r in returns:
        if r <= 0:
            current += 1
            max_consec = max(max_consec, current)
        else:
            current = 0
    return max_consec


def compute_raw_metrics(group: pd.DataFrame) -> dict:
    """محاسبه معیارهای خام برای یک گروه (استراتژی, امضا)."""
    returns = group["total_return"].tolist()
    n = len(returns)

    positive = [r for r in returns if r > 0]
    negative = [r for r in returns if r <= 0]

    win_rate = (len(positive) / n * 100) if n > 0 else 0.0

    sum_neg = abs(sum(negative))
    if sum_neg == 0:
        profit_factor = None  # all-win – بعداً جایگزین می‌شود
    else:
        profit_factor = sum(positive) / sum_neg if positive else 0.0

    max_loss_ratio = consecutive_losses(returns) / n if n > 0 else 0.0

    avg_daily_return = group["avg_daily_return"].mean() if "avg_daily_return" in group.columns else 0.0

    # [فیکس ۱۳] بازه‌ی زمانی بک‌تست این ترکیب: اولین و آخرین تاریخ دوره‌ای که
    # این signature در آن معامله داشته، به‌علاوه‌ی مجموع طول واقعی دوره‌هایی
    # که توشون معامله ثبت شده (نه کل فاصله‌ی تقویمی — چون بین دوره‌ها ممکن
    # است شکاف باشد، مثلاً ماه‌هایی با کمتر از min_sample_count معامله که
    # اصلاً به JSONL نرسیده‌اند).
    period_start_min = None
    period_end_max = None
    active_days_sum = 0
    if "period_start" in group.columns and "period_end" in group.columns:
        starts = pd.to_datetime(group["period_start"], errors="coerce").dropna()
        ends = pd.to_datetime(group["period_end"], errors="coerce").dropna()
        if not starts.empty:
            period_start_min = starts.min().date().isoformat()
        if not ends.empty:
            period_end_max = ends.max().date().isoformat()
    if "period_length_days" in group.columns:
        active_days_sum = int(pd.to_numeric(group["period_length_days"], errors="coerce").fillna(0).sum())

    # میانگین شدت «تعجب خبری» (|actual-forecast|) شاخص غالب این گروه — از
    # dominant_indicator_importance که در JSONL خام موجود است ولی قبلاً اینجا
    # هیچ‌وقت استفاده نمی‌شد. مقادیر None (رویدادهای هنوز-منتشرنشده، مثلاً
    # دوره‌های pre-release) نادیده گرفته می‌شوند؛ اگر هیچ مقدار معتبری نبود،
    # NaN می‌ماند (بعداً در normalize_metrics خنثی/neutral پر می‌شود).
    if "dominant_indicator_importance" in group.columns:
        imp_vals = pd.to_numeric(group["dominant_indicator_importance"], errors="coerce").dropna()
        avg_indicator_importance = float(imp_vals.mean()) if len(imp_vals) > 0 else None
    else:
        avg_indicator_importance = None

    # signature_path: مسیر نسبی فایل JSONL نماینده (رایج‌ترین source_file در گروه).
    # یک گروه (strategy_id, coin, signature) ممکن است از چند فایل JSONL تشکیل شده
    # باشد (چون چند فایل می‌توانند به یک امضای خبری یکسان ختم شوند)، بنابراین
    # رایج‌ترین source_file به‌عنوان نماینده انتخاب می‌شود.
    signature_path = (
        _representative_value(group["source_file"]) if "source_file" in group.columns else None
    )

    # برای تطبیق بعدی با CSV آستانه/نسبت_شانس (load_patterns) — بدون این،
    # تنها راه به‌دست‌آوردن شاخص، parse کردن رشته‌ی signature بود که چون
    # coin/model هم می‌توانند '_' داشته باشند، غیرقابل‌اطمینان است.
    dominant_indicator = (
        _representative_value(group["dominant_indicator"]) if "dominant_indicator" in group.columns else None
    )

    # [فیکس: پشتیبانی ترکیب دوشاخصی] برای اینکه بعداً بشود CSVهای الگوی
    # دوشاخصی (patterns_df با تعداد_شاخص‌ها==2) را هم به این گروه مچ کرد،
    # اتحاد تمام شاخص‌هایی که در طول همه‌ی دوره‌های این گروه به‌عنوان
    # dominant_indicator یا در secondary_indicators دیده شده‌اند نگه داشته
    # می‌شود. صرفاً dominant_indicator (تک‌شاخصی) برای تشخیص یک *جفت* شاخص
    # کافی نیست.
    all_indicators_seen = set()
    if "dominant_indicator" in group.columns:
        all_indicators_seen.update(x for x in group["dominant_indicator"].dropna().tolist() if x)
    if "secondary_indicators" in group.columns:
        for lst in group["secondary_indicators"].dropna():
            if isinstance(lst, (list, tuple, set)):
                all_indicators_seen.update(lst)

    return {
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "max_loss_ratio": max_loss_ratio,
        "avg_daily_return": avg_daily_return,
        "avg_indicator_importance": avg_indicator_importance,
        "dominant_indicator": dominant_indicator,
        "all_indicators_seen": sorted(all_indicators_seen),
        "sample_count": n,
        "total_return_sum": sum(returns),
        "signature_path": signature_path,
        "بازه_زمانی_شروع": period_start_min,
        "بازه_زمانی_پایان": period_end_max,
        "تعداد_روز_فعال": active_days_sum,
    }


def resolve_all_win_pf(raw_df: pd.DataFrame) -> pd.DataFrame:
    """جایگزینی profit_factor=None (all-win) بر اساس total_return گروه.

    - اگر total_return > 1: گروه معتبر است → ضریب 1.5 * max_valid_pf
    - اگر total_return <= 1: سود ناچیز است → ضریب پایین (1.0 * max_valid_pf یا کمتر)
    """
    valid_pf = raw_df.loc[raw_df["profit_factor"].notna(), "profit_factor"]
    max_valid_pf = valid_pf.max() if not valid_pf.empty else 1.0

    all_win_mask = raw_df["profit_factor"].isna()

    def replacement_for(total_return: float) -> float:
        if total_return is not None and total_return > 1:
            return max_valid_pf * 1.5
        return max_valid_pf * 1.0

    raw_df.loc[all_win_mask, "profit_factor"] = raw_df.loc[all_win_mask, "total_return_sum"].apply(
        replacement_for
    )
    return raw_df


# ---------------------------------------------------------------------------
# گام ۳: فیلتر کم‌تکرار
# ---------------------------------------------------------------------------

def is_btc_like(coin: str) -> bool:
    """تشخیص اینکه ارز به اندازه کافی تاریخچه دارد یا خیر."""
    return "BTC" in coin.upper()


def filter_low_sample(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """حذف گروه‌های با تکرار کم."""
    initial = len(raw_df)

    def min_samples(coin: str) -> int:
        return BTC_MIN_SAMPLES if is_btc_like(coin) else ALT_MIN_SAMPLES

    mask = raw_df.apply(lambda r: r["sample_count"] >= min_samples(r["coin_composition"]), axis=1)
    filtered = raw_df[mask].copy()
    removed = initial - len(filtered)
    return filtered, removed


# ---------------------------------------------------------------------------
# گام ۴: نرمال‌سازی با Percentile Rank
# ---------------------------------------------------------------------------

def percentile_rank(series: pd.Series) -> pd.Series:
    """محاسبه Percentile Rank برای یک سری."""
    ranks = series.rank(method="average", pct=True) * 100
    return ranks


def normalize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """نرمال‌سازی معیارها به تفکیک coin_composition."""
    df = df.copy()

    for metric in ["win_rate", "profit_factor", "avg_daily_return"]:
        norm_col = f"{metric}_norm"
        df[norm_col] = df.groupby("coin_composition")[metric].transform(percentile_rank)

    # max_loss_ratio: هر چه کمتر، بهتر – قبل از نرمال‌سازی معکوس می‌شود
    df["max_loss_inv"] = 100 - (df["max_loss_ratio"] * 100)
    df["max_loss_ratio_norm"] = df.groupby("coin_composition")["max_loss_inv"].transform(percentile_rank)
    df.drop(columns=["max_loss_inv"], inplace=True)

    # شدت شاخص خبری: هر چه تعجب (|actual-forecast|) بزرگ‌تر، سیگنال قوی‌تر
    # فرض می‌شود. ردیف‌هایی که مقدار معتبر ندارند (مثلاً چون رویداد anchor
    # هنوز منتشر نشده) با ۵۰ (خنثی، نه امتیاز مثبت نه منفی) پر می‌شوند تا
    # امتیاز نهایی‌شان به‌ناحق پایین/بالا نیاید.
    if "avg_indicator_importance" in df.columns:
        df["indicator_importance_norm"] = df.groupby("coin_composition")["avg_indicator_importance"].transform(percentile_rank)
        df["indicator_importance_norm"] = df["indicator_importance_norm"].fillna(50.0)
    else:
        df["indicator_importance_norm"] = 50.0

    return df


# ---------------------------------------------------------------------------
# گام ۵: محاسبه امتیاز نهایی
# ---------------------------------------------------------------------------

def compute_score(row: pd.Series) -> float:
    """محاسبه امتیاز نهایی با وزن‌های ثابت.

    وزن جدید indicator_importance_norm (شدت تعجب خبری شاخص غالب) اضافه شد؛
    بقیه‌ی وزن‌ها متناسب کم شدند تا مجموع همچنان ۱.۰ بماند
    (۰.۳۰→۰.۲۸، ۰.۳۰→۰.۲۸، ۰.۲۰→۰.۱۸، ۰.۲۰→۰.۱۶، + ۰.۱۰ برای شدت شاخص).
    """
    score = (
        0.28 * row["win_rate_norm"]
        + 0.28 * row["profit_factor_norm"]
        + 0.18 * (100 - row["max_loss_ratio_norm"])
        + 0.16 * row["avg_daily_return_norm"]
        + 0.10 * row.get("indicator_importance_norm", 50.0)
    )
    return round(float(score), 4)


# ---------------------------------------------------------------------------
# مدیریت وضعیت (Status) برای وقفه و ادامه (Resume)
# ---------------------------------------------------------------------------

def load_status(status_path: Path) -> dict | None:
    """بارگذاری فایل وضعیت در صورت وجود."""
    if not status_path.exists():
        return None
    try:
        with open(status_path, encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning(f"خواندن فایل وضعیت ناموفق بود ({exc}) – نادیده گرفته شد.")
        return None


def save_status(
    status_path: Path,
    processed_signatures,
    last_chunk_index: int,
    total_chunks: int,
    status: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    processed_files=None,
) -> dict:
    """ذخیره فایل وضعیت golden_status.json.

    باگ dedup fix: علاوه بر processed_signatures (امضای سطح ردیف، برای resume داخلی)،
    processed_files (مسیر نسبی فایل‌های .jsonl پردازش‌شده) هم ذخیره می‌شود.
    این لیست دوم همان چیزی است که باید با completed_golden.json در YAML مقایسه شود.
    """
    payload = {
        "processed_signatures": sorted(set(processed_signatures)),
        "processed_files": sorted(set(processed_files)) if processed_files is not None else [],
        "last_chunk_index": last_chunk_index,
        "total_chunks": total_chunks,
        "chunk_size": chunk_size,
        "status": status,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    with open(status_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return payload


def check_interrupt(interrupt_flag_path: Path | None) -> bool:
    """بررسی وجود فایل interrupt.flag."""
    if interrupt_flag_path is None:
        return False
    return Path(interrupt_flag_path).exists()


def chunk_list(items: list, chunk_size: int) -> list:
    """تقسیم یک لیست به قطعات با اندازه‌ی ثابت."""
    if chunk_size <= 0:
        chunk_size = DEFAULT_CHUNK_SIZE
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def write_summary(
    output_path: Path,
    status: str,
    total_signatures: int,
    processed_signatures_count: int,
    total_groups: int,
    filtered_groups: int,
    output_files: list,
    version_id: str,
    completed_at: datetime,
) -> Path:
    """نوشتن خروجی خلاصه golden_summary.json."""
    summary = {
        "status": status,
        "total_signatures": total_signatures,
        "processed_signatures": processed_signatures_count,
        "total_groups": total_groups,
        "filtered_groups": filtered_groups,
        "output_files": output_files,
        "version_id": version_id,
        "completed_at": completed_at.isoformat(),
    }
    summary_path = output_path / "golden_summary.json"
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    log.info(f"golden_summary ذخیره شد: {summary_path}")
    return summary_path


# ---------------------------------------------------------------------------
# پردازش اصلی
# ---------------------------------------------------------------------------

def merge_pattern_thresholds(recommendation_df: pd.DataFrame, patterns_df: pd.DataFrame) -> pd.DataFrame:
    """ادغام ستون‌های نسبت_شانس_آستانه_* (تک‌شاخصی + دوشاخصی) از patterns_df
    به recommendation_df.

    [فیکس ۱] قبلاً pivot_table با aggfunc="first" روی (indicator, آستانه)
    بدون "الگوی_وضعیت" در index، ۲ از هر ۳ مقدار (Good/Bad/Neutral) را
    بی‌صدا دور می‌ریخت. راه‌حل درست ادغام بر اساس وضعیت واقعیِ آن سیگنال نیست،
    چون آن وضعیت اصلاً در سطح norm_df/recommendation_df ثبت نشده (فقط
    dominant_indicator نگه داشته می‌شود، نه اینکه در آن دوره وضعیتش Good بوده
    یا Bad). پس به‌جای حدس زدن یک وضعیت، هر سه وضعیت به‌صورت ستون‌های جدا
    نگه داشته می‌شوند: نسبت_شانس_آستانه_{thr}_{Good|Bad|Neutral}

    [فیکس ۲] برای ردیف‌های دوشاخصی (indicator = "A|B")، مچ‌کردن مستقیم با
    dominant_indicator (که همیشه تک‌شاخصی است) امکان‌پذیر نیست. به‌جایش، چک
    می‌شود که آیا هر دو شاخص A و B در all_indicators_seen آن گروه (اتحاد
    dominant_indicator + secondary_indicators در تمام دوره‌های آن گروه)
    حضور داشته‌اند یا نه.
    """
    recommendation_df = recommendation_df.copy()
    if patterns_df.empty:
        log.info("[PATTERNS] patterns_df خالی است — ستون‌های آستانه اضافه نمی‌شوند.")
        return recommendation_df

    recommendation_df["_safe_coin"] = (
        recommendation_df["coin_composition"].astype(str).str.replace("+", "_", regex=False)
    )

    # نگاشت سریع: (strategy_folder, safe_coin) -> لیستی از رکوردهای پترن
    pattern_lookup = defaultdict(list)
    for _, prow in patterns_df.iterrows():
        key = (prow["strategy_folder"], prow["safe_coin"])
        indicator_names = tuple(str(prow["indicator"]).split("|"))
        pattern_lookup[key].append({
            "indicators": indicator_names,
            "threshold": prow["آستانه"],
            "status": prow["الگوی_وضعیت"],
            "odds": prow["نسبت_شانس"],
        })

    def _match_row(row):
        key = (row["strategy_id"], row["_safe_coin"])
        candidates = pattern_lookup.get(key)
        if not candidates:
            return {}
        dom = row["dominant_indicator"]
        seen = set(row["all_indicators_seen"]) if isinstance(row["all_indicators_seen"], (list, tuple, set)) else set()
        out = {}
        for cand in candidates:
            inds = cand["indicators"]
            if len(inds) == 1:
                if inds[0] != dom:
                    continue
            else:
                if not set(inds).issubset(seen):
                    continue
            col = f"نسبت_شانس_آستانه_{cand['threshold']}_{cand['status']}"
            # اگر چند رکورد پترن (مثلاً از فایل‌های تکه‌ای مختلف) به یک ستون
            # برسند، اولین مقدار نگه داشته می‌شود (به‌ندرت رخ می‌دهد).
            if col not in out:
                out[col] = cand["odds"]
        return out

    matched_series = recommendation_df.apply(_match_row, axis=1)
    matched_df = pd.DataFrame(list(matched_series), index=recommendation_df.index)
    recommendation_df = pd.concat([recommendation_df, matched_df], axis=1)
    recommendation_df.drop(columns=["_safe_coin"], errors="ignore", inplace=True)

    thr_cols = [c for c in recommendation_df.columns if c.startswith("نسبت_شانس_آستانه_")]
    matched = recommendation_df[thr_cols].notna().any(axis=1).sum() if thr_cols else 0
    log.info(f"[PATTERNS] {matched:,} از {len(recommendation_df):,} ردیف پیشنهاد با آستانه/نسبت_شانس تطبیق یافتند.")
    return recommendation_df


def run(
    signatures_dir: str,
    strategies_json: str | None,
    version_schema: str | None,
    ohlc_dir: str | None,
    output_dir: str,
    alpha: float,
    status_file: str | None = None,
    resume: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    interrupt_flag: str | None = None,
    archive_index: str | None = None,
    patterns_dir: str | None = None,
) -> None:
    now_utc = datetime.now(timezone.utc)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    status_path = Path(status_file) if status_file else output_path / "golden_status.json"
    interrupt_path = Path(interrupt_flag) if interrupt_flag else output_path / "interrupt.flag"

    # ── بارگذاری ──────────────────────────────────────────────────────────
    log.info("بارگذاری signatures...")
    sig_df = load_signatures(signatures_dir)
    sig_df = fill_missing_strategy_folder(sig_df)

    # ── بارگذاری ایندکس archive_path (اختیاری) ──────────────────────────
    sig_to_archive = load_archive_index(archive_index)

    # ── بارگذاری CSV آستانه/نسبت_شانس (اختیاری) ──────────────────────────
    patterns_df = load_patterns(patterns_dir)

    version_id = load_version_schema(version_schema)
    log.info(f"version_id: {version_id}")

    # ── ساخت امضا ─────────────────────────────────────────────────────────
    log.info("ساخت امضاهای خبری...")
    sig_df["signature"] = sig_df.apply(build_signature, axis=1)

    unique_signatures = sorted(sig_df["signature"].unique().tolist())
    total_signatures = len(unique_signatures)

    # باگ dedup fix: نگاشت هر فایل مبدأ به مجموعه‌ی امضاهای داخل آن، تا بدانیم
    # یک فایل JSONL کِی "کامل" پردازش شده (یعنی همه‌ی امضاهایش در چانک‌های
    # پردازش‌شده هستند) و آن را به completed_golden.json اضافه کنیم.
    file_to_signatures: dict[str, set] = {}
    for src_file, sub_df in sig_df.groupby("source_file"):
        file_to_signatures[src_file] = set(sub_df["signature"].unique().tolist())
    total_files = len(file_to_signatures)
    log.info(f"تعداد فایل‌های .jsonl منبع: {total_files:,}")

    # ── گام ۳: تقسیم به قطعات ────────────────────────────────────────────
    chunks = chunk_list(unique_signatures, chunk_size)
    total_chunks = len(chunks)
    log.info(
        f"تعداد امضاهای منحصربه‌فرد: {total_signatures:,} | "
        f"اندازه قطعه: {chunk_size} | تعداد قطعات: {total_chunks:,}"
    )

    processed_signatures: set = set()
    processed_files: set = set()
    start_chunk = 0
    raw_records: list = []

    # ── گام ۰: بارگذاری وضعیت قبلی (در صورت --resume) ───────────────────
    if resume:
        prev_status = load_status(status_path)
        if prev_status and prev_status.get("status") in ("interrupted", "running"):
            processed_signatures = set(prev_status.get("processed_signatures", []))
            processed_files = set(prev_status.get("processed_files", []))
            start_chunk = prev_status.get("last_chunk_index", -1) + 1
            log.info(
                f"ادامه از وضعیت قبلی: شروع از قطعه {start_chunk}/{total_chunks} | "
                f"امضاهای پردازش‌شده قبلی: {len(processed_signatures):,} | "
                f"فایل‌های کامل قبلی: {len(processed_files):,}"
            )
            try:
                existing_raw = _load(output_path / "golden_raw.csv")
                raw_records = existing_raw.to_dict("records")
                log.info(f"رکوردهای خام قبلی بارگذاری شد: {len(raw_records):,}")
            except FileNotFoundError:
                log.info("فایل golden_raw قبلی یافت نشد – ادامه با رکوردهای خالی.")
        else:
            log.info("هیچ وضعیت قابل‌ادامه‌ای (running/interrupted) یافت نشد. شروع از ابتدا.")

    if total_chunks == 0:
        log.warning("هیچ امضایی برای پردازش یافت نشد.")
        save_status(status_path, processed_signatures, -1, 0, "completed", chunk_size, processed_files)
        write_summary(output_path, "completed", 0, 0, 0, 0, [], version_id, now_utc)
        return

    # ── گام ۴: پردازش هر قطعه ────────────────────────────────────────────
    interrupted = False
    for chunk_idx in range(start_chunk, total_chunks):
        if check_interrupt(interrupt_path):
            log.warning(f"interrupt.flag یافت شد - توقف پردازش قبل از قطعه {chunk_idx}.")
            raw_df_partial = pd.DataFrame(raw_records)
            if not raw_df_partial.empty:
                _save(raw_df_partial, output_path / "golden_raw.csv")
            save_status(status_path, processed_signatures, chunk_idx - 1, total_chunks, "interrupted", chunk_size, processed_files)
            interrupted = True
            break

        chunk_signatures = set(chunks[chunk_idx])
        chunk_df = sig_df[sig_df["signature"].isin(chunk_signatures)]

        groups = chunk_df.groupby(["strategy_folder", "coin_composition", "signature"])
        for (strategy_id, coin, signature), group_df in groups:
            metrics = compute_raw_metrics(group_df)
            raw_records.append(
                {
                    "strategy_id": strategy_id,
                    "coin_composition": coin,
                    "signature": signature,
                    **metrics,
                    "created_at": now_utc,
                }
            )

        processed_signatures.update(chunk_signatures)

        # باگ dedup fix: یک فایل را "کامل" علامت بزن فقط وقتی همه‌ی
        # امضاهای آن فایل دیگر در processed_signatures باشند.
        for src_file, sigs in file_to_signatures.items():
            if src_file in processed_files:
                continue
            if sigs.issubset(processed_signatures):
                processed_files.add(src_file)

        raw_df_partial = pd.DataFrame(raw_records)
        _save(raw_df_partial, output_path / "golden_raw.csv")
        save_status(status_path, processed_signatures, chunk_idx, total_chunks, "running", chunk_size, processed_files)
        log.info(
            f"قطعه {chunk_idx + 1}/{total_chunks} پردازش شد ({len(chunk_signatures)} امضا) | "
            f"فایل‌های کامل تاکنون: {len(processed_files):,}/{total_files:,}"
        )

    if interrupted:
        log.info("پردازش به دلیل interrupt.flag متوقف شد (خروج graceful با کد ۰).")
        return

    # ── حذف رکوردهای تکراری احتمالی ناشی از resume‌های قبلی ──────────────
    raw_df = pd.DataFrame(raw_records)
    if not raw_df.empty:
        raw_df = raw_df.drop_duplicates(
            subset=["strategy_id", "coin_composition", "signature"], keep="last"
        ).reset_index(drop=True)
    total_groups = len(raw_df)

    if raw_df.empty:
        log.warning("هیچ داده‌ای برای پردازش یافت نشد.")
        save_status(status_path, processed_signatures, total_chunks - 1, total_chunks, "completed", chunk_size, processed_files)
        write_summary(output_path, "completed", total_signatures, len(processed_signatures), 0, 0, [], version_id, now_utc)
        return

    # ── مدیریت all-win ────────────────────────────────────────────────────
    raw_df = resolve_all_win_pf(raw_df)

    # ── ذخیره golden_raw.csv ──────────────────────────────────────────
    raw_out = _save(raw_df, output_path / "golden_raw.csv")
    log.info(f"golden_raw ذخیره شد: {raw_out}")

    # ── حذف فیلتر کم‌تکرار: استفاده مستقیم از داده‌های خام ─────────────────
    log.info("فیلتر کم‌تکرار غیرفعال است – استفاده مستقیم از raw_df.")
    filtered_df = raw_df
    removed_count = 0
    log.info(f"گروه‌های باقیمانده: {len(filtered_df):,}")

    if filtered_df.empty:
        log.warning("هیچ گروه معتبری پس از فیلتر باقی نماند. خروج.")
        save_status(status_path, processed_signatures, total_chunks - 1, total_chunks, "completed", chunk_size, processed_files)
        write_summary(
            output_path, "completed", total_signatures, len(processed_signatures),
            total_groups, 0, [raw_out.name], version_id, now_utc,
        )
        return

    # ── نرمال‌سازی ────────────────────────────────────────────────────────
    log.info("نرمال‌سازی معیارها با Percentile Rank...")
    norm_df = normalize_metrics(filtered_df)

    # ── امتیاز نهایی ──────────────────────────────────────────────────────
    log.info("محاسبه امتیاز نهایی...")
    norm_df["score"] = norm_df.apply(compute_score, axis=1)

    # ── آماده‌سازی golden_scores.csv ──────────────────────────────────
    # هیچ ادغامی صورت نمی‌گیرد: هر (strategy_id, signature, coin_composition)
    # به‌عنوان یک ردیف مجزا در خروجی باقی می‌ماند تا عملکرد هر ترکیب کوین
    # به‌طور مستقل قابل بررسی باشد.
    # signature_path ممکن است در برخی مسیرهای قدیمی وجود نداشته باشد؛
    # برای سازگاری با عقب، در صورت نبود، ستون خالی (None) ساخته می‌شود.
    if "signature_path" not in norm_df.columns:
        norm_df["signature_path"] = None

    scores_out_df = norm_df[
        ["strategy_id", "coin_composition", "signature_path", "signature", "score", "sample_count"]
    ].copy()
    scores_out_df["version_id"] = version_id
    scores_out_df["calculated_at"] = now_utc

    # ── پر کردن archive_path از ایندکس (در صورت وجود) ────────────────────
    # این ستون همان چیزی است که build_all_queues.py قبلاً به‌صورت fallback
    # از signature_archive_index.json می‌خواند؛ حالا مستقیماً همینجا در
    # خودِ golden_scores.csv نوشته می‌شود تا نیازی به fallback runtime نباشد.
    scores_out_df["archive_path"] = scores_out_df["signature_path"].map(sig_to_archive)
    matched_archive_count = int(scores_out_df["archive_path"].notna().sum())
    missing_archive_count = len(scores_out_df) - matched_archive_count
    log.info(
        f"[ARCHIVE-INDEX] archive_path پر شد برای {matched_archive_count:,} ردیف | "
        f"یافت نشد برای {missing_archive_count:,} ردیف از {len(scores_out_df):,}"
    )
    if sig_to_archive and missing_archive_count:
        log.warning(
            f"[ARCHIVE-INDEX] {missing_archive_count:,} ردیف signature_path‌ای دارند که در "
            "ایندکس archive_index یافت نشد — archive_path=None برای این ردیف‌ها ثبت می‌شود."
        )

    scores_out = _save(scores_out_df, output_path / "golden_scores.csv")
    log.info(f"golden_scores ذخیره شد: {scores_out}")

    # ── تولید خروجی‌های پیشنهاد استراتژی (الگوی خبری و رژیم بازار) ──────────
    # از norm_df استفاده می‌شود چون همان ردیف‌های scores_out_df را دارد، اما
    # علاوه بر آن win_rate و avg_daily_return را هم در خود نگه داشته است
    # (این دو ستون به‌عمد به golden_scores.csv اضافه نشدند تا فرمت آن فایل،
    # که در بخش‌های دیگر pipeline استفاده می‌شود، دست‌نخورده بماند).
    recommendation_df = norm_df[
        ["strategy_id", "coin_composition", "signature", "sample_count", "win_rate", "avg_daily_return",
         "dominant_indicator", "all_indicators_seen",
         "بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال"]
    ].copy()

    # ── ادغام اختیاری آستانه/نسبت_شانس (تک‌شاخصی + دوشاخصی) از patterns_df ──
    # فقط به دو خروجی پیشنهاد استراتژی اضافه می‌شود، نه golden_scores.csv
    # (فرمت golden_scores.csv دست‌نخورده می‌ماند).
    recommendation_df = merge_pattern_thresholds(recommendation_df, patterns_df)
    recommendation_df.drop(columns=["all_indicators_seen"], errors="ignore", inplace=True)

    generate_news_pattern_recommendation(recommendation_df, output_path)
    generate_news_pattern_no_regime_recommendation(recommendation_df, output_path)
    generate_market_regime_recommendation(recommendation_df, output_path)

    # ── وضعیت نهایی: completed ───────────────────────────────────────────
    save_status(status_path, processed_signatures, total_chunks - 1, total_chunks, "completed", chunk_size, processed_files)

    # ── خروجی خلاصه golden_summary.json ──────────────────────────────────
    write_summary(
        output_path,
        "completed",
        total_signatures,
        len(processed_signatures),
        total_groups,
        len(filtered_df),
        [raw_out.name, scores_out.name],
        version_id,
        now_utc,
    )

    # ── گزارش نهایی ───────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info(f"تعداد گروه‌های پردازش‌شده:  {len(filtered_df):,}")
    log.info(f"تعداد گروه‌های حذف‌شده:     {removed_count:,}")
    log.info(f"تعداد ردیف‌های نهایی امتیاز: {len(scores_out_df):,}")
    log.info(f"خروجی‌ها در: {output_path.resolve()}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# خروجی‌های پیشنهاد استراتژی (افزایشی) – بر اساس الگوی خبری و رژیم بازار
# ---------------------------------------------------------------------------
#
# نکته پیاده‌سازی: طبق پرامپت، این توابع باید از scores_out_df استفاده کنند،
# اما scores_out_df ستون‌های win_rate و avg_daily_return را ندارد (فقط شامل
# strategy_id, coin_composition, signature_path, signature, score, sample_count
# است). برای اینکه ستون‌های اضافه به golden_scores.csv تحمیل نشود (و فرمت آن
# فایل که در جاهای دیگر pipeline استفاده می‌شود دست‌نخورده بماند)، این دو تابع
# با یک دیتافریم کمکی («recommendation_df») که از همان norm_df استخراج شده و
# دقیقاً همان ردیف‌های scores_out_df را با ستون‌های win_rate/avg_daily_return
# اضافه در بر می‌گیرد، فراخوانی می‌شوند. این دیتافریم در تابع run ساخته می‌شود.

RECOMMENDATION_GROUP_COLUMNS = [
    "گروه", "رتبه", "نام_استراتژی", "تعداد_تکرار",
    "تعداد_سود", "تعداد_ضرر", "وین‌ریت_(%)", "میانگین_بازده_(%)",
]


def _threshold_columns(df: pd.DataFrame) -> list[str]:
    """ستون‌های نسبت_شانس_آستانه_* که (در صورت وجود patterns_df) توسط run()
    به recommendation_df اضافه شده‌اند. اگر patterns داده نشده باشد، لیست
    خالی است و رفتار قبلی (بدون ستون آستانه) کاملاً حفظ می‌شود."""
    return sorted(c for c in df.columns if c.startswith("نسبت_شانس_آستانه_"))


def _strip_market_regime(signature) -> str:
    """[فیکس ۹/۱۱] معکوس _extract_market_regime: امضا را بدون پسوند رژیم
    برمی‌گرداند (یعنی coin_indicator_position_distance_model_session)."""
    if not isinstance(signature, str) or not signature:
        return ""
    for regime in sorted(KNOWN_MARKET_REGIMES, key=len, reverse=True):
        suffix = "_" + regime
        if signature.endswith(suffix):
            return signature[: -len(suffix)]
        if signature == regime:
            return ""
    return signature


def _prepare_group_aggregates(df: pd.DataFrame, group_col: str, include_thr_cols: bool = True) -> pd.DataFrame:
    """تجمیع رکوردها بر اساس (گروه، نام_استراتژی) و محاسبه معیارهای خام.

    اگر ستون «نام_استراتژی» از قبل در df موجود باشد، همان استفاده می‌شود
    (این حالت را generate_market_regime_recommendation برای [فیکس ۱۱] به‌کار
    می‌برد تا هر ردیف دقیقاً یک ترکیب یکتا بماند). در غیر این صورت، مثل قبل
    از "{strategy_id} ({coin_composition})" ساخته می‌شود — این حالت پیش‌فرض
    برای «الگوی خبری» کاملاً کافی است چون group_col آنجا خودِ signature
    یکتاست و هیچ ادغام اضافه‌ای رخ نمی‌دهد.

    تعداد_سود و تعداد_ضرر از sample_count و win_rate (وزن‌دهی‌شده) تخمین زده
    می‌شوند، چون scores_out_df/norm_df شمارش خام سود/ضرر را نگه نمی‌دارد.

    [فیکس ۸] ستون‌های نسبت_شانس_آستانه_* فقط وقتی include_thr_cols=True باشد
    محاسبه/نگه داشته می‌شوند. برای گروه‌بندی «الگوی خبری» (group_col=
    گروه_خبری=signature) این درست است چون هر گروه دقیقاً یک ترکیب شاخص
    یکتاست، پس این ستون‌ها هرگز بین دو ترکیب متفاوت میانگین‌گیری نمی‌شوند. اما
    برای «رژیم بازار» (group_col=رژیم_بازار)، include_thr_cols=False پاس داده
    می‌شود.
    """
    work = df.copy()
    if "نام_استراتژی" not in work.columns:
        work["نام_استراتژی"] = work["strategy_id"].astype(str) + " (" + work["coin_composition"].astype(str) + ")"
    work["sample_count"] = work["sample_count"].fillna(0)
    work["_win_raw"] = work["sample_count"] * (work["win_rate"].fillna(0) / 100.0)
    work["_return_weighted"] = work["sample_count"] * work["avg_daily_return"].fillna(0)

    thr_cols = _threshold_columns(work) if include_thr_cols else []
    thr_weighted_cols = []
    for tc in thr_cols:
        wcol = f"_w_{tc}"
        work[wcol] = work["sample_count"] * work[tc].fillna(0)
        thr_weighted_cols.append(wcol)

    agg_spec = dict(
        تعداد_تکرار=("sample_count", "sum"),
        _win_raw_sum=("_win_raw", "sum"),
        _return_weighted_sum=("_return_weighted", "sum"),
    )
    # [فیکس ۱۳] بازه‌ی زمانی بک‌تست: min/max رشته‌های تاریخ ISO (YYYY-MM-DD)
    # به‌درستی lexicographically هم‌ترازِ ترتیب زمانی‌ست، پس نیازی به تبدیل
    # datetime قبل از min/max نیست. NaN/None به‌طور خودکار توسط pandas
    # نادیده گرفته می‌شوند.
    if "بازه_زمانی_شروع" in work.columns:
        agg_spec["بازه_زمانی_شروع"] = ("بازه_زمانی_شروع", "min")
    if "بازه_زمانی_پایان" in work.columns:
        agg_spec["بازه_زمانی_پایان"] = ("بازه_زمانی_پایان", "max")
    if "تعداد_روز_فعال" in work.columns:
        agg_spec["تعداد_روز_فعال"] = ("تعداد_روز_فعال", "sum")
    for wcol in thr_weighted_cols:
        agg_spec[f"{wcol}_sum"] = (wcol, "sum")

    agg = work.groupby([group_col, "نام_استراتژی"], as_index=False).agg(**agg_spec)
    agg["تعداد_تکرار"] = agg["تعداد_تکرار"].round().astype(int)
    agg["تعداد_سود"] = agg["_win_raw_sum"].round().astype(int).clip(lower=0)
    agg["تعداد_ضرر"] = (agg["تعداد_تکرار"] - agg["تعداد_سود"]).clip(lower=0)
    agg["میانگین_بازده_(%)"] = agg.apply(
        lambda r: (r["_return_weighted_sum"] / r["تعداد_تکرار"]) if r["تعداد_تکرار"] else 0.0, axis=1
    )
    agg["وین‌ریت_(%)"] = agg.apply(
        lambda r: (r["تعداد_سود"] / r["تعداد_تکرار"] * 100.0) if r["تعداد_تکرار"] else 0.0, axis=1
    )
    for tc, wcol in zip(thr_cols, thr_weighted_cols):
        agg[tc] = agg.apply(
            lambda r, wcol=wcol: (r[f"{wcol}_sum"] / r["تعداد_تکرار"]) if r["تعداد_تکرار"] else None, axis=1
        )

    period_cols = []
    if "بازه_زمانی_شروع" in agg.columns and "بازه_زمانی_پایان" in agg.columns:
        # [فیکس ۱۳] تعداد_روز_کل_بازه = فاصله‌ی تقویمی شروع تا پایان (شامل
        # شکاف‌های احتمالی بین دوره‌ها) — در تضاد عمدی با تعداد_روز_فعال که
        # فقط طول دوره‌هایی با معامله‌ی واقعی را می‌شمارد.
        start_dt = pd.to_datetime(agg["بازه_زمانی_شروع"], errors="coerce")
        end_dt = pd.to_datetime(agg["بازه_زمانی_پایان"], errors="coerce")
        agg["تعداد_روز_کل_بازه"] = (end_dt - start_dt).dt.days + 1
        period_cols = ["بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال", "تعداد_روز_کل_بازه"]

    agg = agg.rename(columns={group_col: "گروه"})
    return agg[
        ["گروه", "نام_استراتژی", "تعداد_تکرار", "تعداد_سود", "تعداد_ضرر", "وین‌ریت_(%)", "میانگین_بازده_(%)"]
        + period_cols + thr_cols
    ]


def _merge_with_previous(new_agg: pd.DataFrame, old_raw: pd.DataFrame | None) -> pd.DataFrame:
    """ادغام داده‌های جدید با داده‌های قبلیِ استخراج‌شده از فایل خروجی (حافظه).

    میانگین_بازده و وین‌ریت با وزن تعداد_تکرار هر ردیف دوباره محاسبه می‌شوند
    تا ادغام‌های متوالی صحیح (و بدون خطای انباشتی) باقی بمانند. اگر ستون‌های
    نسبت_شانس_آستانه_* در new_agg موجود باشند، همان‌طور و با همان وزن‌دهی حفظ
    می‌شوند (وگرنه بعد از اولین ادغام با فایل قبلی، این ستون‌ها بی‌صدا از بین
    می‌رفتند).

    [فیکس ۱۳] بازه_زمانی_شروع/پایان و تعداد_روز_فعال هم به همین ترتیب دوباره
    (min/max/sum) محاسبه می‌شوند تا وقتی رکورد قدیمی و جدید با هم ادغام
    می‌شوند، بازه‌ی نهایی درست‌ترین (گسترده‌ترین) بازه‌ی ممکن باشد.
    """
    if old_raw is None or old_raw.empty:
        return new_agg

    combined = pd.concat([old_raw, new_agg], ignore_index=True)
    combined["_return_weighted"] = combined["میانگین_بازده_(%)"] * combined["تعداد_تکرار"]

    thr_cols = _threshold_columns(new_agg)
    thr_weighted_cols = []
    for tc in thr_cols:
        if tc not in combined.columns:
            combined[tc] = None
        wcol = f"_w_{tc}"
        combined[wcol] = combined[tc].fillna(0) * combined["تعداد_تکرار"]
        thr_weighted_cols.append(wcol)

    has_period_cols = "بازه_زمانی_شروع" in combined.columns and "بازه_زمانی_پایان" in combined.columns

    agg_spec = dict(
        تعداد_تکرار=("تعداد_تکرار", "sum"),
        تعداد_سود=("تعداد_سود", "sum"),
        تعداد_ضرر=("تعداد_ضرر", "sum"),
        _return_weighted=("_return_weighted", "sum"),
    )
    if has_period_cols:
        agg_spec["بازه_زمانی_شروع"] = ("بازه_زمانی_شروع", "min")
        agg_spec["بازه_زمانی_پایان"] = ("بازه_زمانی_پایان", "max")
    if "تعداد_روز_فعال" in combined.columns:
        agg_spec["تعداد_روز_فعال"] = ("تعداد_روز_فعال", "sum")
    for wcol in thr_weighted_cols:
        agg_spec[f"{wcol}_sum"] = (wcol, "sum")

    merged = combined.groupby(["گروه", "نام_استراتژی"], as_index=False).agg(**agg_spec)
    merged["میانگین_بازده_(%)"] = merged.apply(
        lambda r: (r["_return_weighted"] / r["تعداد_تکرار"]) if r["تعداد_تکرار"] else 0.0, axis=1
    )
    merged["وین‌ریت_(%)"] = merged.apply(
        lambda r: (r["تعداد_سود"] / r["تعداد_تکرار"] * 100.0) if r["تعداد_تکرار"] else 0.0, axis=1
    )
    for tc, wcol in zip(thr_cols, thr_weighted_cols):
        merged[tc] = merged.apply(
            lambda r, wcol=wcol: (r[f"{wcol}_sum"] / r["تعداد_تکرار"]) if r["تعداد_تکرار"] else None, axis=1
        )
    if has_period_cols:
        start_dt = pd.to_datetime(merged["بازه_زمانی_شروع"], errors="coerce")
        end_dt = pd.to_datetime(merged["بازه_زمانی_پایان"], errors="coerce")
        merged["تعداد_روز_کل_بازه"] = (end_dt - start_dt).dt.days + 1
    merged.drop(columns=["_return_weighted"], inplace=True)
    return merged


def _rank_and_select(agg: pd.DataFrame) -> pd.DataFrame:
    """رتبه‌بندی هر گروه (تعداد_تکرار نزولی → میانگین_بازده نزولی) و اعمال
    شرط نمایش: فقط رتبه ۱ اگر (میانگین_بازده ≥ ۰.۵٪ و وین‌ریت > ۸۰٪)، وگرنه ۵ رتبه اول."""
    thr_cols = _threshold_columns(agg)
    # [فیکس ۱۳] ستون‌های بازه‌ی زمانی هم باید در خروجی نهایی حفظ بشن — قبلاً
    # اینجا هاردکد به RECOMMENDATION_GROUP_COLUMNS بود و بی‌صدا حذف می‌شدند.
    period_cols = [c for c in ["بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال", "تعداد_روز_کل_بازه"] if c in agg.columns]
    out_columns = RECOMMENDATION_GROUP_COLUMNS + period_cols + thr_cols
    if agg.empty:
        return pd.DataFrame(columns=out_columns)

    out_frames = []
    for group_val, gdf in agg.groupby("گروه", sort=False):
        gdf = gdf.sort_values(
            by=["تعداد_تکرار", "میانگین_بازده_(%)"], ascending=[False, False]
        ).reset_index(drop=True)
        top = gdf.iloc[0]
        strong = (top["میانگین_بازده_(%)"] >= 0.5) and (top["وین‌ریت_(%)"] > 80)
        selected = gdf.iloc[[0]].copy() if strong else gdf.iloc[:5].copy()
        selected["رتبه"] = range(1, len(selected) + 1)
        out_frames.append(selected)

    result = pd.concat(out_frames, ignore_index=True)
    return result[out_columns]



def _rank1_changed(old_final: pd.DataFrame | None, new_final: pd.DataFrame) -> bool:
    """بررسی اینکه آیا رتبه ۱ حداقل یک گروه نسبت به فایل قبلی تغییر کرده است."""
    if old_final is None or old_final.empty:
        return True  # اولین بار — همیشه نوشته شود
    if "رتبه" not in old_final.columns:
        return True

    old_top = old_final[old_final["رتبه"] == 1].set_index("گروه")["نام_استراتژی"]
    new_top = new_final[new_final["رتبه"] == 1].set_index("گروه")["نام_استراتژی"]
    if set(old_top.index) != set(new_top.index):
        return True
    return not old_top.reindex(new_top.index).equals(new_top)


def _generate_recommendation_output(
    df: pd.DataFrame,
    group_col: str,
    output_path: Path,
    filename: str,
    label: str,
    include_thr_cols: bool = True,
) -> None:
    """منطق مشترک تولید یک خروجی پیشنهاد استراتژی (افزایشی) بر اساس ستون گروه‌بندی مشخص."""
    if df.empty or group_col not in df.columns:
        log.warning(f"[{label}] داده‌ای برای تولید خروجی یافت نشد — رد می‌شود.")
        return

    new_agg = _prepare_group_aggregates(df, group_col, include_thr_cols=include_thr_cols)

    out_file = output_path / filename
    old_final = None
    if out_file.exists():
        try:
            old_final = pd.read_csv(out_file)
        except Exception as exc:
            log.warning(f"[{label}] خواندن فایل قبلی {out_file} ناموفق بود ({exc}) — نادیده گرفته شد.")

    old_raw = None
    if old_final is not None and not old_final.empty:
        # [فیکس ۶] قبلاً فقط ۷ ستون ثابت از فایل قبلی خوانده می‌شد و ستون‌های
        # نسبت_شانس_آستانه_* (در صورت وجود از اجرای قبلی) دور ریخته می‌شدند —
        # یعنی از اجرای دوم incremental به بعد، میانگین وزنی آستانه‌ها با
        # وزن صفر/None برای ردیف‌های قدیمی رقیق و غلط می‌شد. حالا هر ستونی که
        # با نسبت_شانس_آستانه_ شروع شود هم (در صورت وجود در فایل قبلی) نگه
        # داشته می‌شود — مگر برای گزارش‌هایی که include_thr_cols=False دارند
        # ([فیکس ۸]: مثلاً رژیم بازار)، که آنجا این ستون‌ها عمداً همیشه کنار
        # گذاشته می‌شوند، حتی اگر (مثلاً از یک نسخه‌ی قدیمی‌تر) در فایل قبلی مانده باشند.
        base_cols = ["گروه", "نام_استراتژی", "تعداد_تکرار", "تعداد_سود", "تعداد_ضرر", "وین‌ریت_(%)", "میانگین_بازده_(%)"]
        old_thr_cols = [c for c in old_final.columns if c.startswith("نسبت_شانس_آستانه_")] if include_thr_cols else []
        # [فیکس ۱۳] بازه‌ی زمانی هم مثل ستون‌های آستانه باید از فایل قبلی حفظ
        # شود، وگرنه با هر اجرای incremental، بازه‌ی محاسبه‌شده فقط شامل
        # جدیدترین داده می‌شد نه کل تاریخچه.
        old_period_cols = [c for c in ["بازه_زمانی_شروع", "بازه_زمانی_پایان", "تعداد_روز_فعال"] if c in old_final.columns]
        old_raw = old_final[base_cols + old_period_cols + old_thr_cols].copy()

    merged_agg = _merge_with_previous(new_agg, old_raw)
    new_final = _rank_and_select(merged_agg)

    if _rank1_changed(old_final, new_final):
        saved_path = _save(new_final, output_path / Path(filename).stem)
        log.info(f"[{label}] خروجی به‌روزرسانی شد: {saved_path} ({len(new_final)} ردیف)")
    else:
        log.info(f"[{label}] رتبه ۱ هیچ گروهی تغییر نکرد — فایل {out_file} بدون تغییر باقی ماند.")


def generate_news_pattern_recommendation(df: pd.DataFrame, output_dir) -> None:
    """تولید خروجی پیشنهاد استراتژی بر اساس الگوی خبری (افزایشی)."""
    output_path = Path(output_dir)
    work = df.copy()
    work["گروه_خبری"] = work["signature"]
    _generate_recommendation_output(
        work,
        "گروه_خبری",
        output_path,
        "پیشنهاد_استراتژی_بر_اساس_الگوی_خبری.csv",
        "الگوی خبری",
    )


def generate_news_pattern_no_regime_recommendation(df: pd.DataFrame, output_dir) -> None:
    """تولید خروجی پیشنهاد استراتژی بر اساس الگوی خبری، فارغ از رژیم بازار.

    [افزوده] عیناً مثل generate_news_pattern_recommendation عمل می‌کند، با این
    تفاوت که کلید گروه‌بندی به‌جای signature کامل، signature بدون پسوند رژیم
    است (با _strip_market_regime — همان تابعی که generate_market_regime_
    recommendation برای ساخت «نام_استراتژی» استفاده می‌کند).

    نتیجه: ردیف‌هایی که فقط رژیمشان فرق دارد ولی بقیه‌ی ترکیب (کوین + اندیکاتور
    خبری + موقعیت/فاصله + مدل + سشن) یکسان است، در _prepare_group_aggregates
    زیر یک گروه با هم جمع می‌شوند (تعداد_تکرار/سود/ضرر جمع مستقیم می‌شوند،
    میانگین_بازده وزن‌دار بر اساس تعداد_تکرار محاسبه می‌شود — دقیقاً همان
    منطقی که _prepare_group_aggregates برای هر گروه همیشه انجام می‌دهد).

    «نام_استراتژی» عمداً پیش‌فرض (strategy_id + coin_composition) گذاشته
    می‌شود، نه نسخه‌ی امضادار — چون این خروجی برخلاف رژیم بازار، قرار است هر
    گروه دقیقاً یک ترکیب خبری یکتا (بدون رژیم) باشد، پس نیازی به [فیکس ۱۱]
    نیست و include_thr_cols=True می‌ماند (چون هر گروه هنوز یک ترکیب شاخص
    یکتاست، فقط بدون تفکیک رژیم).
    """
    output_path = Path(output_dir)
    work = df.copy()
    work["گروه_خبری_بدون_رژیم"] = work["signature"].apply(_strip_market_regime)
    _generate_recommendation_output(
        work,
        "گروه_خبری_بدون_رژیم",
        output_path,
        "پیشنهاد_استراتژی_بر_اساس_الگوی_خبری_بدون_رژیم.csv",
        "الگوی خبری بدون رژیم",
    )


KNOWN_MARKET_REGIMES = ["trending_up", "trending_down", "volatile", "ranging", "unknown"]


def _extract_market_regime(signature) -> str:
    """استخراج رژیم بازار از رشته signature (مطابق build_signature).

    [فیکس ۷] قبلاً با signature.rsplit("_", 1)[-1] فقط آخرین تکه‌ی بعد از
    آخرین "_" برداشته می‌شد — که برای رژیم‌های حاوی خودِ "_" (trending_up،
    trending_down) نصف اسم رو قطع می‌کرد (مثلاً "up" به‌جای "trending_up").
    حالا در برابر لیست رژیم‌های شناخته‌شده (طولانی‌ترین اول) به‌عنوان پسوند
    کامل چک می‌شود.
    """
    if not isinstance(signature, str) or not signature:
        return "unknown"
    for regime in sorted(KNOWN_MARKET_REGIMES, key=len, reverse=True):
        if signature == regime or signature.endswith("_" + regime):
            return regime
    return "unknown"


def generate_market_regime_recommendation(df: pd.DataFrame, output_dir) -> None:
    """تولید خروجی پیشنهاد استراتژی بر اساس رژیم بازار (افزایشی)."""
    output_path = Path(output_dir)
    work = df.copy()
    work["رژیم_بازار"] = work["signature"].apply(_extract_market_regime)
    # [فیکس ۳] رکوردهای unknown عمدتاً به‌خاطر warm-up period (کمتر از ۲۰۰
    # روز دیتای OHLC قبلی برای MA200) هستند، نه یک رژیم واقعی بازار — حذف می‌شوند.
    before = len(work)
    work = work[work["رژیم_بازار"] != "unknown"]
    dropped = before - len(work)
    if dropped:
        log.info(f"[رژیم بازار] {dropped:,} ردیف با رژیم 'unknown' حذف شد.")
    # [فیکس ۱۱] قبلاً کلید دوم گروه‌بندی («نام_استراتژی») فقط strategy_id+coin
    # بود — یعنی ده‌ها/صدها ترکیب کاملاً متفاوت (اندیکاتور/موقعیت/فاصله/مدل/
    # سشن متفاوت) که فقط strategy_id و کوینشان یکی بود، زیر یک رژیم در یک
    # ردیف با میانگین‌گیری قاطی می‌شدند — دقیقاً همان «میانگین بین ۱۰۰۰ ترکیب»
    # که هدف (پیدا کردن دقیق‌ترین ترکیب سودده) را از بین می‌برد. حالا
    # «نام_استراتژی» شامل کل هویت ترکیب (strategy_id + امضای کامل بدون پسوند
    # رژیم — که خودش کوین/اندیکاتور/موقعیت/فاصله/مدل/سشن را در بر دارد)
    # است، پس هر ردیف در خروجی نهایی دقیقاً یک ترکیب یکتا را نشان می‌دهد، نه
    # میانگینی از چند ترکیب مختلف.
    work["نام_استراتژی"] = (
        work["strategy_id"].astype(str) + " | " + work["signature"].apply(_strip_market_regime)
    )
    # [فیکس ۸] یک گروهِ «رژیم بازار» می‌تواند شامل چندین ترکیب شاخص متفاوت
    # باشد؛ نسبت_شانس_آستانه_* ذاتاً مال یک ترکیب خاص است، پس اینجا اصلاً
    # محاسبه/نمایش داده نمی‌شود تا هرگز دو ترکیب نامرتبط با هم قاطی نشوند.
    # این ستون‌ها همچنان کامل و درست در فایل «الگوی خبری» موجودند (آنجا هر
    # گروه = دقیقاً یک ترکیب یکتاست).
    _generate_recommendation_output(
        work,
        "رژیم_بازار",
        output_path,
        "پیشنهاد_استراتژی_بر_اساس_رژیم_بازار.csv",
        "رژیم بازار",
        include_thr_cols=False,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ماژول Golden – امتیازدهی استراتژی‌ها بر اساس شرایط خبری"
    )
    parser.add_argument("--signatures-dir", required=True, help="مسیر پوشه فایل‌های .jsonl signatures")
    parser.add_argument("--strategies-json", required=False, default=None, help="(اختیاری، بی‌استفاده) مسیر فایل strategies_metadata.json")
    parser.add_argument("--version-schema", default=None, help="(اختیاری) مسیر فایل version_schema.json")
    parser.add_argument("--ohlc-dir", default=None, help="(اختیاری) مسیر پوشه داده‌های OHLC")
    parser.add_argument("--output-dir", required=True, help="مسیر پوشه خروجی")
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA, help="ضریب بازخورد Correlation (پیش‌فرض: 0.1)")
    parser.add_argument(
        "--status-file",
        default=None,
        help="(اختیاری) مسیر فایل وضعیت برای مدیریت وقفه و ادامه (پیش‌فرض: golden_status.json در output-dir)",
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
        help=f"تعداد امضا در هر قطعه (پیش‌فرض: {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--interrupt-flag",
        default=None,
        help="(اختیاری) مسیر فایل interrupt.flag برای توقف کنترل‌شده (پیش‌فرض: interrupt.flag در output-dir)",
    )
    parser.add_argument(
        "--archive-index",
        default=None,
        help="(اختیاری) مسیر فایل signature_archive_index.json برای پر کردن ستون archive_path "
             "در golden_scores.csv. اگر داده نشود، archive_path=None ثبت می‌شود.",
    )
    parser.add_argument(
        "--patterns-dir",
        default=None,
        help="(اختیاری) مسیر پوشه‌ی CSVهای آستانه/نسبت_شانس استخراج‌شده از pattern_archives/ "
             "(ساختار مثل signatures-dir: {module}/{strategy}/{coin}/{interval}_{model}.csv). "
             "در صورت داده‌شدن، ستون‌های نسبت_شانس_آستانه_* (فقط تک‌شاخصی) به دو خروجی پیشنهاد "
             "استراتژی (الگوی خبری و رژیم بازار) اضافه می‌شوند — golden_scores.csv بدون تغییر می‌ماند.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        signatures_dir=args.signatures_dir,
        strategies_json=args.strategies_json,
        version_schema=args.version_schema,
        ohlc_dir=args.ohlc_dir,
        output_dir=args.output_dir,
        alpha=args.alpha,
        status_file=args.status_file,
        resume=args.resume,
        chunk_size=args.chunk_size,
        interrupt_flag=args.interrupt_flag,
        archive_index=args.archive_index,
        patterns_dir=args.patterns_dir,
    )
    sys.exit(0)
