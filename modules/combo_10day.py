#!/usr/bin/env python3
# combo_10day.py - تحلیل ترکیبی الگوهای خبری در بازه‌های زمانی دلخواه
# نسخه ادغام‌شده: هم CSV و هم JSONL (امضاهای per-period) تولید می‌کند.
# اولویت ۱: --ohlc-dir اجباری است. اگر داده نشود یا پوشه خالی باشد، exit 1.
# اولویت ۲: --jsonl-out اختیاری است؛ در صورت داده‌شدن، JSONL نیز تولید می‌شود.

import os
import sys
import json
import csv
import glob
import argparse
import itertools
import statistics
import re
import time
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np

# ================================ ثابت‌ها ================================
INDICATORS = ['CPI m/m', 'Core CPI m/m', 'PPI m/m', 'Core PPI m/m', 'FOMC', 'CPI y/y']
THRESHOLDS = [0.0, 0.1, 0.2, 0.3]

INTERVAL_TO_INDICATOR = {
    'CPI':      'CPI m/m',
    'CoreCPI':  'Core CPI m/m',
    'PPI':      'PPI m/m',
    'CorePPI':  'Core PPI m/m',
    'FOMC':     'FOMC',
    'CPI_y_y':  'CPI y/y',
}

VALID_MODELS = ['simple_hybrid', 'fibonacci_full', 'fibonacci_hybrid']

# نسبت‌های فیبوناچی برای تقسیم بازه‌های زمانی (تجمعی، از ابتدای بازه)
FIBONACCI_RATIOS = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0]

# ================================ توابع کمکی بازده ویژه ================================

def round_to_nearest(value, options):
    if not options:
        return value
    return min(options, key=lambda x: abs(x - value))


def apply_special_rounding(percent, move_percents):
    """تبدیل سود خام به بازده ویژه (special rounded return)"""
    if not move_percents or percent == 0:
        return percent
    try:
        if percent > 0:
            temp = percent + 0.05
            nearest = round_to_nearest(temp, move_percents)
            result = nearest - 0.05
        else:
            temp = abs(percent) + 0.05
            nearest = round_to_nearest(temp, move_percents)
            result = -(nearest - 0.05)
        return result - 0.1
    except Exception:
        return percent


# ================================ توابع کمکی خواندن CSV ================================

def find_column_index(header, keyword):
    kw = keyword.lower()
    for i, h in enumerate(header):
        if kw in h.lower():
            return i
    return -1


def parse_percent(value_str):
    if value_str is None or str(value_str).strip() in ('', '-', '—', '--'):
        return None
    cleaned = re.sub(r'[^\d.-]', '', str(value_str))
    if cleaned in ('', '-', '.', '-.'):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def detect_indicator_from_filename(filename_no_ext):
    name = filename_no_ext.lower()
    if 'core_cpi' in name or 'core cpi' in name:
        return 'Core CPI m/m'
    if 'core_ppi' in name or 'core ppi' in name:
        return 'Core PPI m/m'
    if 'cpi_y_y' in name or 'cpi y/y' in name or 'cpi y_y' in name:
        return 'CPI y/y'
    if 'cpi' in name:
        return 'CPI m/m'
    if 'ppi' in name:
        return 'PPI m/m'
    if 'fomc' in name or 'federal' in name or 'interest rate' in name:
        return 'FOMC'
    if 'moneycontrol' in name or 'united_states' in name:
        return 'FOMC'
    return None


def load_news_from_directory(news_dir):
    events = []
    if not os.path.isdir(news_dir):
        print(f"⚠️ پوشه اخبار یافت نشد: {news_dir}")
        return events

    print(f"📂 بارگذاری اخبار از {news_dir}")
    for filename in sorted(os.listdir(news_dir)):
        if not filename.endswith('.csv'):
            continue

        file_path = os.path.join(news_dir, filename)
        base_name = filename.replace('.csv', '').strip()
        indicator = detect_indicator_from_filename(base_name)
        if indicator is None:
            print(f"   ⚠️ {filename}: indicator ناشناخته → نادیده گرفته شد")
            continue

        is_fomc = (indicator == 'FOMC')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                date_idx     = find_column_index(header, 'date')
                actual_idx   = find_column_index(header, 'actual')
                forecast_idx = find_column_index(header, 'forecast')
                if forecast_idx == -1:
                    forecast_idx = find_column_index(header, 'consensus')
                previous_idx  = find_column_index(header, 'previous')
                reference_idx = find_column_index(header, 'reference') if is_fomc else -1

                if date_idx == -1:
                    print(f"   ⚠️ {filename}: ستون تاریخ یافت نشد → نادیده گرفته شد")
                    continue

                count = 0
                for row in reader:
                    if not row:
                        continue
                    if is_fomc and reference_idx != -1 and reference_idx < len(row):
                        ref = row[reference_idx].strip()
                        if 'Interest Rate' not in ref and 'FOMC' not in ref:
                            continue
                    if date_idx >= len(row):
                        continue
                    date_str = row[date_idx].strip()
                    try:
                        event_date = datetime.strptime(date_str, "%b %d, %Y").date()
                    except ValueError:
                        try:
                            event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        except ValueError:
                            continue

                    actual   = parse_percent(row[actual_idx])   if actual_idx != -1   and actual_idx < len(row)   else None
                    forecast = parse_percent(row[forecast_idx]) if forecast_idx != -1 and forecast_idx < len(row) else None
                    previous = parse_percent(row[previous_idx]) if previous_idx != -1 and previous_idx < len(row) else None

                    events.append({
                        "date":      event_date,
                        "indicator": indicator,
                        "actual":    actual,
                        "forecast":  forecast,
                        "previous":  previous,
                    })
                    count += 1

            print(f"   ✅ {filename} → [{indicator}]: {count} رویداد")
        except Exception as e:
            print(f"   ⚠️ خطا در خواندن {filename}: {e}")

    print(f"📰 مجموع رویدادهای خبری بارگذاری‌شده: {len(events)}")
    return events


# ================================ OHLC بارگذاری ================================

def extract_coin_name(filename):
    """
    استخراج نام واقعی کوین از نام فایل OHLC تکه‌تکه‌شده.
    مثال: BTCUSDT-5m-2018-01-01_2018-01-10.csv → BTCUSDT
    الگو: هر چیزی قبل از اولین توکن تایم‌فریم (مثل -5m- یا -1h- یا -1d- یا -1w-)
    که با خط‌تیره از دو طرف جدا شده.
    اگر الگوی تایم‌فریم پیدا نشد (مثلاً فایل از قبل به‌ازای هر کوین یکی است،
    بدون پسوند تایم‌فریم/بازه)، کل نام فایل (بدون پسوند) به‌عنوان نام کوین
    برگردانده می‌شود (سازگاری با عقب).
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    match = re.match(r'^(.+?)-\d+[mhdwM]-', base)
    if match:
        return match.group(1).upper()
    return base


def load_ohlc_data(ohlc_dir):
    """
    [اولویت ۱] بارگذاری داده‌های OHLC از پوشه.
    - فایل‌های OHLC ممکن است به‌صورت تکه‌تکه (مثلاً بازه‌های ۱۰روزه) برای یک
      کوین ذخیره شده باشند؛ نام واقعی کوین از روی نام فایل استخراج می‌شود
      (extract_coin_name) و تمام تکه‌های مربوط به یک کوین قبل از resample
      با هم ادغام می‌شوند.
    - ستون‌های date یا timestamp را می‌پذیرد.
    - تایم‌فریم را بر اساس میانگین فاصله‌ی زمانی بین رکوردهای *ادغام‌شده‌ی*
      هر کوین تشخیص می‌دهد.
    - اگر تایم‌فریم زیر-روزانه بود، resample روزانه روی کل سری زمانی آن
      کوین اعمال می‌شود (نه روی هر فایل/تکه به‌تنهایی).
    - اگر بعد از ادغام و resample، تعداد کل روزهای آن کوین < 200 بود،
      هشدار چاپ می‌کند (MA200 قابل‌محاسبه نخواهد بود).
    """
    columns = ["date", "coin", "open", "high", "low", "close"]
    if not ohlc_dir or not os.path.isdir(ohlc_dir):
        return pd.DataFrame(columns=columns)

    csv_paths = sorted(glob.glob(os.path.join(ohlc_dir, "*.csv")))
    if not csv_paths:
        return pd.DataFrame(columns=columns)

    # ۱. خواندن خام تمام فایل‌ها و گروه‌بندی بر اساس نام واقعی کوین
    #    (چند فایل/تکه می‌توانند به یک کوین تعلق داشته باشند)
    coin_raw_frames = defaultdict(list)
    for path in csv_paths:
        coin = extract_coin_name(path)
        fname = os.path.basename(path)
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"   ⚠️ خطا در خواندن {fname}: {e}")
            continue

        df.columns = [str(col).strip().lower() for col in df.columns]

        # پشتیبانی از ستون timestamp به‌عنوان date
        if "date" not in df.columns and "timestamp" in df.columns:
            df = df.rename(columns={"timestamp": "date"})

        required = {"date", "open", "high", "low", "close"}
        if not required.issubset(set(df.columns)):
            print(f"   ⚠️ [{fname}] ستون‌های لازم یافت نشد → نادیده گرفته شد.")
            continue

        df = df[["date", "open", "high", "low", "close"]].copy()
        # [اصلاح تایم‌زون] بعد از تبدیل به datetime، تایم‌زون را حذف می‌کنیم تا
        # مقایسه با Timestampهای naive در compute_market_regime با خطای
        # "Invalid comparison between dtype=datetime64[ns, UTC] and Timestamp" مواجه نشود.
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
        df = df.dropna(subset=["date"])
        if df.empty:
            continue

        coin_raw_frames[coin].append(df)

    if not coin_raw_frames:
        return pd.DataFrame(columns=columns)

    coin_names_preview = ', '.join(list(coin_raw_frames.keys())[:5])
    more_suffix = '...' if len(coin_raw_frames) > 5 else ''
    print(f"📦 {len(csv_paths)} فایل CSV → {len(coin_raw_frames)} کوین یکتا شناسایی شد "
          f"(مثال: {coin_names_preview}{more_suffix})")

    frames = []
    for coin, parts in coin_raw_frames.items():
        # ۲. ادغام تمام تکه‌های یک کوین در یک DataFrame واحد
        df = pd.concat(parts, ignore_index=True)

        # ۳. مرتب‌سازی بر اساس تاریخ (و حذف رکوردهای تکراری احتمالی در مرز تکه‌ها)
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        # ۴. تشخیص تایم‌فریم روی کل سری زمانیِ ادغام‌شده (نه هر فایل جداگانه)
        if len(df) >= 2:
            time_diffs = df["date"].diff().dropna()
            avg_diff_minutes = time_diffs.dt.total_seconds().mean() / 60.0
        else:
            avg_diff_minutes = 1440.0  # فرض روزانه

        is_intraday = avg_diff_minutes < 60 * 23  # کمتر از ۲۳ ساعت → زیر-روزانه

        if is_intraday:
            timeframe_str = (
                f"{int(avg_diff_minutes)}دقیقه‌ای" if avg_diff_minutes < 60
                else f"{avg_diff_minutes/60:.1f}ساعته"
            )
            print(f"   ⏱️ [{coin}] {len(parts)} فایل ادغام شد → تایم‌فریم: ~{timeframe_str} "
                  f"(میانگین فاصله {avg_diff_minutes:.1f} دقیقه) → resample به روزانه روی کل سری")

            # ۵. resample روزانه روی کل داده‌ی ادغام‌شده‌ی کوین
            df = df.set_index("date")
            df_daily = df.resample("D").agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
            ).dropna(subset=["close"])
            df_daily = df_daily.reset_index()
            df_daily.columns = ["date", "open", "high", "low", "close"]
            df = df_daily
            print(f"   ✅ [{coin}] بعد از ادغام و resample: {len(df)} روز کاری "
                  f"(از {len(parts)} فایل تکه‌ای)")
        else:
            print(f"   ✅ [{coin}] {len(parts)} فایل ادغام شد → تایم‌فریم روزانه "
                  f"(میانگین فاصله {avg_diff_minutes:.1f} دقیقه)، مجموع {len(df)} روز")

        # ۶. هشدار اگر تعداد کل روزها (پس از ادغام همه‌ی تکه‌ها) کمتر از ۲۰۰ بود
        if len(df) < 200:
            print(f"   ⚠️ [{coin}] تعداد کل روزهای OHLC پس از ادغام ({len(df)}) کمتر از ۲۰۰ است → "
                  f"market_regime این کوین 'unknown' خواهد بود (MA200 قابل‌محاسبه نیست).")

        df["coin"] = coin
        frames.append(df[columns])

    if not frames:
        return pd.DataFrame(columns=columns)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["coin", "date"]).reset_index(drop=True)
    print(f"✅ OHLC: {len(combined)} ردیف از {len(frames)} کوین "
          f"(ادغام‌شده از {len(csv_paths)} فایل) بارگذاری شد.")
    tz_info = combined["date"].dt.tz if len(combined) > 0 else None
    print(f"🕒 [تایم‌زون] ستون date تایم‌زون‌زدایی شد (tz_localize(None)) → tz فعلی: {tz_info}")
    return combined


def _build_ohlc_coin_index(ohlc_df):
    """[بهینه‌سازی سرعت] پیش‌محاسبه یک‌باره: به‌ازای هر کوین، آرایه‌های numpy از
    close/high/low (که در load_ohlc_data از قبل بر اساس ["coin","date"] مرتب
    شده‌اند) را همراه با لیست تاریخ‌ها نگه می‌دارد.

    این کار جایگزین دو مشکل نسخه‌ی قبلی می‌شود:
    ۱) فیلتر `ohlc_df[ohlc_df["coin"] == coin]` که به‌ازای *هر دوره* از نو روی
       کل دیتافریم چندکوینه اجرا می‌شد.
    ۲) سربار خود pandas (ساخت Series/DataFrame جدید در هر .tail()/.mean()) که
       حتی بعد از رفع مورد (۱) هنوز غالب زمان اجرا بود — با کار مستقیم روی
       آرایه‌های numpy این سربار هم حذف می‌شود. مقدار محاسبه‌شده دقیقاً یکسان
       می‌ماند (np.nanmean == pandas .mean() با skipna=True پیش‌فرض).
    """
    index = {}
    if ohlc_df is None or len(ohlc_df) == 0:
        return index
    for coin, sub in ohlc_df.groupby("coin", sort=False):
        sub_sorted = sub.reset_index(drop=True)  # از قبل بر اساس date مرتب است
        dates_list = list(sub_sorted["date"])
        close_arr  = sub_sorted["close"].to_numpy(dtype="float64")
        high_arr   = sub_sorted["high"].to_numpy(dtype="float64")
        low_arr    = sub_sorted["low"].to_numpy(dtype="float64")
        index[coin] = (dates_list, close_arr, high_arr, low_arr)
    return index


def compute_market_regime(coin_ohlc_index, coin, start_date):
    """
    رژیم بازار را برای یک کوین مشخص، صرفاً بر اساس داده‌های قیمت *قبل* از
    start_date محاسبه می‌کند (بدون آینده‌نگری).

    [بهینه‌سازی سرعت] به‌جای pandas DataFrame/Series، مستقیماً روی آرایه‌های
    numpy از قبل ساخته‌شده (coin_ohlc_index از _build_ohlc_coin_index) کار
    می‌کند و برش تاریخی را با bisect انجام می‌دهد. خروجی دقیقاً معادل نسخه‌ی
    قبلی است (np.nanmean دقیقاً معادل pandas .mean() پیش‌فرض است).
    """
    if not coin_ohlc_index or coin is None:
        return "unknown"

    entry = coin_ohlc_index.get(coin)
    if entry is None:
        return "unknown"
    dates_list, close_arr, high_arr, low_arr = entry
    if len(dates_list) == 0:
        return "unknown"

    cutoff = pd.Timestamp(start_date) - timedelta(days=1)
    idx = bisect_right(dates_list, cutoff)
    if idx == 0 or idx < 200:
        return "unknown"

    close_200 = close_arr[idx - 200:idx]
    close_50  = close_arr[idx - 50:idx]
    high_14   = high_arr[idx - 14:idx]
    low_14    = low_arr[idx - 14:idx]

    ma50  = np.nanmean(close_50)
    ma200 = np.nanmean(close_200)
    atr   = np.nanmean(high_14 - low_14)
    price = close_arr[idx - 1]

    if price is None or pd.isna(price) or price == 0:
        return "unknown"
    if pd.isna(ma50) or pd.isna(ma200) or pd.isna(atr):
        return "unknown"

    if (atr / price) > 0.02:
        return "volatile"
    if ma50 > ma200:
        return "trending_up"
    if ma50 < ma200:
        return "trending_down"
    if abs(ma50 - ma200) / price < 0.05:
        return "ranging"
    return "unknown"


def _precompute_regime_series(coin_ohlc_index):
    """[فیکس: رژیم per-trade] برای هر کوین، رژیم را برای *تمام* موقعیت‌های
    ممکنِ idx در دیتای آن کوین یک‌بار و به‌صورت کاملاً vectorized (numpy)
    پیش‌محاسبه می‌کند — دقیقاً با همان فرمول compute_market_regime (بدون
    آینده‌نگری: MA50/MA200/ATR14 روی idx-200:idx و امثال آن).

    خروجی: dict به ازای هر کوین → (dates_list, regimes_by_idx)
    regimes_by_idx یک آرایه numpy به طول len(dates_list)+1 است، به‌طوری‌که
    regimes_by_idx[idx] دقیقاً همان چیزی است که compute_market_regime وقتی
    idx = bisect_right(dates_list, cutoff) باشد برمی‌گرداند.
    """
    regime_index = {}
    for coin, (dates_list, close_arr, high_arr, low_arr) in coin_ohlc_index.items():
        n = len(close_arr)
        regimes = np.full(n + 1, "unknown", dtype=object)
        if n < 200:
            regime_index[coin] = (dates_list, regimes)
            continue

        close_s = pd.Series(close_arr)
        ma50_arr  = close_s.rolling(window=50,  min_periods=50).mean().to_numpy()
        ma200_arr = close_s.rolling(window=200, min_periods=200).mean().to_numpy()
        atr_arr   = pd.Series(high_arr - low_arr).rolling(window=14, min_periods=14).mean().to_numpy()

        with np.errstate(invalid="ignore", divide="ignore"):
            safe_price = np.where(close_arr == 0, np.nan, close_arr)
            atr_ratio  = atr_arr / safe_price
            ma_diff_pct = np.abs(ma50_arr - ma200_arr) / safe_price

        cond_volatile = atr_ratio > 0.02
        cond_up       = (~cond_volatile) & (ma50_arr > ma200_arr)
        cond_down     = (~cond_volatile) & (ma50_arr < ma200_arr)
        cond_ranging  = (~cond_volatile) & (~cond_up) & (~cond_down) & (ma_diff_pct < 0.05)

        labels = np.full(n, "unknown", dtype=object)
        labels[cond_volatile] = "volatile"
        labels[cond_up]       = "trending_up"
        labels[cond_down]     = "trending_down"
        labels[cond_ranging]  = "ranging"

        regimes[1:] = labels
        regimes[:200] = "unknown"
        regime_index[coin] = (dates_list, regimes)
    return regime_index


def lookup_market_regime(regime_index, coin, trade_date):
    """[فیکس: رژیم per-trade] معادل دقیق compute_market_regime(...، trade_date)
    ولی با لوکاپ ارزان (bisect) روی جدول از پیش‌ساخته‌ی _precompute_regime_series."""
    if not regime_index or coin is None:
        return "unknown"
    entry = regime_index.get(coin)
    if entry is None:
        return "unknown"
    dates_list, regimes = entry
    if len(dates_list) == 0:
        return "unknown"
    cutoff = pd.Timestamp(trade_date) - timedelta(days=1)
    idx = bisect_right(dates_list, cutoff)
    if idx == 0 or idx < 200 or idx >= len(regimes):
        return "unknown"
    return regimes[idx]


# ================================ توابع کمکی بازه‌های زمانی ================================

def parse_interval(interval):
    m_fixed = re.match(r'^fixed_(\d+)d$', interval)
    if m_fixed:
        return ('fixed', int(m_fixed.group(1)), None)
    m_news = re.match(r'^(.+)_(post|pre)_(\d+)d$', interval)
    if m_news:
        return (m_news.group(1), m_news.group(2), int(m_news.group(3)))
    return None


def find_nearest_event_date(events, target_date, indicator, direction='post'):
    filtered = [ev["date"] for ev in events
                if ev["indicator"] == indicator and ev["date"] is not None]
    if not filtered:
        return None
    if direction == 'post':
        candidates = [d for d in filtered if d <= target_date]
        return max(candidates) if candidates else None
    else:
        candidates = [d for d in filtered if d >= target_date]
        return min(candidates) if candidates else None


def find_adjacent_event_date(events, anchor_date, indicator, direction):
    filtered = sorted({ev["date"] for ev in events
                        if ev["indicator"] == indicator and ev["date"] is not None})
    if not filtered:
        return None
    if direction == 'post':
        candidates = [d for d in filtered if d > anchor_date]
        return min(candidates) if candidates else None
    else:
        candidates = [d for d in filtered if d < anchor_date]
        return max(candidates) if candidates else None


# ================================ ایندکس‌سازی سریع رویدادهای خبری ================================
# نکته کارایی: توابع بالا (find_nearest_event_date و find_adjacent_event_date) در نسخه
# اصلی هر بار که صدا زده می‌شوند، کل لیست news_events را به‌صورت خطی (O(E)) اسکن
# می‌کنند. از آنجا که get_period_key_from_date به ازای *هر معامله* صدا زده می‌شود،
# این اسکن خطی به‌صورت O(تعداد_معاملات × تعداد_رویدادها) تکرار می‌شود که کندترین
# بخش اسکریپت برای interval های post/pre است.
#
# توابع زیر همان منطق را با ایندکس مرتب‌شده و جستجوی دودویی (bisect) در O(log E)
# انجام می‌دهند و از نظر خروجی دقیقاً معادل نسخه اصلی هستند (بدون تغییر رفتار):

def _build_indicator_date_index(news_events):
    """یک‌بار برای کل اجرا: به ازای هر indicator، لیست مرتب و یکتای تاریخ‌ها را می‌سازد."""
    by_indicator = defaultdict(set)
    for ev in news_events:
        if ev["date"] is not None:
            by_indicator[ev["indicator"]].add(ev["date"])
    return {ind: sorted(dates) for ind, dates in by_indicator.items()}


def _build_sorted_event_index(news_events):
    """یک‌بار برای کل اجرا: رویدادها را بر اساس تاریخ مرتب می‌کند تا بازه‌جویی
    با bisect ممکن شود (برای compute_indicator_status_for_period و events_in_range)."""
    sorted_events = sorted(news_events, key=lambda ev: ev["date"])
    sorted_dates = [ev["date"] for ev in sorted_events]
    return sorted_dates, sorted_events


def _events_in_range_fast(sorted_dates, sorted_events, start_date, end_date):
    """معادل [ev for ev in news_events if start_date <= ev['date'] <= end_date]
    اما با bisect در O(log E + k) به‌جای O(E)."""
    lo = bisect_left(sorted_dates, start_date)
    hi = bisect_right(sorted_dates, end_date)
    return sorted_events[lo:hi]


def find_nearest_event_date_fast(indicator_dates_sorted, target_date, direction='post'):
    """معادل دقیق find_nearest_event_date اما با bisect روی لیست از قبل مرتب‌شده."""
    if not indicator_dates_sorted:
        return None
    if direction == 'post':
        idx = bisect_right(indicator_dates_sorted, target_date)
        return indicator_dates_sorted[idx - 1] if idx > 0 else None
    else:
        idx = bisect_left(indicator_dates_sorted, target_date)
        return indicator_dates_sorted[idx] if idx < len(indicator_dates_sorted) else None


def find_adjacent_event_date_fast(indicator_dates_sorted, anchor_date, direction):
    """معادل دقیق find_adjacent_event_date اما با bisect روی لیست از قبل مرتب‌شده."""
    if not indicator_dates_sorted:
        return None
    if direction == 'post':
        idx = bisect_right(indicator_dates_sorted, anchor_date)
        return indicator_dates_sorted[idx] if idx < len(indicator_dates_sorted) else None
    else:
        idx = bisect_left(indicator_dates_sorted, anchor_date)
        return indicator_dates_sorted[idx - 1] if idx > 0 else None


def build_fibonacci_sub_periods(gap_start, gap_end):
    total_days = (gap_end - gap_start).days + 1
    if total_days <= 0:
        return []

    boundaries = [0]
    for ratio in FIBONACCI_RATIOS:
        offset = round(ratio * total_days)
        offset = max(boundaries[-1], min(offset, total_days))
        boundaries.append(offset)
    if boundaries[-1] != total_days:
        boundaries[-1] = total_days

    sub_periods = []
    for i in range(len(boundaries) - 1):
        start_offset = boundaries[i]
        end_offset   = boundaries[i + 1]
        if end_offset <= start_offset:
            continue
        sub_start = gap_start + timedelta(days=start_offset)
        sub_end   = gap_start + timedelta(days=end_offset - 1)
        sub_periods.append((sub_start, sub_end))

    return sub_periods


def find_sub_period_for_date(date, sub_periods):
    for start, end in sub_periods:
        if start <= date <= end:
            return (start, end)
    return None


def get_period_key_from_date(date, interval, news_events, model='simple_hybrid', indicator_index=None):
    parsed = parse_interval(interval)
    if parsed is None:
        print(f"⚠️ interval ناشناخته: {interval}")
        return None

    mode = parsed[0]
    if mode == 'fixed':
        days = parsed[1]
        epoch = datetime(2000, 1, 1).date()
        delta = (date - epoch).days
        period_num = delta // days
        start = epoch + timedelta(days=period_num * days)
        end   = start + timedelta(days=days - 1)
        return f"{start.isoformat()}_{end.isoformat()}"

    indicator_key  = mode
    direction      = parsed[1]
    days           = parsed[2]
    indicator_name = INTERVAL_TO_INDICATOR.get(indicator_key)
    if not indicator_name:
        print(f"⚠️ indicator_key ناشناخته در interval: {indicator_key}")
        return None

    if days == 0:
        return date.isoformat()

    # اگر ایندکس از قبل ساخته‌شده در دسترس بود، از جستجوی دودویی O(log E) استفاده
    # کن؛ در غیر این صورت (سازگاری با عقب) به همان اسکن خطی اصلی fallback کن.
    if indicator_index is not None:
        indicator_dates_sorted = indicator_index.get(indicator_name, [])
        event_date = find_nearest_event_date_fast(indicator_dates_sorted, date, direction)
    else:
        event_date = find_nearest_event_date(news_events, date, indicator_name, direction)
    if event_date is None:
        return None

    if model == 'simple_hybrid':
        if direction == 'post':
            start = event_date + timedelta(days=1)
            end   = start + timedelta(days=days - 1)
        else:
            end   = event_date - timedelta(days=1)
            start = end - timedelta(days=days - 1)
        return f"{start.isoformat()}_{end.isoformat()}"

    if indicator_index is not None:
        adjacent_event_date = find_adjacent_event_date_fast(indicator_dates_sorted, event_date, direction)
    else:
        adjacent_event_date = find_adjacent_event_date(news_events, event_date, indicator_name, direction)
    if adjacent_event_date is None:
        if direction == 'post':
            start = event_date + timedelta(days=1)
            end   = start + timedelta(days=days - 1)
        else:
            end   = event_date - timedelta(days=1)
            start = end - timedelta(days=days - 1)
        return f"{start.isoformat()}_{end.isoformat()}"

    if direction == 'post':
        gap_start = event_date + timedelta(days=1)
        gap_end   = adjacent_event_date - timedelta(days=1)
    else:
        gap_start = adjacent_event_date + timedelta(days=1)
        gap_end   = event_date - timedelta(days=1)

    gap_total_days = (gap_end - gap_start).days + 1
    if gap_total_days <= 0:
        return None

    if model == 'fibonacci_full':
        sub_periods = build_fibonacci_sub_periods(gap_start, gap_end)
        match = find_sub_period_for_date(date, sub_periods)
        if match is None:
            return None
        start, end = match
        return f"{start.isoformat()}_{end.isoformat()}"

    if model != 'fibonacci_hybrid':
        print(f"⚠️ مدل ناشناخته در get_period_key_from_date: {model}")
        return None

    # fibonacci_hybrid
    if gap_total_days <= days:
        return f"{gap_start.isoformat()}_{gap_end.isoformat()}"

    if direction == 'post':
        fixed_start = gap_start
        fixed_end   = fixed_start + timedelta(days=days - 1)
        remainder_start = fixed_end + timedelta(days=1)
        remainder_end   = gap_end
    else:
        fixed_end   = gap_end
        fixed_start = fixed_end - timedelta(days=days - 1)
        remainder_start = gap_start
        remainder_end   = fixed_start - timedelta(days=1)

    if fixed_start <= date <= fixed_end:
        return f"{fixed_start.isoformat()}_{fixed_end.isoformat()}"

    sub_periods = build_fibonacci_sub_periods(remainder_start, remainder_end)
    match = find_sub_period_for_date(date, sub_periods)
    if match is None:
        return None
    start, end = match
    return f"{start.isoformat()}_{end.isoformat()}"


def compute_indicator_status_for_period(start_date, end_date, news_events,
                                         sorted_index=None):
    if sorted_index is not None:
        sorted_dates, sorted_events = sorted_index
        events_in_range = _events_in_range_fast(sorted_dates, sorted_events, start_date, end_date)
    else:
        events_in_range = [ev for ev in news_events
                           if start_date <= ev["date"] <= end_date]
    events_by_indicator = defaultdict(list)
    for ev in events_in_range:
        events_by_indicator[ev["indicator"]].append(ev)

    status = {}
    for ind in INDICATORS:
        evs = events_by_indicator.get(ind, [])
        if not evs:
            status[ind] = None
            continue
        diffs = []
        for ev in evs:
            if ev["actual"] is not None and ev["forecast"] is not None:
                diffs.append(ev["actual"] - ev["forecast"])
        if not diffs:
            status[ind] = {thr: 'Neutral' for thr in THRESHOLDS}
            continue
        avg_diff = statistics.mean(diffs)
        status[ind] = {}
        for thr in THRESHOLDS:
            if avg_diff > thr:
                status[ind][thr] = 'Bad'
            elif avg_diff < -thr:
                status[ind][thr] = 'Good'
            else:
                status[ind][thr] = 'Neutral'
    return status


# ================================ محاسبه بازده بر اساس مدل ================================

def compute_trade_profit(raw_profit, move_percents, model, trade, use_tp_sl=False, take_profit=None, stop_loss=None):
    """
    محاسبه بازده معامله:
    - حالت skeep (use_tp_sl=True): بازده خام با TP/SL محدود می‌شود.
    - حالت عادی: apply_special_rounding اجباری است.
    """
    if use_tp_sl:
        result = raw_profit
        if take_profit is not None and result >= take_profit:
            result = take_profit
        if stop_loss is not None and result <= -stop_loss:
            result = -stop_loss
        return result

    if not move_percents:
        return raw_profit
    return apply_special_rounding(raw_profit, move_percents)


# ================================ تابع اصلی تحلیل ================================

def load_skeep_tp_sl(trades_json_path):
    base_dir = os.path.dirname(os.path.abspath(trades_json_path))
    enc_path = trades_json_path

    if not os.path.exists(enc_path):
        return False, None, None

    skeep_path = os.path.join(base_dir, "skeepmove_percents.txt")
    if not os.path.exists(skeep_path):
        return False, None, None

    take_profit = None
    stop_loss   = None
    try:
        with open(skeep_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("take_profit"):
                    val = re.sub(r'[^\d.-]', '', line.split("=", 1)[-1])
                    take_profit = float(val) if val else None
                elif line.startswith("stop_loss"):
                    val = re.sub(r'[^\d.-]', '', line.split("=", 1)[-1])
                    stop_loss = float(val) if val else None
        print(f"📄 skeepmove_percents.txt یافت شد → take_profit={take_profit}, stop_loss={stop_loss}")
        return True, take_profit, stop_loss
    except Exception as e:
        print(f"⚠️ خطا در خواندن skeepmove_percents.txt: {e}")
        return False, None, None


# ══════════════════════════════════════════════════════════════
# سشن‌های معاملاتی (به وقت UTC) — دقیقاً هم‌راستا با SESSION_WINDOWS در
# calculators.py. اگر --session داده شود، فقط معاملاتی که ساعت ورودشان
# (entryTime، UTC) داخل این بازه باشد پردازش می‌شوند؛ اگر داده نشود (پیش‌فرض
# None)، رفتار قبلی (کل ۲۴ ساعت) دقیقاً حفظ می‌شود — سازگار با گذشته.
# ══════════════════════════════════════════════════════════════
SESSION_WINDOWS = {
    "asia_tokyo":       (0, 9),
    "london":           (8, 17),
    "newyork":          (13, 22),
    "london_ny_overlap": (13, 17),
}


def _trade_hour_utc(time_str):
    """استخراج سریع ساعت UTC (0-23) از رشته‌ی زمان معامله، بدون parse کامل
    datetime برای مسیر معمول (فرمت استاندارد ISO). در صورت شکست، None
    برمی‌گرداند تا معامله نادیده گرفته شود (نه اینکه به‌اشتباه قبول شود)."""
    s = str(time_str).strip()
    try:
        if 'T' in s:
            time_part = s.split('T', 1)[1]
        elif ' ' in s:
            time_part = s.split(' ', 1)[1]
        else:
            return None
        hh = time_part[0:2]
        if hh.isdigit():
            return int(hh)
        return None
    except Exception:
        return None


def normalize_symbol(sym):
    """
    نرمال‌سازی نماد معامله برای تطبیق با --coin.
    فایل معاملات نمادها را با پسوند تایم‌فریم ذخیره می‌کند (مثل BTCUSDT-1m)
    اما --coin این پسوند را ندارد (مثل BTCUSDT یا BTCUSDT+ETHUSDT).
    این تابع پسوند انتهایی مثل -1m/-5m/-1h/-1d را حذف می‌کند تا مقایسه درست انجام شود.
    """
    if not sym:
        return sym
    return re.sub(r'-\d+[mhdwM]$', '', str(sym).strip()).upper()


def _importance(actual, forecast, distance_days):
    if actual is None or forecast is None:
        return None
    d = distance_days if distance_days is not None else 0
    return abs(actual - forecast) * (1.0 / (d + 1))


def process_analysis(trades_json_path, news_dir, interval, target_coin,
                     chunk_start, chunk_end, output_path, model,
                     ohlc_dir=None, jsonl_out=None, min_sample_count=1,
                     session=None, strategy_folder=""):
    """
    تحلیل اصلی.
    [اولویت ۱] ohlc_dir اجباری است — بدون آن، پردازش ادامه می‌یابد اما
    market_regime همه "unknown" خواهد بود (چون enforce در main() است).
    [اولویت ۲] اگر jsonl_out داده شده باشد، فایل JSONL نیز تولید می‌شود.
    """

    # ---------- بارگذاری OHLC ----------
    ohlc_df = None
    if ohlc_dir:
        print(f"📊 بارگذاری OHLC از {ohlc_dir} ...")
        ohlc_df = load_ohlc_data(ohlc_dir)
        if len(ohlc_df) == 0:
            print("⚠️ هیچ داده OHLC یافت نشد — market_regime همه 'unknown' خواهند بود.")
    else:
        print("ℹ️ --ohlc-dir داده نشده — market_regime همه 'unknown' خواهند بود.")

    # [بهینه‌سازی سرعت] ایندکس یک‌باره‌ی OHLC به‌ازای کوین، برای compute_market_regime
    coin_ohlc_index = _build_ohlc_coin_index(ohlc_df)

    # ---------- مرحله ۰: بررسی حالت skeep ----------
    use_tp_sl, take_profit, stop_loss = load_skeep_tp_sl(trades_json_path)
    if use_tp_sl:
        print(f"🔀 حالت skeep فعال: take_profit={take_profit}, stop_loss={stop_loss}")
    else:
        print("✅ حالت عادی: apply_special_rounding اجباری برای همه معاملات")

    # ---------- مرحله ۱: بارگذاری معاملات ----------
    print(f"🔍 بارگذاری معاملات از {trades_json_path}")
    with open(trades_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        trades   = data
        metadata = None
        print("⚠️ فرمت قدیمی (بدون متادیتا).")
    elif isinstance(data, dict) and "trades" in data:
        trades   = data["trades"]
        metadata = data.get("metadata", {})
        print("✅ فرمت جدید با متادیتا.")
    else:
        raise ValueError("فایل معاملات باید آرایه JSON یا دیکشنری با کلید 'trades' باشد.")

    if not isinstance(trades, list):
        raise ValueError("فیلد 'trades' باید آرایه JSON باشد.")

    print(f"📊 تعداد کل معاملات: {len(trades)}")

    move_percents = metadata.get("move_percents", []) if metadata else []

    if not use_tp_sl:
        if move_percents:
            print(f"📈 move_percents (اجباری): {move_percents}")
        else:
            print("⚠️ move_percents خالی است — fallback به raw_profit")

    # ---------- مرحله ۲: فیلتر بر اساس کوین ----------
    # نکته: --coin پسوند تایم‌فریم ندارد (مثل BTCUSDT) اما نماد داخل فایل معاملات
    # دارد (مثل BTCUSDT-1m). ترکیب ارزی (coin+coin) از قبل به‌صورت یک موجودیت
    # آماده داخل فایل معاملات وجود ندارد؛ اینجا با نرمال‌سازی نمادها، معاملات
    # هر کوین منفرد از فایل استخراج و به‌عنوان ترکیب ساخته می‌شود.
    target_coins = [normalize_symbol(c.strip()) for c in target_coin.split('+')]

    if session:
        if session not in SESSION_WINDOWS:
            raise ValueError(f"--session نامعتبر: {session} (باید یکی از {list(SESSION_WINDOWS.keys())} باشد)")
        sess_start_h, sess_end_h = SESSION_WINDOWS[session]
        print(f"🕐 فیلتر سشن فعال: {session} (ساعت UTC {sess_start_h}:00 تا {sess_end_h}:00)")

    trade_list = []
    for t in trades:
        symbol = t.get("symbol") or t.get("pair") or t.get("coin")
        if normalize_symbol(symbol) not in target_coins:
            continue

        time_str = (t.get("entryTime") or t.get("entry_time") or
                    t.get("open_time") or t.get("time") or t.get("timestamp"))
        if not time_str:
            continue

        if session:
            hour = _trade_hour_utc(time_str)
            if hour is None or not (sess_start_h <= hour < sess_end_h):
                continue

        try:
            if 'T' in str(time_str):
                date_part = str(time_str).split('T')[0]
            elif ' ' in str(time_str):
                date_part = str(time_str).split(' ')[0]
            else:
                date_part = str(time_str)

            # [بهینه‌سازی سرعت] datetime.strptime برای هر معامله صدا زده می‌شود
            # و سربار قابل‌توجهی دارد. اگر date_part دقیقاً فرمت استاندارد و
            # zero-padded "YYYY-MM-DD" باشد (که تقریباً همیشه فرمت واقعی
            # entryTime است)، مستقیم و سریع پارس می‌کنیم. در غیر این صورت
            # (هر فرمت غیرمعمول، مثل ماه/روز تک‌رقمی که strptime هم می‌پذیرد)
            # دقیقاً به همان datetime.strptime اصلی برمی‌گردیم — یعنی رفتار
            # برای هیچ ورودی‌ای تغییر نمی‌کند، فقط مسیر معمول سریع‌تر می‌شود.
            if (len(date_part) == 10 and date_part[4] == '-' and date_part[7] == '-'
                    and date_part[0:4].isdigit() and date_part[5:7].isdigit() and date_part[8:10].isdigit()):
                trade_date = datetime(int(date_part[0:4]), int(date_part[5:7]), int(date_part[8:10])).date()
            else:
                trade_date = datetime.strptime(date_part, "%Y-%m-%d").date()
        except (ValueError, AttributeError):
            continue

        raw_profit = t.get("profitPercent", 0.0)
        try:
            raw_profit = float(raw_profit)
        except (TypeError, ValueError):
            raw_profit = 0.0

        profit = compute_trade_profit(
            raw_profit, move_percents, model, t,
            use_tp_sl=use_tp_sl,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )
        trade_list.append((trade_date, profit))

    if not trade_list:
        sess_suffix = f" (سشن {session})" if session else ""
        print(f"⚠️ هیچ معامله‌ای برای کوین {target_coin}{sess_suffix} یافت نشد.")
        _write_empty_csv(output_path)
        if jsonl_out:
            _write_jsonl(jsonl_out, [])
        return

    sess_suffix = f" | سشن={session}" if session else ""
    print(f"✅ معاملات {target_coin}{sess_suffix} با تاریخ معتبر: {len(trade_list)}")

    # ---------- مرحله ۳: بارگذاری اخبار ----------
    news_events = load_news_from_directory(news_dir)
    if not news_events:
        print("⚠️ هیچ رویداد خبری بارگذاری نشد.")
        _write_empty_csv(output_path)
        if jsonl_out:
            _write_jsonl(jsonl_out, [])
        return

    # [بهینه‌سازی سرعت] ایندکس‌سازی یک‌باره‌ی رویدادها (بدون تغییر خروجی):
    # - indicator_index: برای جستجوی nearest/adjacent event با bisect به‌جای اسکن خطی
    # - sorted_event_index: برای بازه‌جویی events_in_range با bisect
    indicator_index    = _build_indicator_date_index(news_events)
    sorted_event_index = _build_sorted_event_index(news_events)

    # ---------- مرحله ۴: گروه‌بندی معاملات بر اساس بازه زمانی ----------
    # [بهینه‌سازی سرعت] get_period_key_from_date فقط به تاریخ معامله وابسته است،
    # نه به مقدار سود آن؛ پس اگر چند معامله در یک روز باشند، بازه آن روز فقط
    # یک‌بار محاسبه و کش می‌شود (به‌جای تکرار برای هر معامله).
    period_groups = defaultdict(list)
    skipped = 0
    period_key_cache = {}
    for date, profit in trade_list:
        if date in period_key_cache:
            period_key = period_key_cache[date]
        else:
            period_key = get_period_key_from_date(
                date, interval, news_events, model=model, indicator_index=indicator_index
            )
            period_key_cache[date] = period_key
        if period_key is None:
            skipped += 1
            continue
        # [فیکس: رژیم per-trade] تاریخ دقیق معامله دیگر دور ریخته نمی‌شود.
        period_groups[period_key].append((date, profit))

    if skipped > 0:
        print(f"   ℹ️ {skipped} معامله بدون بازه معتبر نادیده گرفته شد.")

    if not period_groups:
        print("⚠️ هیچ بازه معتبری تشکیل نشد.")
        _write_empty_csv(output_path)
        if jsonl_out:
            _write_jsonl(jsonl_out, [])
        return

    print(f"📊 تعداد دوره‌های تشکیل‌شده: {len(period_groups)}")

    # [فیکس: رژیم per-trade] period_groups اکنون (trade_date, profit) است؛
    # period_returns (فقط برای CSV الگوی خبری، بی‌ربط به رژیم) مثل قبل مجموع کل دوره است.
    period_returns = {period: sum(p for _, p in profits) for period, profits in period_groups.items()}

    # ---------- مرحله ۵: محاسبه وضعیت خبری هر دوره ----------
    period_status = {}
    for period_key, ret in period_returns.items():
        if len(period_key) < 21:
            continue
        start_str = period_key[:10]
        end_str   = period_key[11:]
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date   = datetime.strptime(end_str,   "%Y-%m-%d").date()
        except ValueError:
            continue
        status = compute_indicator_status_for_period(
            start_date, end_date, news_events, sorted_index=sorted_event_index
        )
        period_status[period_key] = (status, ret, start_date, end_date)

    if not period_status:
        print("⚠️ هیچ دوره‌ای وضعیت خبری معتبر نداشت.")
        _write_empty_csv(output_path)
        if jsonl_out:
            _write_jsonl(jsonl_out, [])
        return

    print(f"✅ دوره‌های دارای وضعیت خبری: {len(period_status)}")

    # ---------- مرحله ۶: تولید ترکیب‌های شاخص/آستانه و CSV ----------
    all_combinations = []
    # [فیکس: محدود به حداکثر دوشاخصی] r=1 (همه‌ی شاخص‌های تکی) و r=2 (همه‌ی
    # ترکیبات دوبه‌دو) — ترکیبات ۳تایی به بالا دیگر اصلاً تولید نمی‌شوند.
    for r in range(1, 3):
        for subset_tuple in itertools.combinations(INDICATORS, r):
            for thr in THRESHOLDS:
                all_combinations.append((thr, list(subset_tuple)))

    total     = len(all_combinations)
    start_idx = max(0, chunk_start)
    end_idx   = min(total, chunk_end) if chunk_end is not None else total
    selected  = all_combinations[start_idx:end_idx]
    print(f"🎯 پردازش چانک: {start_idx} تا {end_idx} (تعداد {len(selected)})")

    # [موازی‌سازی] period_status را قبل از ساخت Pool در متغیر سراسری ست می‌کنیم
    # تا کارگرهای fork-شده بدون pickle سنگین به آن دسترسی داشته باشند.
    global _WORKER_PERIOD_STATUS
    _WORKER_PERIOD_STATUS = period_status

    total_selected = len(selected)
    combo_results = [None] * total_selected
    failed_indices = []
    _t0 = time.time()
    _log_every = max(1, (total_selected * 5) // 100)  # هر ~۵٪ یک لاگ پیشرفت
    _next_log = _log_every
    for _i, _combo in enumerate(selected):
        try:
            combo_results[_i] = _process_combination(_combo)
        except MemoryError:
            print(f"⚠️ MemoryError در ترکیب {_i} → ناموفق")
            failed_indices.append(_i)
        except Exception as _e:
            print(f"⚠️ خطا در ترکیب {_i}: {_e}")
            failed_indices.append(_i)
        _done = _i + 1
        if _done >= _next_log or _done == total_selected:
            _elapsed = max(time.time() - _t0, 1e-6)
            print(f"📦 پیشرفت: {_done * 100 // total_selected}% ({_done}/{total_selected}) — "
                  f"{_done / _elapsed:.1f} ترکیب/ثانیه, ناموفق={len(failed_indices)}")
            _next_log += _log_every

    if failed_indices:
        failed_combos = [selected[i] for i in failed_indices]
        print(f"⚠️ {len(failed_combos)} ترکیب ناموفق بود (MemoryError یا خطا). "
              f"پردازش بقیه ادامه یافت.")
        _write_failed_combos(output_path, failed_combos)

    csv_rows = []
    for rows in combo_results:
        if rows:
            csv_rows.extend(rows)

    _write_csv(output_path, csv_rows)

    # ---------- [اولویت ۲] تولید JSONL در صورت درخواست ----------
    if jsonl_out:
        parsed_iv = parse_interval(interval)
        indicator_key = parsed_iv[0] if parsed_iv else None
        direction     = parsed_iv[1] if parsed_iv else None
        distance_days = parsed_iv[2] if parsed_iv else None
        first_coin    = target_coins[0] if target_coins else None

        # [فیکس: رژیم per-trade] یک‌بار برای این کوین جدول رژیم روزانه
        # پیش‌محاسبه می‌شود؛ رژیم هر معامله از روی تاریخ خودش لوکاپ می‌شود.
        regime_index = _precompute_regime_series(coin_ohlc_index)

        records = []
        for period_key, (status_dict, ret, start_date, end_date) in period_status.items():
            dated_profits = period_groups.get(period_key, [])
            if len(dated_profits) < min_sample_count:
                continue

            sorted_dates, sorted_events = sorted_event_index
            events_in_range = _events_in_range_fast(sorted_dates, sorted_events, start_date, end_date)

            # ویژگی‌های خبری در سطح کل دوره محاسبه می‌شوند — بی‌ربط به رژیم، بدون تغییر.
            dominant_indicator = None
            dominant_score = -1.0
            diffs_all = []
            indicators_present = set()
            for ev in events_in_range:
                indicators_present.add(ev["indicator"])
                if ev["actual"] is None or ev["forecast"] is None:
                    continue
                diff = ev["actual"] - ev["forecast"]
                diffs_all.append(diff)
                d_days = abs((ev["date"] - start_date).days)
                score = _importance(ev["actual"], ev["forecast"], d_days)
                if score is not None and score > dominant_score:
                    dominant_score = score
                    dominant_indicator = ev["indicator"]

            period_len = (end_date - start_date).days + 1
            secondary  = sorted(indicators_present - ({dominant_indicator} if dominant_indicator else set()))

            # [فیکس: رژیم per-trade] معاملات این دوره را بر اساس رژیمِ روزِ
            # خودشان (نه رژیم ثابتِ ابتدای دوره) به زیرگروه تقسیم می‌کنیم.
            regime_buckets = defaultdict(list)
            for trade_date, profit in dated_profits:
                regime = lookup_market_regime(regime_index, first_coin, trade_date)
                regime_buckets[regime].append(profit)

            for market_regime, bucket_profits in regime_buckets.items():
                if len(bucket_profits) < min_sample_count:
                    continue
                total_return  = sum(bucket_profits)
                trade_count   = len(bucket_profits)
                avg_trade_ret = (total_return / trade_count) if trade_count else 0.0
                avg_daily_ret = (avg_trade_ret / period_len) if period_len else 0.0

                records.append({
                    "coin_composition":                target_coin,
                    "model":                           model,
                    "interval":                        interval,
                    "indicator_key":                   indicator_key,
                    "position":                        direction,
                    "distance_days":                   distance_days,
                    "period_start":                    start_date.isoformat(),
                    "period_end":                      end_date.isoformat(),
                    "period_length_days":              period_len,
                    "total_return":                    total_return,
                    "trade_count":                     trade_count,
                    "avg_trade_return":                avg_trade_ret,
                    "avg_daily_return":                avg_daily_ret,
                    "dominant_indicator":              dominant_indicator,
                    "dominant_indicator_importance":   (dominant_score if dominant_score >= 0 else None),
                    "secondary_indicators":            secondary,
                    "diff_avg":    (statistics.mean(diffs_all) if diffs_all else None),
                    "diff_std":    (statistics.pstdev(diffs_all) if len(diffs_all) > 1 else (0.0 if diffs_all else None)),
                    "event_count":         len(events_in_range),
                    "indicator_diversity": len(indicators_present),
                    "use_tp_sl":    use_tp_sl,
                    "take_profit":  take_profit,
                    "stop_loss":    stop_loss,
                    "strategy_folder": strategy_folder,
                    # [فیکس ۱۰] قبلاً این فیلد همیشه "" بود (process_analysis
                    # اصلاً پارامتر strategy_folder نداشت) — یعنی هر استراتژی
                    # پردازش‌شده با combo_10day.py توی golden.py با
                    # strategy_id="" یکسان می‌شد و همه‌ی استراتژی‌های متفاوت
                    # قاطی می‌شدند.
                    "market_regime": market_regime,
                    # [فیکس ۹] سشن معاملاتی (مثل london) باید در امضای نهایی
                    # لحاظ شود، وگرنه «استراتژی A همیشه» و «استراتژی A فقط
                    # سشن لندن» به‌عنوان یک ترکیب یکسان با هم قاطی می‌شوند.
                    "session": session if session else "none",
                })

        _write_jsonl(jsonl_out, records)


# ================================ [موازی‌سازی] پردازش هر ترکیب ================================
# period_status حجیم است و فقط-خواندنی؛ به‌جای ارسال آن به هر کار (pickle سنگین)،
# قبل از ساخت Pool در متغیر سراسری زیر ست می‌شود تا کارگرهای fork-شده آن را از
# طریق COW حافظه‌ی پردازه‌ی والد ببینند، بدون کپی یا سریالایز اضافه.
_WORKER_PERIOD_STATUS = None


def _process_combination(combo):
    """پردازش یک ترکیب (thr, subset) → لیست ردیف‌های CSV آن ترکیب.
    منطق دقیقاً همان حلقه‌ی سریال قبلی است، فقط به شکل تابع مستقل درآمده تا
    بتواند در یک پردازه‌ی کارگر اجرا شود."""
    thr, subset = combo
    period_status = _WORKER_PERIOD_STATUS

    pattern_counts = defaultdict(lambda: {
        'total': 0, 'loss': 0, 'profit': 0, 'periods': []
    })

    for period, (status_dict, ret, _sd, _ed) in period_status.items():
        valid = True
        pattern_parts = []
        for ind in subset:
            st = status_dict.get(ind)
            if st is None or thr not in st:
                valid = False
                break
            pattern_parts.append(st[thr])
        if not valid:
            continue

        pattern = tuple(pattern_parts)
        pattern_counts[pattern]['total'] += 1
        pattern_counts[pattern]['periods'].append(period)
        if ret < 0:
            pattern_counts[pattern]['loss'] += 1
        else:
            pattern_counts[pattern]['profit'] += 1

    rows = []
    for pattern_tuple, counts in pattern_counts.items():
        total_cnt  = counts['total']
        loss_cnt   = counts['loss']
        profit_cnt = counts['profit']
        loss_pct   = (loss_cnt / total_cnt) * 100 if total_cnt > 0 else 0
        profit_pct = (profit_cnt / total_cnt) * 100 if total_cnt > 0 else 0
        odds       = (loss_pct / profit_pct) if profit_pct > 0 else float('inf')

        rows.append({
            'آستانه':          thr,
            'تعداد_شاخص‌ها':   len(subset),
            'لیست_شاخص‌ها':    '|'.join(subset),
            'الگوی_وضعیت':     '|'.join(pattern_tuple),
            'تعداد_کل_وقوع':   total_cnt,
            'تعداد_ضررده':     loss_cnt,
            'تعداد_سودده':     profit_cnt,
            'درصد_ضررده':      round(loss_pct, 1),
            'درصد_سودده':      round(profit_pct, 1),
            'نسبت_شانس':       odds if odds == float('inf') else round(odds, 2),
            'دوره‌ها':         '|'.join(counts['periods']),
        })
    return rows


def _write_failed_combos(output_path, failed_combos):
    """لیست ترکیب‌های ناموفق (MemoryError یا خطای دیگر) را برای اجرای مجدد در
    ران بعدی ذخیره می‌کند. YAML یا هیچ فایل دیگری تغییر نمی‌کند؛ صرفاً یک
    فایل کنار خروجی نوشته می‌شود که در صورت نیاز می‌توان بعداً مصرف کرد."""
    retry_path = output_path + ".failed_combos.json"
    try:
        with open(retry_path, "w", encoding="utf-8") as f:
            json.dump(
                [{"threshold": thr, "indicators": subset} for thr, subset in failed_combos],
                f, ensure_ascii=False, indent=2
            )
        print(f"📝 لیست ترکیب‌های ناموفق ذخیره شد: {retry_path}")
    except Exception as e:
        print(f"⚠️ ذخیره لیست ترکیب‌های ناموفق ممکن نشد: {e}")


def _write_empty_csv(output_path):
    _write_csv(output_path, [])


def _write_csv(output_path, csv_rows):
    fieldnames = [
        'آستانه', 'تعداد_شاخص‌ها', 'لیست_شاخص‌ها', 'الگوی_وضعیت',
        'تعداد_کل_وقوع', 'تعداد_ضررده', 'تعداد_سودده',
        'درصد_ضررده', 'درصد_سودده', 'نسبت_شانس', 'دوره‌ها',
    ]
    parent_dir = os.path.dirname(output_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if csv_rows:
            writer.writerows(csv_rows)

    if csv_rows:
        print(f"✅ {len(csv_rows)} ردیف ذخیره شد: {output_path}")
    else:
        print(f"⚠️ فایل خالی ایجاد شد (داده‌ای یافت نشد): {output_path}")


def _write_jsonl(out_path, records):
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"✅ {len(records)} رکورد JSONL ذخیره شد: {out_path}")


# ================================ main ================================

def main():
    parser = argparse.ArgumentParser(
        description="تحلیل ترکیبی الگوهای خبری (combo_10day) — هم CSV هم JSONL"
    )
    parser.add_argument("--trades-json",      required=True)
    parser.add_argument("--news-dir",         required=True)
    parser.add_argument("--interval",         required=True)
    parser.add_argument("--chunk-start",      type=int, default=0)
    parser.add_argument("--chunk-end",        type=int, default=None)
    parser.add_argument("--strategy-folder",  required=True)
    parser.add_argument("--coin",             required=True)
    parser.add_argument("--model",            required=True, choices=VALID_MODELS)
    parser.add_argument("--session",          required=False, default=None,
                        choices=list(SESSION_WINDOWS.keys()),
                        help="اختیاری. اگر داده شود فقط معاملات داخل بازه‌ی ساعتی این سشن "
                             "(UTC) پردازش می‌شوند. اگر داده نشود، کل ۲۴ ساعت (رفتار قبلی) پردازش می‌شود.")

    # [اولویت ۱] --ohlc-dir اجباری است
    parser.add_argument("--ohlc-dir", required=True,
                        help="مسیر پوشه CSVهای OHLC روزانه (هر فایل = یک کوین). اجباری است.")

    # [اولویت ۲] تولید JSONL
    parser.add_argument("--jsonl-out", default=None,
                        help="مسیر خروجی JSONL (امضاهای per-period). اختیاری.")
    parser.add_argument("--min-sample-count", type=int, default=1,
                        help="حداقل تعداد معامله در هر دوره برای ثبت در JSONL.")

    args = parser.parse_args()

    # [اولویت ۱] اجبار OHLC: اگر پوشه وجود نداشت یا خالی بود، exit 1
    if not os.path.isdir(args.ohlc_dir):
        print(f"❌ [OHLC اجباری] پوشه --ohlc-dir یافت نشد: {args.ohlc_dir}")
        print("❌ pipeline باید fail شود: داده OHLC وجود ندارد.")
        sys.exit(1)

    csv_count = len(glob.glob(os.path.join(args.ohlc_dir, "*.csv")))
    if csv_count == 0:
        print(f"❌ [OHLC اجباری] پوشه --ohlc-dir خالی است (هیچ CSV یافت نشد): {args.ohlc_dir}")
        print("❌ pipeline باید fail شود: داده OHLC وجود ندارد.")
        sys.exit(1)

    print(f"✅ [OHLC] پوشه معتبر یافت شد با {csv_count} فایل CSV: {args.ohlc_dir}")

    session_suffix = f"_{args.session}" if args.session else ""
    output_file = f"{args.strategy_folder}_{args.coin}_{args.interval}_{args.model}{session_suffix}.csv"
    output_path = os.path.join(os.getcwd(), output_file)

    process_analysis(
        trades_json_path=args.trades_json,
        news_dir=args.news_dir,
        interval=args.interval,
        target_coin=args.coin,
        chunk_start=args.chunk_start,
        chunk_end=args.chunk_end,
        output_path=output_path,
        model=args.model,
        ohlc_dir=args.ohlc_dir,
        jsonl_out=args.jsonl_out,
        min_sample_count=args.min_sample_count,
        session=args.session,
        strategy_folder=args.strategy_folder,
    )


if __name__ == "__main__":
    main()
