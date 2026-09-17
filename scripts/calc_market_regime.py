#!/usr/bin/env python3
# calc_market_regime.py - محاسبه‌ی رژیم بازار فعلی برای همه‌ی کوین‌ها
#
# منطق و فرمول دقیقاً از combo_10day.py / combo_monthly.py کپی شده
# (توابع extract_coin_name / load_ohlc_data / _build_ohlc_coin_index /
# compute_market_regime بدون هیچ تغییری در فرمول). این اسکریپت فقط یک
# لایه‌ی نازک اضافه می‌کند که به‌جای رژیم هر معامله، رژیمِ "الان" (آخرین
# روز موجود در OHLC) را برای تک‌تک کوین‌ها حساب و ذخیره می‌کند.

import os
import sys
import json
import glob
import argparse
import re
from bisect import bisect_right
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np


# ═══════════════════════════════════════════════════════════════════════
# از اینجا تا انتهای بخش، عیناً از combo_10day.py / combo_monthly.py
# کپی شده — همان فرمول، بدون تغییر.
# ═══════════════════════════════════════════════════════════════════════

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
    بارگذاری داده‌های OHLC از پوشه (کپی دقیق از combo_10day.py).
    - فایل‌های OHLC ممکن است به‌صورت تکه‌تکه برای یک کوین ذخیره شده باشند؛
      نام واقعی کوین از روی نام فایل استخراج می‌شود و تمام تکه‌های مربوط به
      یک کوین قبل از resample با هم ادغام می‌شوند.
    - ستون‌های date یا timestamp را می‌پذیرد.
    - تایم‌فریم بر اساس میانگین فاصله‌ی زمانی بین رکوردهای ادغام‌شده تشخیص
      داده می‌شود؛ اگر زیر-روزانه بود، resample روزانه اعمال می‌شود.
    """
    columns = ["date", "coin", "open", "high", "low", "close"]
    if not ohlc_dir or not os.path.isdir(ohlc_dir):
        return pd.DataFrame(columns=columns)

    csv_paths = sorted(glob.glob(os.path.join(ohlc_dir, "*.csv")))
    if not csv_paths:
        return pd.DataFrame(columns=columns)

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

        if "date" not in df.columns and "timestamp" in df.columns:
            df = df.rename(columns={"timestamp": "date"})

        required = {"date", "open", "high", "low", "close"}
        if not required.issubset(set(df.columns)):
            print(f"   ⚠️ [{fname}] ستون‌های لازم یافت نشد → نادیده گرفته شد.")
            continue

        df = df[["date", "open", "high", "low", "close"]].copy()
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
        df = pd.concat(parts, ignore_index=True)
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        if len(df) >= 2:
            time_diffs = df["date"].diff().dropna()
            avg_diff_minutes = time_diffs.dt.total_seconds().mean() / 60.0
        else:
            avg_diff_minutes = 1440.0

        is_intraday = avg_diff_minutes < 60 * 23

        if is_intraday:
            timeframe_str = (
                f"{int(avg_diff_minutes)}دقیقه‌ای" if avg_diff_minutes < 60
                else f"{avg_diff_minutes/60:.1f}ساعته"
            )
            print(f"   ⏱️ [{coin}] {len(parts)} فایل ادغام شد → تایم‌فریم: ~{timeframe_str} "
                  f"(میانگین فاصله {avg_diff_minutes:.1f} دقیقه) → resample به روزانه روی کل سری")

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
    return combined


def _build_ohlc_coin_index(ohlc_df):
    """پیش‌محاسبه یک‌باره: به‌ازای هر کوین، آرایه‌های numpy از close/high/low
    همراه با لیست تاریخ‌ها (کپی دقیق از combo_10day.py)."""
    index = {}
    if ohlc_df is None or len(ohlc_df) == 0:
        return index
    for coin, sub in ohlc_df.groupby("coin", sort=False):
        sub_sorted = sub.reset_index(drop=True)
        dates_list = list(sub_sorted["date"])
        close_arr = sub_sorted["close"].to_numpy(dtype="float64")
        high_arr = sub_sorted["high"].to_numpy(dtype="float64")
        low_arr = sub_sorted["low"].to_numpy(dtype="float64")
        index[coin] = (dates_list, close_arr, high_arr, low_arr)
    return index


def compute_market_regime(coin_ohlc_index, coin, start_date):
    """
    رژیم بازار را برای یک کوین مشخص، صرفاً بر اساس داده‌های قیمت *قبل* از
    start_date محاسبه می‌کند (بدون آینده‌نگری). کپی دقیق از combo_10day.py /
    combo_monthly.py — همان فرمول MA50 / MA200 / ATR14.
    """
    if not coin_ohlc_index or coin is None:
        return "unknown", {}

    entry = coin_ohlc_index.get(coin)
    if entry is None:
        return "unknown", {}
    dates_list, close_arr, high_arr, low_arr = entry
    if len(dates_list) == 0:
        return "unknown", {}

    cutoff = pd.Timestamp(start_date) - timedelta(days=1)
    idx = bisect_right(dates_list, cutoff)
    if idx == 0 or idx < 200:
        return "unknown", {}

    close_200 = close_arr[idx - 200:idx]
    close_50 = close_arr[idx - 50:idx]
    high_14 = high_arr[idx - 14:idx]
    low_14 = low_arr[idx - 14:idx]

    ma50 = np.nanmean(close_50)
    ma200 = np.nanmean(close_200)
    atr = np.nanmean(high_14 - low_14)
    price = close_arr[idx - 1]

    diag = {
        "as_of_date": dates_list[idx - 1].strftime("%Y-%m-%d"),
        "close": float(price) if price is not None and not pd.isna(price) else None,
        "ma50": float(ma50) if not pd.isna(ma50) else None,
        "ma200": float(ma200) if not pd.isna(ma200) else None,
        "atr14": float(atr) if not pd.isna(atr) else None,
    }

    if price is None or pd.isna(price) or price == 0:
        return "unknown", diag
    if pd.isna(ma50) or pd.isna(ma200) or pd.isna(atr):
        return "unknown", diag

    if (atr / price) > 0.02:
        return "volatile", diag
    if ma50 > ma200:
        return "trending_up", diag
    if ma50 < ma200:
        return "trending_down", diag
    if abs(ma50 - ma200) / price < 0.05:
        return "ranging", diag
    return "unknown", diag


# ═══════════════════════════════════════════════════════════════════════
# پایان بخش کپی‌شده. از اینجا به بعد کد مخصوص این اسکریپت است.
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="محاسبه‌ی رژیم بازار فعلی برای همه‌ی کوین‌ها")
    parser.add_argument("--ohlc-dir", required=True, help="پوشه‌ی حاوی فایل‌های CSV خام OHLC")
    parser.add_argument("--output", required=True, help="مسیر فایل JSON خروجی")
    args = parser.parse_args()

    print(f"📦 بارگذاری OHLC از: {args.ohlc_dir}")
    ohlc_df = load_ohlc_data(args.ohlc_dir)
    if ohlc_df.empty:
        print("❌ هیچ داده OHLC معتبری یافت نشد.")
        sys.exit(1)

    coin_index = _build_ohlc_coin_index(ohlc_df)
    print(f"🪙 تعداد کوین‌ها: {len(coin_index)}")

    results = []
    for coin, (dates_list, close_arr, high_arr, low_arr) in sorted(coin_index.items()):
        n = len(dates_list)
        if n < 200:
            # کمتر از ۲۰۰ روز → هیچ روزی قابل‌محاسبه نیست (MA200 ندارد)
            continue

        coin_days = 0
        # برای هر روزی که حداقل ۲۰۰ روز قبل از آن (شامل خودش) داده وجود دارد،
        # دقیقاً همان فرمول compute_market_regime را با idx متناظر همان روز
        # اجرا می‌کنیم (idx چنان انتخاب می‌شود که close_arr[idx-1] == قیمت
        # همان روز باشد — بدون آینده‌نگری، چون MA/ATR فقط از idx-200..idx-1
        # و idx-50..idx-1 و idx-14..idx-1 ساخته می‌شوند).
        for idx in range(200, n + 1):
            close_200 = close_arr[idx - 200:idx]
            close_50 = close_arr[idx - 50:idx]
            high_14 = high_arr[idx - 14:idx]
            low_14 = low_arr[idx - 14:idx]

            ma50 = np.nanmean(close_50)
            ma200 = np.nanmean(close_200)
            atr = np.nanmean(high_14 - low_14)
            price = close_arr[idx - 1]
            as_of_date = dates_list[idx - 1]

            diag = {
                "as_of_date": as_of_date.strftime("%Y-%m-%d"),
                "close": float(price) if price is not None and not pd.isna(price) else None,
                "ma50": float(ma50) if not pd.isna(ma50) else None,
                "ma200": float(ma200) if not pd.isna(ma200) else None,
                "atr14": float(atr) if not pd.isna(atr) else None,
            }

            if price is None or pd.isna(price) or price == 0 or pd.isna(ma50) or pd.isna(ma200) or pd.isna(atr):
                regime = "unknown"
            elif (atr / price) > 0.02:
                regime = "volatile"
            elif ma50 > ma200:
                regime = "trending_up"
            elif ma50 < ma200:
                regime = "trending_down"
            elif abs(ma50 - ma200) / price < 0.05:
                regime = "ranging"
            else:
                regime = "unknown"

            row = {"coin": coin, "regime": regime}
            row.update(diag)
            results.append(row)
            coin_days += 1

        print(f"   → {coin}: {coin_days} روز محاسبه شد "
              f"(از {dates_list[199].strftime('%Y-%m-%d')} تا {dates_list[-1].strftime('%Y-%m-%d')})")

    output = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "coin_count": len(coin_index),
        "record_count": len(results),
        "regimes": results,
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ رژیم {len(results)} رکورد روزانه برای {len(coin_index)} کوین در {args.output} ذخیره شد.")


if __name__ == "__main__":
    main()
