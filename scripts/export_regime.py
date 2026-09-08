#!/usr/bin/env python3
# export_regime.py
# هدف: محاسبه رژیم بازار روزانه (market_regime) دقیقاً با همان فرمول
# compute_market_regime در combo_monthly.py، و ذخیره کل تاریخچه به‌صورت CSV
# قابل دانلود (به‌جای این‌که فقط داخل پایپ‌لاین اصلی مصرف و دور ریخته شود).
#
# این فایل هیچ فایل موجودی را تغییر نمی‌دهد؛ کاملاً مستقل و جدید است.
#
# منطق OHLC-loading و compute_market_regime عیناً از combo_monthly.py کپی
# شده تا خروجی ۱۰۰٪ با چیزی که پایپ‌لاین اصلی مصرف می‌کند یکسان باشد.

import os
import sys
import glob
import argparse
import pandas as pd
import numpy as np
from collections import defaultdict
from bisect import bisect_right
from datetime import timedelta


# ================================ (کپی عیناً از combo_monthly.py) ================================

def extract_coin_name(filename):
    base = os.path.splitext(os.path.basename(filename))[0]
    import re
    match = re.match(r'^(.+?)-\d+[mhdwM]-', base)
    if match:
        return match.group(1).upper()
    return base


def load_ohlc_data(ohlc_dir):
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

        if len(df) < 200:
            print(f"   ⚠️ [{coin}] تعداد کل روزهای OHLC ({len(df)}) کمتر از ۲۰۰ است → "
                  f"market_regime این کوین 'unknown' خواهد بود.")

        df["coin"] = coin
        frames.append(df[columns])

    if not frames:
        return pd.DataFrame(columns=columns)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["coin", "date"]).reset_index(drop=True)
    return combined


def _build_ohlc_coin_index(ohlc_df):
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
    """عیناً از combo_monthly.py — بدون هیچ تغییری."""
    if not coin_ohlc_index or coin is None:
        return "unknown", None, None, None

    entry = coin_ohlc_index.get(coin)
    if entry is None:
        return "unknown", None, None, None
    dates_list, close_arr, high_arr, low_arr = entry
    if len(dates_list) == 0:
        return "unknown", None, None, None

    cutoff = pd.Timestamp(start_date) - timedelta(days=1)
    idx = bisect_right(dates_list, cutoff)
    if idx == 0 or idx < 200:
        return "unknown", None, None, None

    close_200 = close_arr[idx - 200:idx]
    close_50 = close_arr[idx - 50:idx]
    high_14 = high_arr[idx - 14:idx]
    low_14 = low_arr[idx - 14:idx]

    ma50 = np.nanmean(close_50)
    ma200 = np.nanmean(close_200)
    atr = np.nanmean(high_14 - low_14)
    price = close_arr[idx - 1]

    if price is None or pd.isna(price) or price == 0:
        return "unknown", None, None, None
    if pd.isna(ma50) or pd.isna(ma200) or pd.isna(atr):
        return "unknown", None, None, None

    atr_ratio = atr / price

    if atr_ratio > 0.02:
        return "volatile", ma50, ma200, atr_ratio
    if ma50 > ma200:
        return "trending_up", ma50, ma200, atr_ratio
    if ma50 < ma200:
        return "trending_down", ma50, ma200, atr_ratio
    if abs(ma50 - ma200) / price < 0.05:
        return "ranging", ma50, ma200, atr_ratio
    return "unknown", ma50, ma200, atr_ratio


# ================================ خروجی کامل تاریخچه ================================

def export_full_regime_history(ohlc_dir, coins, output_csv):
    print(f"📂 بارگذاری OHLC از {ohlc_dir} ...")
    ohlc_df = load_ohlc_data(ohlc_dir)
    if len(ohlc_df) == 0:
        print("❌ هیچ داده OHLC یافت نشد.")
        sys.exit(1)

    all_coins_available = sorted(ohlc_df["coin"].unique())
    print(f"✅ کوین‌های موجود در OHLC: {all_coins_available}")

    coin_index = _build_ohlc_coin_index(ohlc_df)

    if coins:
        target_coins = [c.upper() for c in coins]
    else:
        target_coins = all_coins_available

    rows = []
    for coin in target_coins:
        if coin not in coin_index:
            print(f"⚠️ کوین {coin} در OHLC یافت نشد → رد شد.")
            continue
        dates_list, close_arr, high_arr, low_arr = coin_index[coin]
        print(f"🧮 محاسبه رژیم روزانه برای {coin} ({len(dates_list)} روز)...")
        for i, d in enumerate(dates_list):
            # start_date = خود روز d (رفتار compute_market_regime، بدون آینده‌نگری:
            # فقط داده‌ی قبل از d استفاده می‌شود، دقیقاً مثل لوکاپ per-trade در
            # combo_monthly.py که trade_date را به‌عنوان start_date می‌دهد)
            regime, ma50, ma200, atr_ratio = compute_market_regime(coin_index, coin, d)
            rows.append({
                "coin": coin,
                "date": d.date().isoformat(),
                "close": close_arr[i],
                "ma50": ma50,
                "ma200": ma200,
                "atr14_over_price": atr_ratio,
                "market_regime": regime,
            })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(output_csv, index=False)
    print(f"✅ {len(out_df)} ردیف ذخیره شد: {output_csv}")

    # خلاصه‌ی سریع برای لاگ
    if len(out_df):
        print("\n📊 خلاصه رژیم به تفکیک کوین:")
        print(out_df.groupby(["coin", "market_regime"]).size())


def main():
    parser = argparse.ArgumentParser(
        description="خروجی کامل تاریخچه رژیم روزانه بازار (market_regime) برای یک یا چند کوین"
    )
    parser.add_argument("--ohlc-dir", required=True,
                         help="پوشه CSVهای OHLC (مثل data/All_Coins_Combined از OHLC_REPO)")
    parser.add_argument("--coins", nargs="*", default=None,
                         help="لیست کوین‌ها (مثل XRPUSDT BTCUSDT). اگر داده نشود، همه کوین‌های موجود.")
    parser.add_argument("--output", default="market_regime_history.csv")
    args = parser.parse_args()

    export_full_regime_history(args.ohlc_dir, args.coins, args.output)


if __name__ == "__main__":
    main()
