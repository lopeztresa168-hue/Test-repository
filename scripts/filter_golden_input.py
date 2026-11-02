#!/usr/bin/env python3
import sys, json, os, csv, re

def main():
    if len(sys.argv) != 3:
        print("Usage: python filter_golden_input.py <input_dir> <output_json>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_json = sys.argv[2]

    valid_strategies = []

    # پیمایش فایل‌های CSV در پوشه (فرض می‌کنیم فایل‌ها با .csv ختم می‌شوند)
    for fname in os.listdir(input_dir):
        if not fname.endswith(".csv"):
            continue
        # استخراج WinRate و Profit از نام فایل با الگوی _WinRateXX.X_ProfitYY.Y_
        match = re.search(r'_WinRate(\d+\.?\d*)_Profit(-?\d+\.?\d*)', fname)
        if match:
            winrate = float(match.group(1))
            profit = float(match.group(2))
            if winrate >= 20.0 and profit > 0.0:
                # استخراج نام استراتژی از ابتدای نام فایل (الگوی combo_10day_<Strategy>_...)
                parts = fname.split('_')
                if len(parts) >= 2:
                    strategy_name = parts[1]
                    valid_strategies.append(strategy_name)

    # حذف تکراری‌ها
    valid_strategies = list(set(valid_strategies))

    with open(output_json, 'w') as f:
        json.dump({"valid_strategies": valid_strategies}, f)

if __name__ == "__main__":
    main()