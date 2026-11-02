#!/usr/bin/env python3
# deep.py - تحلیل عمیق یک استراتژی: بررسی ماهانه و اخبار مرتبط با هر فایل
import os
import sys
import json
import argparse
import pickle
import re
from datetime import datetime

# ================================ توابع کمکی ================================
def extract_start_date_from_period(period_str):
    match = re.search(r'(\d{4}-\d{2}-\d{2})_', period_str)
    return match.group(1) if match else None

def extract_end_date_from_period(period_str):
    match = re.search(r'_(\d{4}-\d{2}-\d{2})$', period_str)
    return match.group(1) if match else None

def find_events_in_range(events, start_date_str, end_date_str):
    try:
        start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except:
        return []
    return [ev for ev in events if start <= ev["date"] <= end]

def load_news_data(pickle_path):
    with open(pickle_path, "rb") as f:
        return pickle.load(f)

def load_strategy_results(results_base_dir, group, folder_name, returns_cache_json):
    """بارگذاری فایل‌های JSON یک استراتژی همراه با بازده از کش"""
    folder_path = os.path.join(results_base_dir, group, folder_name)
    if not os.path.isdir(folder_path):
        return []
    returns_cache = {}
    if os.path.exists(returns_cache_json):
        with open(returns_cache_json, "r", encoding="utf-8") as f:
            returns_cache = json.load(f)
    results = []
    for fname in os.listdir(folder_path):
        if fname.endswith(".json") and fname != "1.json":
            file_path = os.path.join(folder_path, fname)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                info = data.get("اطلاعات_فایل", {}) or {}
                period_name = info.get("نام_فایل", "")
                key = f"{group}_{folder_name}_{fname}"
                if key in returns_cache:
                    ret = returns_cache[key].get("special", 0) if group == "1" else returns_cache[key].get("real", 0)
                else:
                    ret = float(info.get("بازدهی_کل", "0%").replace("%", ""))
                results.append({
                    "file_name": fname,
                    "period_name": period_name,
                    "return": ret,
                })
            except:
                continue
    return results

def compute_monthly_records_from_files(files, group):
    """گروه‌بندی فایل‌ها به ماه‌های سه‌تایی (روزهای 1، 11، 21)"""
    monthly_files = {}
    for r in files:
        start = extract_start_date_from_period(r["period_name"])
        if not start:
            continue
        ym = start[:7]
        monthly_files.setdefault(ym, []).append(r)
    monthly_records = []
    for ym, flist in monthly_files.items():
        selected = []
        for day in [1, 11, 21]:
            target = f"{ym}-{day:02d}"
            found = next((f for f in flist if extract_start_date_from_period(f["period_name"]) == target), None)
            if found:
                selected.append(found)
        if len(selected) != 3:
            continue
        total_return = sum(f["return"] for f in selected)
        start_dates = [extract_start_date_from_period(f["period_name"]) for f in selected]
        end_dates = [extract_end_date_from_period(f["period_name"]) for f in selected]
        monthly_records.append({
            "year_month": ym,
            "total_return": total_return,
            "files": [f["file_name"] for f in selected],
            "file_objects": selected,
            "start_date": min(start_dates),
            "end_date": max(end_dates),
        })
    monthly_records.sort(key=lambda x: x["year_month"])
    return monthly_records

# ================================ تحلیل عمیق ================================
def create_deep_analysis(strategy_folder, group, results_base_dir, news_events, returns_cache_json, output_dir):
    """
    ایجاد فایل متنی تحلیل عمیق استراتژی با ساختار درختی:
    - برای هر ماه، سه فایل و بازده کل ماه
    - برای هر فایل، بازه و اخبار مرتبط
    """
    print(f"🔄 تحلیل عمیق برای {strategy_folder} (گروه {group})...")
    # بارگذاری فایل‌های استراتژی
    files = load_strategy_results(results_base_dir, group, strategy_folder, returns_cache_json)
    if not files:
        print("⚠️ هیچ فایل نتیجه‌ای یافت نشد.")
        return
    # محاسبه رکوردهای ماهانه
    monthly = compute_monthly_records_from_files(files, group)
    if not monthly:
        print("⚠️ هیچ رکورد ماهانه معتبری یافت نشد.")
        return
    # ساخت مسیر خروجی
    out_filename = f"تحلیل_عمیق_{strategy_folder}.txt"
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, out_filename)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("="*60 + "\n")
        f.write(f"📊 تحلیل عمیق استراتژی: {strategy_folder} (گروه {group})\n")
        f.write(f"تاریخ تولید: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*60 + "\n\n")
        for rec in monthly:
            f.write(f"📅 ماه {rec['year_month']} (بازده ماه: {rec['total_return']:.2f}%)\n")
            for file_obj in rec["file_objects"]:
                start = extract_start_date_from_period(file_obj["period_name"])
                end = extract_end_date_from_period(file_obj["period_name"])
                ret = file_obj["return"]
                f.write(f"    📄 فایل: {file_obj['file_name']}\n")
                f.write(f"        📅 بازه: {start} تا {end}\n")
                f.write(f"        📈 بازده فایل: {ret:.2f}%\n")
                f.write(f"        📰 اخبار مرتبط:\n")
                events_in_range = find_events_in_range(news_events, start, end)
                if events_in_range:
                    for ev in events_in_range:
                        details = f"{ev['indicator']}"
                        if ev['actual'] is not None:
                            details += f" Actual: {ev['actual']}%"
                        if ev['forecast'] is not None:
                            details += f" Forecast: {ev['forecast']}%"
                        if ev['previous'] is not None:
                            details += f" Previous: {ev['previous']}%"
                        f.write(f"            • {ev['date']}: {details}\n")
                else:
                    f.write(f"            (ندارد)\n")
            f.write("\n")
    print(f"✅ تحلیل عمیق ذخیره شد: {out_path}")

# ================================ اصلی ================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-folder", required=True, help="نام پوشه استراتژی")
    parser.add_argument("--group", required=True, choices=["1", "2"])
    parser.add_argument("--results-base", required=True, help="مسیر پایه نتایج بکتست (شامل 1 و 2)")
    parser.add_argument("--news-pickle", required=True, help="مسیر فایل pickle اخبار")
    parser.add_argument("--returns-cache", required=True, help="مسیر کش بازده (JSON)")
    parser.add_argument("--output-dir", required=True, help="پوشه خروجی نهایی (final_output)")
    args = parser.parse_args()

    # بارگذاری اخبار
    print("🔄 بارگذاری اخبار...")
    news = load_news_data(args.news_pickle)

    # تحلیل عمیق
    create_deep_analysis(
        strategy_folder=args.strategy_folder,
        group=args.group,
        results_base_dir=args.results_base,
        news_events=news,
        returns_cache_json=args.returns_cache,
        output_dir=args.output_dir
    )
    print("✅ ماژول deep پایان یافت.")

if __name__ == "__main__":
    main()
