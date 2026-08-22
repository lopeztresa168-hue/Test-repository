#!/usr/bin/env python3
# loader.py - ماژول استخراج (بارگذاری اخبار و استراتژی‌ها)
import os
import sys
import json
import csv
import re
import pickle
import argparse
from datetime import datetime
from collections import defaultdict

# ================================ بخش اخبار ================================
def parse_percent(value_str):
    """تبدیل رشته درصد به float"""
    if value_str is None or str(value_str).strip() in ('', '-', '—', '--'):
        return None
    cleaned = re.sub(r'[^\d.-]', '', str(value_str))
    if cleaned in ('', '-', '.', '-.'):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None

def find_column_index(header, keyword):
    for i, h in enumerate(header):
        if keyword in h.lower():
            return i
    return -1

def load_news_files(news_folder_path):
    """خواندن تمام فایل‌های CSV در پوشه اخبار و تبدیل به لیست رویدادها"""
    events = []
    indicator_map = {
        'US Core CPI m_m': 'Core CPI m/m',
        'US Core PPI m_m': 'Core PPI m/m',
        'US CPI m_m': 'CPI m/m',
        'US CPI y_y': 'CPI y/y',
        'US PPI m_m': 'PPI m/m',
        'United States Fomc Minutes _ Historical Dates 1971-2026 Data_ Moneycontrol': 'FOMC',
        'FOMC': 'FOMC',
    }
    if not os.path.isdir(news_folder_path):
        print(f"⚠️ پوشه اخبار یافت نشد: {news_folder_path}")
        return events
    for filename in os.listdir(news_folder_path):
        if not filename.endswith('.csv'):
            continue
        file_path = os.path.join(news_folder_path, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                base_name = filename.replace('.csv', '').strip()
                base_name = re.sub(r'\s*_\s*Forex\s*Factory\s*$', '', base_name, flags=re.IGNORECASE)
                indicator = indicator_map.get(base_name, base_name)
                is_fomc = (indicator == 'FOMC')
                header_lower = [h.strip().lower() for h in header]
                date_idx = find_column_index(header_lower, 'date')
                if date_idx == -1:
                    continue
                actual_idx = find_column_index(header_lower, 'actual')
                forecast_idx = find_column_index(header_lower, 'forecast')
                if forecast_idx == -1:
                    forecast_idx = find_column_index(header_lower, 'consensus')
                previous_idx = find_column_index(header_lower, 'previous')
                for row in reader:
                    if not row:
                        continue
                    date_str = row[date_idx].strip()
                    try:
                        event_date = datetime.strptime(date_str, "%b %d, %Y").date()
                    except:
                        try:
                            event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        except:
                            continue
                    if is_fomc:
                        actual = None
                        forecast = None
                        previous = None
                    else:
                        actual = parse_percent(row[actual_idx]) if actual_idx != -1 and actual_idx < len(row) else None
                        forecast = parse_percent(row[forecast_idx]) if forecast_idx != -1 and forecast_idx < len(row) else None
                        previous = parse_percent(row[previous_idx]) if previous_idx != -1 and previous_idx < len(row) else None
                    events.append({
                        "date": event_date,
                        "indicator": indicator,
                        "actual": actual,
                        "forecast": forecast,
                        "previous": previous
                    })
        except Exception as e:
            print(f"❌ خطا در خواندن فایل خبر {filename}: {e}")
    return events

# ================================ بخش استراتژی‌ها ================================
def extract_stop_loss_from_config(data):
    """استخراج مقدار stopLoss از محتوای فایل 1.json"""
    patterns = [
        r'stopLoss\s*:\s*close\s*\*\s*([0-9.]+)',
        r'stopLoss\s*:\s*([0-9.]+)(?![%*])',
        r'stopLoss\s*:\s*["\']?([0-9.]+)%["\']?',
        r'stopLossInitial\s*:\s*([-\d.]+)',
        r'stopLoss\s*:\s*([-\d.]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, data, re.IGNORECASE)
        if match:
            val_str = match.group(1)
            try:
                val = float(val_str)
            except:
                continue
            if '%' in pattern or val < 0:
                return -abs(val)
            if 0 < val < 1:
                return -((1 - val) * 100)
            return -abs(val)
    return -2.0

def extract_move_percents_from_config(file_path):
    """استخراج movePercentها و max_move از فایل 1.json"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = f.read()
        matches = re.findall(r'movePercent\s*:\s*([0-9.-]+)', data)
        if matches:
            return sorted([float(m) for m in matches])
        return None
    except:
        return None

def load_strategies(aggregated_dir, strategies_dir=None):
    """خواندن تمام استراتژی‌ها از پوشه aggregated/ (ساختار یکپارچه جدید)
    
    aggregated_dir: مسیر پوشه aggregated/ که زیرپوشه‌های آن نام استراتژی‌ها هستند
    strategies_dir: مسیر پوشه strategies/ برای خواندن فایل 1.json (اختیاری)
    """
    strategies = []
    if not os.path.isdir(aggregated_dir):
        print(f"❌ مسیر aggregated وجود ندارد: {aggregated_dir}")
        return strategies
    for strategy_name in sorted(os.listdir(aggregated_dir)):
        strategy_path = os.path.join(aggregated_dir, strategy_name)
        if not os.path.isdir(strategy_path):
            continue
        aggregated_file = f"{strategy_name}_trades.enc"
        aggregated_file_path = os.path.join(strategy_path, aggregated_file)
        if not os.path.exists(aggregated_file_path):
            print(f"⚠️ فایل یکپارچه یافت نشد: {aggregated_file_path}")
            continue
        # جستجوی فایل 1.json در strategies/ یا aggregated/strategy/
        config_file = None
        if strategies_dir:
            candidate = os.path.join(strategies_dir, strategy_name, "1.json")
            if os.path.exists(candidate):
                config_file = candidate
            else:
                # بررسی فایل .js به‌عنوان جایگزین
                candidate_js = os.path.join(strategies_dir, strategy_name, f"{strategy_name}.js")
                if os.path.exists(candidate_js):
                    config_file = candidate_js
        if config_file is None:
            local_candidate = os.path.join(strategy_path, "1.json")
            if os.path.exists(local_candidate):
                config_file = local_candidate
        move_percents = None
        stop_loss_initial = -2.0
        max_move_percent = None
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, "r", encoding='utf-8') as cf:
                    config_text = cf.read()
                stop_loss_initial = extract_stop_loss_from_config(config_text)
                move_percents = extract_move_percents_from_config(config_file)
                if move_percents:
                    max_move_percent = max(move_percents)
            except:
                pass
        strategies.append({
            "folder": strategy_name,
            "group": "aggregated",
            "config_file": config_file or "",
            "aggregated_file": aggregated_file_path,
            "move_percents": move_percents,
            "stop_loss_initial": stop_loss_initial,
            "max_move_percent": max_move_percent
        })
    return strategies

# ================================ بخش کش ================================
def save_news_cache(news_events, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(news_events, f)
    print(f"✅ {len(news_events)} رویداد خبری در {output_path} ذخیره شد.")

def save_strategies_cache(strategies, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"strategies": strategies}, f, indent=2, ensure_ascii=False)
    print(f"✅ {len(strategies)} استراتژی در {output_path} ذخیره شد.")

# ================================ اصلی ================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--news-dir", required=True, help="پوشه حاوی CSV اخبار")
    parser.add_argument("--results-dir", required=True, help="مسیر پوشه aggregated/ (ساختار یکپارچه)")
    parser.add_argument("--strategies-dir", required=False, default="", help="مسیر پوشه strategies/ برای خواندن 1.json (اختیاری)")
    parser.add_argument("--output-news", required=True, help="مسیر ذخیره cache اخبار (pickle)")
    parser.add_argument("--output-strategies", required=True, help="مسیر ذخیره cache استراتژی‌ها (json)")
    args = parser.parse_args()

    print("🔄 بارگذاری اخبار...")
    news = load_news_files(args.news_dir)
    save_news_cache(news, args.output_news)

    print("🔄 بارگذاری استراتژی‌ها از aggregated/...")
    strategies_dir = args.strategies_dir if args.strategies_dir else None
    strategies = load_strategies(args.results_dir, strategies_dir)
    save_strategies_cache(strategies, args.output_strategies)

    print("✅ ماژول loader با موفقیت پایان یافت.")

if __name__ == "__main__":
    main()
