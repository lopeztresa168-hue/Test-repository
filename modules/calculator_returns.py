#!/usr/bin/env python3
# calculator_returns.py - محاسبه بازده واقعی و ویژه برای فایل‌های نتایج
import os
import sys
import json
import argparse
from decimal import Decimal

def safe_percentage(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace('%', '').replace('+', '').strip()
    if cleaned == "":
        return 0.0
    try:
        return float(cleaned)
    except:
        return 0.0

def round_to_nearest(value, options):
    if not options:
        return value
    return min(options, key=lambda x: abs(x - value))

def apply_special_rounding(percent, move_percents):
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
    except:
        return percent

def calculate_real_return(distribution_data):
    total = Decimal("0")
    for item in distribution_data or []:
        percent = safe_percentage(item.get("درصد_دقیق"))
        count = item.get("تعداد_معاملات", 0) or 0
        total += Decimal(str(percent)) * Decimal(str(count))
    return float(total)

def calculate_special_return(distribution_data, move_percents):
    total = Decimal("0")
    for item in distribution_data or []:
        percent = safe_percentage(item.get("درصد_دقیق"))
        count = item.get("تعداد_معاملات", 0) or 0
        rounded = apply_special_rounding(percent, move_percents)
        total += Decimal(str(rounded)) * Decimal(str(count))
    return float(total)

def decrypt_enc_file(enc_path, password):
    """رمزگشایی فایل .enc با استفاده از Node.js (همانند منطق موجود در workflow)"""
    import subprocess, tempfile, os
    script = f"""
const crypto=require('crypto'),fs=require('fs');
const pass={json.dumps(password)};
const data=fs.readFileSync({json.dumps(enc_path)});
const key=crypto.scryptSync(pass,'salt',32);
const iv=data.slice(0,16),enc=data.slice(16);
const decipher=crypto.createDecipheriv('aes-256-cbc',key,iv);
const dec=Buffer.concat([decipher.update(enc),decipher.final()]);
process.stdout.write(dec);
"""
    result = subprocess.run(["node", "-e", script], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"رمزگشایی ناموفق: {result.stderr.decode()}")
    return json.loads(result.stdout.decode("utf-8"))

def load_aggregated_data(enc_path, password=None):
    """بارگذاری داده‌های یکپارچه از فایل .enc یا .json"""
    if enc_path.endswith(".enc"):
        if not password:
            password = os.environ.get("RESULTS_PASSWORD", os.environ.get("DATA_PASSWORD", ""))
        return decrypt_enc_file(enc_path, password)
    with open(enc_path, "r", encoding="utf-8") as f:
        return json.load(f)

def process_aggregated_file(enc_path, move_percents_list, password=None):
    data = load_aggregated_data(enc_path, password)
    distribution = data.get("توزیع_دقیق_سود_ضرر", [])
    real = calculate_real_return(distribution)
    special = calculate_special_return(distribution, move_percents_list or [])
    return {"real": real, "special": special}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategies-json", required=True, help="خروجی loader (strategies_metadata.json)")
    parser.add_argument("--results-base", required=True, help="مسیر پوشه aggregated/")
    parser.add_argument("--output", required=True, help="فایل خروجی کش returns_cache.json")
    parser.add_argument("--password", required=False, default="", help="رمز رمزگشایی فایل‌های .enc")
    args = parser.parse_args()

    password = args.password or os.environ.get("RESULTS_PASSWORD", os.environ.get("DATA_PASSWORD", ""))

    with open(args.strategies_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    strategies = data["strategies"]
    cache = {}
    for s in strategies:
        folder = s["folder"]
        move = s.get("move_percents")
        # فایل یکپارچه: مسیر از metadata یا ساخته‌شده از results_base
        enc_path = s.get("aggregated_file") or os.path.join(args.results_base, folder, f"{folder}_trades.enc")
        if not os.path.exists(enc_path):
            print(f"⚠️ فایل یکپارچه یافت نشد: {enc_path}")
            continue
        key = folder  # کلید به‌صورت نام استراتژی (بدون شماره فایل)
        try:
            res = process_aggregated_file(enc_path, move, password)
            cache[key] = res
        except Exception as e:
            print(f"⚠️ خطا در {enc_path}: {e}")
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)
    print(f"✅ کش بازده برای {len(cache)} استراتژی در {args.output} ذخیره شد.")

if __name__ == "__main__":
    main()
