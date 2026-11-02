#!/usr/bin/env python3
# calculator_inverse.py - ساخت نسخه معکوس معاملات برای هر فایل
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

def calculate_inverse_distribution(original_dist, stop_loss_initial, max_gain):
    inv_sl = abs(stop_loss_initial)
    inv_dist = []
    for item in original_dist or []:
        pct = safe_percentage(item.get("درصد_دقیق"))
        cnt = item.get("تعداد_معاملات", 0) or 0
        if pct > 0:
            inv_pct = -inv_sl
        elif pct < 0:
            gain = abs(pct)
            if max_gain is not None:
                gain = min(gain, max_gain)
            inv_pct = gain
        else:
            inv_pct = 0.0
        inv_dist.append({
            "درصد_دقیق": f"{inv_pct:+.4f}%".replace("+-", "-"),
            "تعداد_معاملات": cnt
        })
    return inv_dist

def decrypt_enc_file(enc_path, password):
    import subprocess
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
    if enc_path.endswith(".enc"):
        if not password:
            password = os.environ.get("RESULTS_PASSWORD", os.environ.get("DATA_PASSWORD", ""))
        return decrypt_enc_file(enc_path, password)
    with open(enc_path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategies-json", required=True)
    parser.add_argument("--results-base", required=True, help="مسیر پوشه aggregated/")
    parser.add_argument("--stoploss-cache", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--password", required=False, default="")
    args = parser.parse_args()

    password = args.password or os.environ.get("RESULTS_PASSWORD", os.environ.get("DATA_PASSWORD", ""))

    with open(args.strategies_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    strategies = data["strategies"]
    with open(args.stoploss_cache, "r", encoding="utf-8") as f:
        sl_cache = json.load(f)

    inverse_cache = {}  # key: strategy_name, value: inverse_distribution
    for s in strategies:
        folder = s["folder"]
        key_sl = folder  # کلید جدید: نام استراتژی
        sl_info = sl_cache.get(key_sl, {"stop_loss": -2.0, "move_percents": None})
        stop_loss = sl_info["stop_loss"]
        max_gain = max(sl_info["move_percents"]) if sl_info.get("move_percents") else None
        enc_path = s.get("aggregated_file") or os.path.join(args.results_base, folder, f"{folder}_trades.enc")
        if not os.path.exists(enc_path):
            print(f"⚠️ فایل یکپارچه یافت نشد: {enc_path}")
            continue
        try:
            orig_data = load_aggregated_data(enc_path, password)
            dist = orig_data.get("توزیع_دقیق_سود_ضرر", [])
            inv_dist = calculate_inverse_distribution(dist, stop_loss, max_gain)
            inverse_cache[folder] = inv_dist
        except Exception as e:
            print(f"⚠️ خطا در {enc_path}: {e}")
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(inverse_cache, f, indent=2)
    print(f"✅ کش معکوس برای {len(inverse_cache)} استراتژی در {args.output} ذخیره شد.")

if __name__ == "__main__":
    main()
