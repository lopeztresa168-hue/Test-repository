#!/usr/bin/env python3
# calculator_risk.py - محاسبه حداکثر ضرر متوالی
import os
import sys
import json
import argparse

def calculate_max_consecutive_loss(count, stop_loss_initial):
    count = int(count or 0)
    penalty = count * -0.1
    main_losses = count * float(stop_loss_initial)
    return penalty + main_losses

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

    risk_cache = {}
    for s in strategies:
        folder = s["folder"]
        key_sl = folder  # کلید جدید: فقط نام استراتژی
        stop_loss = sl_cache.get(key_sl, {"stop_loss": -2.0})["stop_loss"]
        enc_path = s.get("aggregated_file") or os.path.join(args.results_base, folder, f"{folder}_trades.enc")
        if not os.path.exists(enc_path):
            print(f"⚠️ فایل یکپارچه یافت نشد: {enc_path}")
            continue
        try:
            file_data = load_aggregated_data(enc_path, password)
            risk_info = file_data.get("آنالیز_ریسک", {})
            max_consecutive_count = risk_info.get("بیشترین_ضرر_متوالی", 0) or 0
            max_loss_value = calculate_max_consecutive_loss(max_consecutive_count, stop_loss)
            key = folder
            risk_cache[key] = {
                "max_consecutive_loss": max_loss_value,
                "max_consecutive_count": max_consecutive_count
            }
        except Exception as e:
            print(f"⚠️ خطا در {enc_path}: {e}")
            continue

    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(risk_cache, f, indent=2)
    print(f"✅ کش ریسک برای {len(risk_cache)} استراتژی در {args.output} ذخیره شد.")

if __name__ == "__main__":
    main()
