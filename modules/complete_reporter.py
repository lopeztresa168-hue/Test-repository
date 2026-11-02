#!/usr/bin/env python3
# complete_reporter.py - تولید گزارش‌های جامع مانند نسخه اصلی 5
# شامل: استراتژی‌های معکوس، خلاصه پوشه، گزارش کلی، جداول اضافی، گزارش ماهانه برترین‌ها، تحلیل تکمیلی، همبستگی با وقفه

import os
import sys
import json
import csv
import re
import pickle
import statistics
import itertools
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal

# ------------------ توابع کمکی (از کد اصلی) ------------------
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

def safe_float(value, default=0.0):
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except:
        return default

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

def calculate_max_consecutive_loss(count, stop_loss_initial):
    count = int(count or 0)
    penalty = count * -0.1
    main_losses = count * float(stop_loss_initial)
    return penalty + main_losses

def extract_start_date_from_period(period_str):
    m = re.search(r'(\d{4}-\d{2}-\d{2})_', period_str)
    return m.group(1) if m else None

def extract_end_date_from_period(period_str):
    m = re.search(r'_(\d{4}-\d{2}-\d{2})$', period_str)
    return m.group(1) if m else None

def find_events_in_range(events, start_date_str, end_date_str):
    try:
        start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except:
        return []
    return [ev for ev in events if start <= ev["date"] <= end]

def load_news_pickle(pickle_path):
    with open(pickle_path, "rb") as f:
        return pickle.load(f)

# ------------------ ساخت all_results با استراتژی‌های اصلی و معکوس ------------------
def build_all_results_with_inverse(returns_cache_path, strategies_json_path, risk_cache_path, inverse_cache_path, results_base=None):
    with open(returns_cache_path, "r") as f:
        returns = json.load(f)   # key: strategy_name -> {"real":, "special":}
    with open(strategies_json_path, "r") as f:
        strat_data = json.load(f)
    with open(risk_cache_path, "r") as f:
        risk = json.load(f)      # key: strategy_name -> {"max_consecutive_loss": , ...}
    # کش معکوس (توزیع معکوس) - اختیاری
    inverse_dist_cache = {}
    if inverse_cache_path and os.path.exists(inverse_cache_path):
        with open(inverse_cache_path, "r") as f:
            inverse_dist_cache = json.load(f)   # key: strategy_name -> list of distribution items
    
    strategies = strat_data["strategies"]
    # دیکشنری کمکی برای move_percents و stop_loss_initial به ازای هر استراتژی
    strategy_params = {}
    for s in strategies:
        key = s["folder"]
        strategy_params[key] = {
            "move_percents": s.get("move_percents"),
            "stop_loss_initial": s.get("stop_loss_initial", -2.0),
            "max_move_percent": s.get("max_move_percent")
        }
    
    all_results = []  # لیست دیکشنری‌های فایل اصلی و معکوس
    
    for s in strategies:
        folder = s["folder"]
        group = s.get("group", "aggregated")
        params = strategy_params[folder]
        move_percents = params["move_percents"]
        stop_loss = params["stop_loss_initial"]
        max_move = params["max_move_percent"]
        
        # کلید جدید: نام استراتژی
        key = folder
        ret_data = returns.get(key, {"real": 0, "special": 0})
        risk_data = risk.get(key, {"max_consecutive_loss": 0})
        
        # اطلاعات اضافی: از فایل یکپارچه (در صورت دسترسی) یا مقادیر پیش‌فرض
        period_name = folder
        total_trades = 0
        win_trades = 0
        loss_trades = 0
        win_rate = 0.0
        max_drawdown = 0.0
        profit_loss_ratio = 0.0
        sharpe_ratio = 0.0
        raw_distribution = []
        
        # تلاش برای خواندن اطلاعات از فایل یکپارچه رمزگشایی‌شده (اگر موجود باشد)
        aggregated_file = s.get("aggregated_file", "")
        if aggregated_file and os.path.exists(aggregated_file):
            try:
                import subprocess
                password = os.environ.get("RESULTS_PASSWORD", os.environ.get("DATA_PASSWORD", ""))
                script = f"""
const crypto=require('crypto'),fs=require('fs');
const pass={json.dumps(password)};
const data=fs.readFileSync({json.dumps(aggregated_file)});
const key=crypto.scryptSync(pass,'salt',32);
const iv=data.slice(0,16),enc=data.slice(16);
const decipher=crypto.createDecipheriv('aes-256-cbc',key,iv);
const dec=Buffer.concat([decipher.update(enc),decipher.final()]);
process.stdout.write(dec);
"""
                result = subprocess.run(["node", "-e", script], capture_output=True)
                if result.returncode == 0:
                    data = json.loads(result.stdout.decode("utf-8"))
                    info = data.get("اطلاعات_فایل", {})
                    period_name = info.get("نام_فایل", folder)
                    stats = data.get("آمار_کلی_معاملات", {})
                    total_trades = safe_float(stats.get("تعداد_کل_معاملات", 0), 0)
                    win_trades = safe_float(stats.get("معاملات_سودده", 0), 0)
                    loss_trades = safe_float(stats.get("معاملات_زیانده", 0), 0)
                    win_rate_str = stats.get("نرخ_برد", "0%")
                    win_rate = safe_float(str(win_rate_str).replace("%", ""), 0)
                    risk_info = data.get("آنالیز_ریسک", {})
                    max_drawdown = safe_float(str(risk_info.get("حداکثر_افت_سرمایه", "0%")).replace("%", ""), 0)
                    profit_loss_ratio = safe_float(risk_info.get("نسبت_سود_به_ضرر", 0), 0)
                    sharpe_ratio = safe_float(risk_info.get("نسبت_شارپ", 0), 0)
                    raw_distribution = data.get("توزیع_دقیق_سود_ضرر", [])
            except Exception as e:
                print(f"⚠️ خطا در خواندن فایل یکپارچه {aggregated_file}: {e}")
        
        # رکورد اصلی
        main_record = {
            "folder_name": folder,
            "group": group,
            "file_name": folder,
            "period_name": period_name,
            "total_trades": total_trades,
            "win_trades": win_trades,
            "loss_trades": loss_trades,
            "win_rate": win_rate,
            "max_consecutive_loss": safe_float(risk_data.get("max_consecutive_loss", 0), 0),
            "max_drawdown": max_drawdown,
            "profit_loss_ratio": profit_loss_ratio,
            "sharpe_ratio": sharpe_ratio,
            "real_return": safe_float(ret_data.get("real", 0), 0),
            "special_rounded_return": safe_float(ret_data.get("special", 0), 0),
            "is_inverse": False,
            "_raw_distribution": raw_distribution,
            "_move_percents": move_percents,
            "_stop_loss": stop_loss,
            "_max_move": max_move
        }
        all_results.append(main_record)
        
        # ساخت نسخه معکوس اگر توزیع موجود باشد یا کش معکوس موجود باشد
        inv_dist = None
        if key in inverse_dist_cache:
            inv_dist = inverse_dist_cache[key]
        elif raw_distribution:
            inv_dist = calculate_inverse_distribution(raw_distribution, stop_loss, max_move)
        
        if inv_dist is not None:
            real_inv = calculate_real_return(inv_dist)
            special_inv = calculate_special_return(inv_dist, move_percents if move_percents else [])
            inv_record = {
                "folder_name": folder + "_INV",
                "group": group,
                "file_name": "INV_" + folder,
                "period_name": period_name,
                "total_trades": total_trades,
                "win_trades": loss_trades,
                "loss_trades": win_trades,
                "win_rate": 100.0 - win_rate if win_rate is not None else 50.0,
                "max_consecutive_loss": None,
                "max_drawdown": max_drawdown,
                "profit_loss_ratio": 1/profit_loss_ratio if profit_loss_ratio > 0 else 0,
                "sharpe_ratio": -sharpe_ratio if sharpe_ratio else 0,
                "real_return": real_inv,
                "special_rounded_return": special_inv,
                "is_inverse": True,
                "_original_file": folder,
                "_original_folder": folder
            }
            all_results.append(inv_record)
    
    return all_results

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

# ------------------ محاسبه امتیاز (همانند نسخه اصلی) ------------------
def calculate_strategy_score_new(folder_data):
    returns_ge_1 = folder_data["returns_ge_1"]
    returns_gt_0 = folder_data["returns_gt_0"]
    avg_max_loss = folder_data["avg_max_loss"]
    total_files = folder_data["total_files"]
    if total_files == 0:
        return {"امتیاز_نهایی": 0, "رتبه": "بدون داده"}
    score_ge1 = (returns_ge_1 / total_files) * 40
    score_gt0 = (returns_gt_0 / total_files) * 30
    loss_abs = abs(avg_max_loss)
    if loss_abs <= 5:
        score_loss = 30
    elif loss_abs <= 10:
        score_loss = 30 * (10 - loss_abs) / 5
    elif loss_abs <= 15:
        score_loss = 15 * (15 - loss_abs) / 5
    else:
        score_loss = max(0, 5 * (20 - loss_abs) / 5)
    total_score = score_ge1 + score_gt0 + score_loss
    if total_score >= 80:
        rating = "عالی ★★★★★"
    elif total_score >= 60:
        rating = "خوب ★★★★"
    elif total_score >= 40:
        rating = "متوسط ★★★"
    elif total_score >= 20:
        rating = "ضعیف ★★"
    else:
        rating = "بسیار ضعیف ★"
    return {"امتیاز_نهایی": round(total_score, 1), "رتبه": rating}

# ------------------ 1. خلاصه هر پوشه (شامل اصلی و معکوس در پوشه خودشان) ------------------
def create_folder_summary_csv(folder_name, folder_path, folder_results, move_percents, stop_loss_initial, max_move_percent=None):
    """همانند نسخه اصلی، فایل خلاصه_پوشه.csv را در folder_path ذخیره می‌کند"""
    if not folder_results:
        return
    output_path = os.path.join(folder_path, "خلاصه_پوشه.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([f"📊 خلاصه پوشه: {folder_name}"])
        writer.writerow(["تاریخ تولید", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        writer.writerow(["مسیر پوشه", folder_path])
        if folder_name.endswith("_INV"):
            writer.writerow(["🔄 این پوشه نسخه معکوس استراتژی اصلی است."])
            writer.writerow([f"   حد ضرر معکوس: {abs(stop_loss_initial):.2f}%"])
            if max_move_percent:
                writer.writerow([f"   سقف سود معکوس: {max_move_percent:.2f}%"])
            writer.writerow([])
        writer.writerow(["تعداد فایل‌های پردازش شده", len(folder_results)])
        if move_percents:
            writer.writerow(["movePercentها", ", ".join([str(x) for x in move_percents])])
        writer.writerow(["stopLossInitial", f"{stop_loss_initial:.2f}%"])
        writer.writerow([])
        writer.writerow(["ردیف", "نام فایل", "دوره معاملاتی", "تعداد معاملات",
                         "معاملات سودده", "معاملات زیانده", "وین‌ریت (%)",
                         "بازدهی واقعی (%)", "بازدهی گردشده ویژه (%)",
                         "حداکثر ضرر متوالی (%)", "حداکثر افت سرمایه (%)",
                         "نسبت سود به ضرر", "نسبت شارپ", "وضعیت"])
        for i, r in enumerate(folder_results, 1):
            real_ret = r.get("real_return", 0)
            special_ret = r.get("special_rounded_return", 0)
            win_rate = r.get("win_rate", 0)
            if special_ret >= 1.0:
                status = "✅ عالی (≥۱%)"
            elif special_ret > 0:
                status = "👍 مثبت"
            else:
                status = "⚠️ منفی"
            max_loss_display = f"{r['max_consecutive_loss']:.2f}" if r.get('max_consecutive_loss') is not None else "-"
            sharpe_display = f"{r['sharpe_ratio']:.3f}" if r.get('sharpe_ratio') is not None else "-"
            writer.writerow([
                i, r["file_name"].replace(".json", ""), r.get("period_name", ""),
                r.get("total_trades", 0), r.get("win_trades", 0), r.get("loss_trades", 0),
                f"{win_rate:.2f}", f"{real_ret:.4f}", f"{special_ret:.4f}",
                max_loss_display, f"{r.get('max_drawdown', 0):.2f}",
                f"{r.get('profit_loss_ratio', 0):.3f}", sharpe_display, status
            ])
        # افزودن خلاصه کلی پوشه (میانگین‌ها، توزیع و ...) مشابه نسخه اصلی - مختصر
        writer.writerow([])
        writer.writerow(["📈 خلاصه کلی پوشه:"])
        total_files = len(folder_results)
        pos_files = sum(1 for r in folder_results if r.get("special_rounded_return", 0) > 0)
        ge1_files = sum(1 for r in folder_results if r.get("special_rounded_return", 0) >= 1.0)
        writer.writerow(["فایل‌های با بازدهی مثبت", f"{pos_files} ({pos_files/total_files*100:.1f}%)"])
        writer.writerow(["فایل‌های با بازدهی ≥۱٪", f"{ge1_files} ({ge1_files/total_files*100:.1f}%)"])
        avg_special = statistics.mean([r["special_rounded_return"] for r in folder_results]) if folder_results else 0
        writer.writerow(["میانگین بازدهی ویژه", f"{avg_special:.4f}%"])
    print(f"✅ خلاصه پوشه {folder_name} ایجاد شد: {output_path}")

# ------------------ 2. گزارش کلی نتایج (شامل اصلی و معکوس) ------------------
def create_global_report(all_results, output_dir):
    output_path = os.path.join(output_dir, "گزارش_کلی_نتایج.csv")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["گروه", "نام پوشه", "نام فایل", "دوره معاملاتی", "تعداد معاملات",
                         "معاملات سودده", "معاملات زیانده", "وین ریت (%)",
                         "حداکثر ضرر متوالی (%)", "حداکثر افت سرمایه (%)",
                         "نسبت سود به ضرر", "نسبت شارپ", "بازدهی واقعی (%)",
                         "بازدهی گرد شده ویژه (%)", "نوع"])
        for r in all_results:
            inv_type = "معکوس" if r.get("is_inverse") else "اصلی"
            max_loss_disp = f"{r['max_consecutive_loss']:.2f}" if r['max_consecutive_loss'] is not None else "-"
            sharpe_disp = f"{r['sharpe_ratio']:.3f}" if r.get('sharpe_ratio') is not None else "-"
            writer.writerow([
                r["group"], r["folder_name"], r["file_name"], r.get("period_name", ""),
                r.get("total_trades", 0), r.get("win_trades", 0), r.get("loss_trades", 0),
                f"{r.get('win_rate', 0):.2f}", max_loss_disp, f"{r.get('max_drawdown', 0):.2f}",
                f"{r.get('profit_loss_ratio', 0):.3f}", sharpe_disp,
                f"{r.get('real_return', 0):.4f}", f"{r.get('special_rounded_return', 0):.4f}",
                inv_type
            ])
    print(f"✅ گزارش کلی نتایج در {output_path} ذخیره شد.")

# ------------------ 3. دو جدول مقایسه اضافی (جدول ۲ و ۳) ------------------
def create_extra_comparison_tables(all_results, output_dir):
    # تجمیع داده‌های هر استراتژی (folder+group)
    folder_agg = {}
    for r in all_results:
        key = (r["folder_name"], r["group"])
        if key not in folder_agg:
            folder_agg[key] = {
                "folder": r["folder_name"], "group": r["group"],
                "win_rates": [], "sharpes": [], "max_losses": [],
                "real_returns": [], "special_returns": [],
                "total_trades": 0, "returns_ge_1": 0, "returns_gt_0": 0,
                "file_count": 0
            }
        agg = folder_agg[key]
        agg["win_rates"].append(r["win_rate"])
        agg["sharpes"].append(r["sharpe_ratio"])
        agg["max_losses"].append(r["max_consecutive_loss"] if r["max_consecutive_loss"] is not None else 0)
        agg["real_returns"].append(r["real_return"])
        agg["special_returns"].append(r["special_rounded_return"])
        agg["total_trades"] += r["total_trades"]
        if r["special_rounded_return"] >= 1.0:
            agg["returns_ge_1"] += 1
        if r["special_rounded_return"] > 0:
            agg["returns_gt_0"] += 1
        agg["file_count"] += 1

    folder_list = []
    for (folder, group), agg in folder_agg.items():
        file_cnt = agg["file_count"]
        avg_win = statistics.mean(agg["win_rates"]) if agg["win_rates"] else 0
        avg_sharpe = statistics.mean(agg["sharpes"]) if agg["sharpes"] else 0
        avg_max_loss = statistics.mean(agg["max_losses"]) if agg["max_losses"] else 0
        avg_real = statistics.mean(agg["real_returns"]) if agg["real_returns"] else 0
        avg_special = statistics.mean(agg["special_returns"]) if agg["special_returns"] else 0
        score_res = calculate_strategy_score_new({
            "returns_ge_1": agg["returns_ge_1"],
            "returns_gt_0": agg["returns_gt_0"],
            "avg_max_loss": avg_max_loss,
            "total_files": file_cnt
        })
        folder_list.append({
            "folder": folder, "group": group, "file_count": file_cnt,
            "avg_win_rate": avg_win, "avg_sharpe": avg_sharpe, "avg_max_loss": avg_max_loss,
            "avg_real_return": avg_real, "avg_special_return": avg_special,
            "total_trades": agg["total_trades"],
            "score": score_res["امتیاز_نهایی"], "rating": score_res["رتبه"],
            "is_inverse": folder.endswith("_INV")
        })
    folder_list_sorted = sorted(folder_list, key=lambda x: x["score"], reverse=True)

    # جدول ۲: همه با بازده واقعی
    out_path2 = os.path.join(output_dir, "جدول_مقایسه_همه_بازده_واقعی.csv")
    with open(out_path2, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["رتبه", "نام پوشه", "گروه", "نوع", "تعداد فایل‌ها", "بازده واقعی (%)",
                         "وین‌ریت (%)", "حداکثر ضرر متوالی (%)", "نسبت شارپ", "کل معاملات",
                         "امتیاز نهایی", "وضعیت"])
        for i, item in enumerate(folder_list_sorted, 1):
            writer.writerow([
                i, item["folder"], item["group"], "معکوس" if item["is_inverse"] else "اصلی",
                item["file_count"], f"{item['avg_real_return']:.4f}%", f"{item['avg_win_rate']:.2f}",
                f"{item['avg_max_loss']:.2f}", f"{item['avg_sharpe']:.3f}",
                item["total_trades"], f"{item['score']:.1f}/100", item["rating"]
            ])
    print(f"✅ جدول ۲ (بازده واقعی) در {out_path2} ذخیره شد.")

    # جدول ۳: همه با بازده گردش‌شده ویژه
    out_path3 = os.path.join(output_dir, "جدول_مقایسه_همه_بازده_ویژه.csv")
    with open(out_path3, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["رتبه", "نام پوشه", "گروه", "نوع", "تعداد فایل‌ها", "بازده ویژه (%)",
                         "وین‌ریت (%)", "حداکثر ضرر متوالی (%)", "نسبت شارپ", "کل معاملات",
                         "امتیاز نهایی", "وضعیت"])
        for i, item in enumerate(folder_list_sorted, 1):
            writer.writerow([
                i, item["folder"], item["group"], "معکوس" if item["is_inverse"] else "اصلی",
                item["file_count"], f"{item['avg_special_return']:.4f}%", f"{item['avg_win_rate']:.2f}",
                f"{item['avg_max_loss']:.2f}", f"{item['avg_sharpe']:.3f}",
                item["total_trades"], f"{item['score']:.1f}/100", item["rating"]
            ])
    print(f"✅ جدول ۳ (بازده ویژه) در {out_path3} ذخیره شد.")

# ------------------ 4. گزارش ماهانه برای سه استراتژی برتر ------------------
def generate_top_strategies_monthly_reports(all_results, top_n=3, output_dir=None):
    # ابتدا محاسبه امتیاز هر استراتژی (بر اساس special_return برای گروه 1 و real برای گروه 2؟ مشابه نسخه اصلی)
    # برای سادگی از special_return برای همه استفاده می‌کنیم (چون امتیازدهی بر مبنای special است)
    strat_scores = {}
    strat_files = defaultdict(list)
    for r in all_results:
        key = (r["folder_name"], r["group"])
        strat_files[key].append(r)
    for key, files in strat_files.items():
        folder, group = key
        total = len(files)
        ge1 = sum(1 for f in files if f["special_rounded_return"] >= 1.0)
        gt0 = sum(1 for f in files if f["special_rounded_return"] > 0)
        max_losses = [f["max_consecutive_loss"] for f in files if f["max_consecutive_loss"] is not None]
        avg_loss = statistics.mean(max_losses) if max_losses else 0
        score_data = {"returns_ge_1": ge1, "returns_gt_0": gt0, "avg_max_loss": avg_loss, "total_files": total}
        score = calculate_strategy_score_new(score_data)["امتیاز_نهایی"]
        strat_scores[key] = score
    
    # مرتب‌سازی و انتخاب top_n
    sorted_strats = sorted(strat_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    for (folder, group), score in sorted_strats:
        # جمع‌آوری فایل‌های این استراتژی (هم اصلی و هم معکوس؟ فقط اصلی یا هر دو؟ نسخه اصلی فقط استراتژی اصلی را می‌گرفت)
        # برای سادگی فقط فایل‌های غیر معکوس (is_inverse=False) را در نظر می‌گیریم
        files = [f for f in strat_files[(folder, group)] if not f.get("is_inverse")]
        if not files:
            continue
        # محاسبه رکوردهای ماهانه
        monthly = compute_monthly_records_for_strategy(files, group)
        if not monthly:
            continue
        # محاسبه امتیاز و رتبه هر ماه
        for rec in monthly:
            rets_ge1 = sum(1 for f in rec["file_objects"] if f["special_rounded_return"] >= 1.0)
            rets_gt0 = sum(1 for f in rec["file_objects"] if f["special_rounded_return"] > 0)
            max_loss_month = max((f["max_consecutive_loss"] for f in rec["file_objects"] if f["max_consecutive_loss"] is not None), default=0)
            sdata = {"returns_ge_1": rets_ge1, "returns_gt_0": rets_gt0, "avg_max_loss": max_loss_month, "total_files": 3}
            sc = calculate_strategy_score_new(sdata)
            rec["score"] = sc["امتیاز_نهایی"]
            rec["rating"] = sc["رتبه"]
        # مرتب‌سازی از بدترین ماه به بهترین
        monthly_sorted = sorted(monthly, key=lambda x: x["score"])
        # مسیر پوشه استراتژی (در results_base)
        strat_path = os.path.join(output_dir, group, folder) if output_dir else os.path.join("final_output", group, folder)
        os.makedirs(strat_path, exist_ok=True)
        out_file = os.path.join(strat_path, "گزارش_ماهانه_برترین.csv")
        with open(out_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([f"📊 گزارش ماهانه استراتژی: {folder} (گروه {group})"])
            writer.writerow(["تاریخ تولید", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            writer.writerow(["مرتب‌شده از بدترین ماه به بهترین ماه بر اساس امتیاز"])
            writer.writerow([])
            writer.writerow(["رتبه (بدترین=1)", "ماه", "بازده ماه (%)", "وین‌ریت ماه (%)",
                             "حداکثر ضرر متوالی ماه (%)", "میانگین شارپ ماه", "امتیاز ماه", "وضعیت",
                             "فایل‌های تشکیل‌دهنده"])
            for i, rec in enumerate(monthly_sorted, 1):
                total_trades = sum(f["total_trades"] for f in rec["file_objects"])
                win_trades = sum(f["win_trades"] for f in rec["file_objects"])
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                avg_sharpe = statistics.mean([f["sharpe_ratio"] for f in rec["file_objects"] if f["sharpe_ratio"] is not None]) or 0
                writer.writerow([
                    i, rec["year_month"], f"{rec['total_return']:.4f}", f"{win_rate:.2f}",
                    f"{rec.get('max_loss',0):.2f}", f"{avg_sharpe:.3f}",
                    f"{rec['score']:.1f}/100", rec["rating"],
                    "; ".join(rec["files"])
                ])
        print(f"✅ گزارش ماهانه برای {folder} (گروه {group}) در {out_file} ذخیره شد.")

def compute_monthly_records_for_strategy(strategy_results, group):
    monthly_files = {}
    for r in strategy_results:
        start = extract_start_date_from_period(r.get("period_name", ""))
        if not start:
            continue
        ym = start[:7]
        monthly_files.setdefault(ym, []).append(r)
    monthly_records = []
    for ym, files in monthly_files.items():
        selected = []
        for day in [1, 11, 21]:
            target = f"{ym}-{day:02d}"
            found = next((f for f in files if extract_start_date_from_period(f.get("period_name","")) == target), None)
            if found:
                selected.append(found)
        if len(selected) != 3:
            continue
        total_return = sum(f["special_rounded_return"] for f in selected) if group == "1" else sum(f["real_return"] for f in selected)
        max_loss_month = max((f["max_consecutive_loss"] for f in selected if f["max_consecutive_loss"] is not None), default=0)
        monthly_records.append({
            "year_month": ym,
            "total_return": total_return,
            "files": [f["file_name"] for f in selected],
            "file_objects": selected,
            "max_loss": max_loss_month
        })
    monthly_records.sort(key=lambda x: x["year_month"])
    return monthly_records

# ------------------ 5. تحلیل تکمیلی برای هر استراتژی (فایل متنی) ------------------
def create_complementary_analysis(all_results, news_events, output_dir):
    # گروه‌بندی بر اساس استراتژی (غیر معکوس)
    strat_map = defaultdict(list)
    for r in all_results:
        if r.get("is_inverse"):
            continue
        key = (r["folder_name"], r["group"])
        strat_map[key].append(r)
    
    for (folder, group), files in strat_map.items():
        # محاسبه رکوردهای ماهانه
        monthly = compute_monthly_records_for_strategy(files, group)
        if not monthly:
            continue
        # جمع‌آوری رویدادهای خبری برای هر ماه
        event_list = []  # هر آیتم: month, return_month, indicator, actual, forecast, diff
        for rec in monthly:
            # بازه ماه را از فایل اول (یا از start_date/end_date) بدست آوریم
            start = extract_start_date_from_period(rec["file_objects"][0]["period_name"])
            end = extract_end_date_from_period(rec["file_objects"][-1]["period_name"])
            if not start or not end:
                continue
            events = find_events_in_range(news_events, start, end)
            for ev in events:
                if ev["actual"] is not None and ev["forecast"] is not None:
                    diff = ev["actual"] - ev["forecast"]
                    event_list.append({
                        "month": rec["year_month"],
                        "return_month": rec["total_return"],
                        "indicator": ev["indicator"],
                        "actual": ev["actual"],
                        "forecast": ev["forecast"],
                        "diff": diff
                    })
        if not event_list:
            continue
        
        # تحلیل هر شاخص
        indicators = set(e["indicator"] for e in event_list)
        output_path = os.path.join(output_dir, group, folder, "تحلیل_تکمیلی_استراتژی.txt")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("="*70 + "\n")
            f.write(f"📈 تحلیل تکمیلی استراتژی: {folder} (گروه {group})\n")
            f.write(f"تاریخ تولید: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*70 + "\n\n")
            for ind in sorted(indicators):
                ind_events = [e for e in event_list if e["indicator"] == ind]
                neg_events = [e for e in ind_events if e["return_month"] < 0]
                pos_events = [e for e in ind_events if e["return_month"] >= 0]
                
                def calc_stats(evs):
                    if not evs:
                        return {"count":0, "avg_diff":None}
                    diffs = [e["diff"] for e in evs if e["diff"] is not None]
                    avg = statistics.mean(diffs) if diffs else None
                    return {"count": len(evs), "avg_diff": avg}
                
                neg_stats = calc_stats(neg_events)
                pos_stats = calc_stats(pos_events)
                f.write(f"\n📌 شاخص: {ind}\n")
                f.write("-"*50 + "\n")
                f.write(f"تعداد رویدادها در ماه‌های ضررده: {neg_stats['count']}\n")
                f.write(f"میانگین diff در ماه‌های ضررده: {neg_stats['avg_diff'] if neg_stats['avg_diff'] is not None else '---'}\n")
                f.write(f"تعداد رویدادها در ماه‌های سودده: {pos_stats['count']}\n")
                f.write(f"میانگین diff در ماه‌های سودده: {pos_stats['avg_diff'] if pos_stats['avg_diff'] is not None else '---'}\n")
            f.write("\n" + "="*70 + "\n")
            f.write("🎯 نتیجه‌گیری کلی\n")
            all_neg_diffs = [e["diff"] for e in event_list if e["return_month"] < 0 and e["diff"] is not None]
            all_pos_diffs = [e["diff"] for e in event_list if e["return_month"] >= 0 and e["diff"] is not None]
            avg_neg = statistics.mean(all_neg_diffs) if all_neg_diffs else None
            avg_pos = statistics.mean(all_pos_diffs) if all_pos_diffs else None
            if avg_neg is not None and avg_pos is not None:
                if avg_neg > avg_pos:
                    f.write("در ماه‌های ضررده، اخبار نسبت به پیش‌بینی بدتر بوده‌اند.\n")
                elif avg_neg < avg_pos:
                    f.write("در ماه‌های ضررده، اخبار نسبت به پیش‌بینی بهتر بوده‌اند.\n")
                else:
                    f.write("تفاوت معناداری مشاهده نمی‌شود.\n")
        print(f"✅ تحلیل تکمیلی برای {folder} در {output_path} ذخیره شد.")

# ------------------ 6. تحلیل همبستگی با وقفه ------------------
def create_lag_correlation(all_results, news_events, output_dir):
    INDICATORS = ['CPI m/m', 'Core CPI m/m', 'PPI m/m', 'Core PPI m/m', 'CPI y/y']
    # فقط استراتژی‌های غیر معکوس
    non_inverse = [r for r in all_results if not r.get("is_inverse")]
    # گروه‌بندی بر اساس استراتژی
    strat_map = defaultdict(list)
    for r in non_inverse:
        strat_map[(r["folder_name"], r["group"])].append(r)
    
    report_rows = []
    for (folder, group), files in strat_map.items():
        if len(files) < 3:
            continue
        X_features = []
        y_returns = []
        for f in files:
            start = extract_start_date_from_period(f.get("period_name", ""))
            end = extract_end_date_from_period(f.get("period_name", ""))
            if not start or not end:
                continue
            start_date = datetime.strptime(start, "%Y-%m-%d").date()
            ret = f["special_rounded_return"] if group == "1" else f["real_return"]
            y_returns.append(ret)
            features = {}
            for ind in INDICATORS:
                events_in = find_events_in_range(news_events, start, end)
                evs = [e for e in events_in if e["indicator"] == ind]
                diffs = [e["actual"] - e["forecast"] for e in evs if e["actual"] is not None and e["forecast"] is not None]
                features[f"{ind}_diff_lag0"] = statistics.mean(diffs) if diffs else 0.0
                features[f"{ind}_volatility"] = statistics.stdev(diffs) if len(diffs) > 1 else 0.0
                # lag1: 10 روز قبل
                lag1_start = (start_date - timedelta(days=10)).strftime("%Y-%m-%d")
                lag1_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
                evs1 = find_events_in_range(news_events, lag1_start, lag1_end)
                diffs1 = [e["actual"] - e["forecast"] for e in evs1 if e["indicator"] == ind and e["actual"] is not None and e["forecast"] is not None]
                features[f"{ind}_diff_lag1"] = statistics.mean(diffs1) if diffs1 else 0.0
                # lag2: 10 روز قبل از lag1 (یعنی 20 روز قبل)
                lag2_start = (start_date - timedelta(days=20)).strftime("%Y-%m-%d")
                lag2_end = (start_date - timedelta(days=11)).strftime("%Y-%m-%d")
                evs2 = find_events_in_range(news_events, lag2_start, lag2_end)
                diffs2 = [e["actual"] - e["forecast"] for e in evs2 if e["indicator"] == ind and e["actual"] is not None and e["forecast"] is not None]
                features[f"{ind}_diff_lag2"] = statistics.mean(diffs2) if diffs2 else 0.0
            X_features.append(features)
        if len(X_features) < 3:
            continue
        # محاسبه همبستگی برای هر ویژگی
        feature_names = list(X_features[0].keys())
        for feat in feature_names:
            x_vals = [xf[feat] for xf in X_features]
            if len(set(x_vals)) <= 1:
                corr = 0.0
            else:
                n = len(x_vals)
                mean_x = sum(x_vals) / n
                mean_y = sum(y_returns) / n
                cov = sum((x_vals[i]-mean_x)*(y_returns[i]-mean_y) for i in range(n))
                std_x = (sum((xi-mean_x)**2 for xi in x_vals) ** 0.5)
                std_y = (sum((yi-mean_y)**2 for yi in y_returns) ** 0.5)
                corr = cov/(std_x*std_y) if std_x and std_y else 0.0
            if abs(corr) > 0.05:
                report_rows.append({
                    "استراتژی": f"{folder} (گروه {group})",
                    "ویژگی_خبری": feat,
                    "ضریب_همبستگی": round(corr, 3),
                    "جهت_رابطه": "مستقیم" if corr > 0 else "معکوس",
                    "تعداد_نمونه": len(X_features)
                })
    out_path = os.path.join(output_dir, "تحلیل_همبستگی_خبری.csv")
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["استراتژی", "ویژگی_خبری", "ضریب_همبستگی", "جهت_رابطه", "تعداد_نمونه"])
        for row in sorted(report_rows, key=lambda x: -abs(x["ضریب_همبستگی"])):
            writer.writerow([row["استراتژی"], row["ویژگی_خبری"], row["ضریب_همبستگی"], row["جهت_رابطه"], row["تعداد_نمونه"]])
    print(f"✅ تحلیل همبستگی خبری در {out_path} ذخیره شد.")

# ------------------ تابع اصلی ماژول ------------------
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--returns-cache", required=True)
    parser.add_argument("--strategies-json", required=True)
    parser.add_argument("--risk-cache", required=True)
    parser.add_argument("--inverse-cache", required=False, default="")
    parser.add_argument("--results-base", required=False, default="", help="مسیر پوشه aggregated/ (اختیاری، برای خواندن فایل‌های .enc)")
    parser.add_argument("--news-pickle", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    
    news_events = load_news_pickle(args.news_pickle)
    all_results = build_all_results_with_inverse(
        args.returns_cache, args.strategies_json, args.risk_cache,
        args.inverse_cache if args.inverse_cache else None,
        args.results_base if args.results_base else None
    )
    print(f"✅ تعداد کل استراتژی‌های بارگذاری شده (اصلی+معکوس): {len(all_results)}")
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 1. خلاصه هر پوشه — برای ساختار aggregated پوشه خلاصه در output_dir ذخیره می‌شود
    folders_map = defaultdict(list)
    for r in all_results:
        if r.get("is_inverse"):
            continue
        key = r["folder_name"]
        folders_map[key].append(r)
    for folder, files in folders_map.items():
        folder_output_path = os.path.join(args.output_dir, folder)
        os.makedirs(folder_output_path, exist_ok=True)
        first = files[0]
        move_percents = first.get("_move_percents")
        stop_loss = first.get("_stop_loss", -2.0)
        max_move = first.get("_max_move")
        create_folder_summary_csv(folder, folder_output_path, files, move_percents, stop_loss, max_move)
    
    # 2. گزارش کلی
    create_global_report(all_results, args.output_dir)
    
    # 3. جداول اضافی
    create_extra_comparison_tables(all_results, args.output_dir)
    
    # 4. گزارش ماهانه برای سه استراتژی برتر (فقط اصلی)
    generate_top_strategies_monthly_reports(all_results, top_n=3, output_dir=args.output_dir)
    
    # 5. تحلیل تکمیلی (فقط اصلی)
    create_complementary_analysis(all_results, news_events, args.output_dir)
    
    # 6. تحلیل همبستگی با وقفه
    create_lag_correlation(all_results, news_events, args.output_dir)
    
    print("✅ همه گزارش‌های جامع تولید شدند.")

if __name__ == "__main__":
    main()
