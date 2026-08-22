#!/usr/bin/env python3
# rest.py - نسخه اصلاح شده با تبدیل مقادیر به float
import os
import sys
import json
import csv
import argparse
import statistics
from datetime import datetime
from collections import defaultdict

def safe_float(value, default=0.0):
    """تبدیل هر مقدار به float، در صورت خطا مقدار پیش‌فرض برگردان"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = str(value).strip()
        if cleaned == "" or cleaned == "-":
            return default
        return float(cleaned)
    except:
        return default

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

def build_all_results(returns_cache_path, strategies_json_path, risk_cache_path, results_base):
    with open(returns_cache_path, "r") as f:
        returns = json.load(f)
    with open(strategies_json_path, "r") as f:
        strat_data = json.load(f)
    with open(risk_cache_path, "r") as f:
        risk = json.load(f)
    strategies = strat_data["strategies"]
    all_results = []
    for s in strategies:
        folder = s["folder"]
        group = s["group"]
        for fname in s["result_files"]:
            key = f"{group}_{folder}_{fname}"
            ret_data = returns.get(key, {"real": 0, "special": 0})
            risk_data = risk.get(key, {"max_consecutive_loss": 0, "max_consecutive_count": 0})
            file_path = os.path.join(results_base, group, folder, fname)
            period_name = ""
            total_trades = 0
            win_trades = 0
            loss_trades = 0
            win_rate = 0.0
            max_drawdown = 0.0
            profit_loss_ratio = 0.0
            sharpe_ratio = 0.0
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    info = data.get("اطلاعات_فایل", {})
                    period_name = info.get("نام_فایل", "")
                    stats = data.get("آمار_کلی_معاملات", {})
                    total_trades = safe_float(stats.get("تعداد_کل_معاملات", 0), 0)
                    win_trades = safe_float(stats.get("معاملات_سودده", 0), 0)
                    loss_trades = safe_float(stats.get("معاملات_زیانده", 0), 0)
                    win_rate_str = stats.get("نرخ_برد", "0%")
                    win_rate = safe_float(win_rate_str.replace("%", ""), 0)
                    risk_info = data.get("آنالیز_ریسک", {})
                    max_drawdown = safe_float(risk_info.get("حداکثر_افت_سرمایه", "0%").replace("%", ""), 0)
                    profit_loss_ratio = safe_float(risk_info.get("نسبت_سود_به_ضرر", 0), 0)
                    sharpe_ratio = safe_float(risk_info.get("نسبت_شارپ", 0), 0)
                except Exception as e:
                    print(f"⚠️ خطا در خواندن {file_path}: {e}")
            all_results.append({
                "folder_name": folder,
                "group": group,
                "file_name": fname,
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
            })
    return all_results

def extract_start_date_from_period(period_str):
    import re
    m = re.search(r'(\d{4}-\d{2}-\d{2})_', period_str)
    return m.group(1) if m else None

def extract_end_date_from_period(period_str):
    import re
    m = re.search(r'_(\d{4}-\d{2}-\d{2})$', period_str)
    return m.group(1) if m else None

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
        monthly_records.append({
            "year_month": ym,
            "total_return": total_return,
            "file_objects": selected,
            "start_date": min(extract_start_date_from_period(f["period_name"]) for f in selected),
            "end_date": max(extract_end_date_from_period(f["period_name"]) for f in selected)
        })
    monthly_records.sort(key=lambda x: x["year_month"])
    return monthly_records

def create_comparison_tables(all_results, output_dir):
    folder_agg = {}
    for r in all_results:
        key = (r["folder_name"], r["group"])
        if key not in folder_agg:
            folder_agg[key] = {
                "folder": r["folder_name"],
                "group": r["group"],
                "win_rates": [], "sharpes": [], "max_losses": [],
                "real_returns": [], "special_returns": [],
                "total_trades": 0, "returns_ge_1": 0, "returns_gt_0": 0
            }
        agg = folder_agg[key]
        agg["win_rates"].append(r["win_rate"])
        agg["sharpes"].append(r["sharpe_ratio"])
        agg["max_losses"].append(r["max_consecutive_loss"])
        agg["real_returns"].append(r["real_return"])
        agg["special_returns"].append(r["special_rounded_return"])
        agg["total_trades"] += r["total_trades"]
        if r["special_rounded_return"] >= 1.0:
            agg["returns_ge_1"] += 1
        if r["special_rounded_return"] > 0:
            agg["returns_gt_0"] += 1

    folder_list = []
    for (folder, group), agg in folder_agg.items():
        file_count = len(agg["win_rates"])
        avg_win_rate = statistics.mean(agg["win_rates"]) if agg["win_rates"] else 0
        avg_sharpe = statistics.mean(agg["sharpes"]) if agg["sharpes"] else 0
        avg_max_loss = statistics.mean(agg["max_losses"]) if agg["max_losses"] else 0
        avg_real_return = statistics.mean(agg["real_returns"]) if agg["real_returns"] else 0
        avg_special_return = statistics.mean(agg["special_returns"]) if agg["special_returns"] else 0
        score_res = calculate_strategy_score_new({
            "returns_ge_1": agg["returns_ge_1"],
            "returns_gt_0": agg["returns_gt_0"],
            "avg_max_loss": avg_max_loss,
            "total_files": file_count
        })
        folder_list.append({
            "folder": folder, "group": group, "file_count": file_count,
            "avg_win_rate": avg_win_rate, "avg_sharpe": avg_sharpe, "avg_max_loss": avg_max_loss,
            "avg_real_return": avg_real_return, "avg_special_return": avg_special_return,
            "total_trades": agg["total_trades"],
            "score": score_res["امتیاز_نهایی"], "rating": score_res["رتبه"],
            "is_inverse": folder.endswith("_INV")
        })
    folder_list_sorted = sorted(folder_list, key=lambda x: x["score"], reverse=True)

    out_path = os.path.join(output_dir, "جدول_مقایسه_گروه‌ها.csv")
    os.makedirs(output_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["رتبه", "نام پوشه", "گروه", "نوع", "تعداد فایل‌ها", "بازده (بر مبنای گروه)", "وین‌ریت (%)",
                         "حداکثر ضرر متوالی (%)", "نسبت شارپ", "کل معاملات", "امتیاز نهایی", "وضعیت"])
        for i, item in enumerate(folder_list_sorted, 1):
            ret = item["avg_special_return"] if item["group"] == "1" else item["avg_real_return"]
            writer.writerow([i, item["folder"], item["group"], "معکوس" if item["is_inverse"] else "اصلی",
                             item["file_count"], f"{ret:.4f}%", f"{item['avg_win_rate']:.2f}",
                             f"{item['avg_max_loss']:.2f}", f"{item['avg_sharpe']:.3f}",
                             item["total_trades"], f"{item['score']:.1f}/100", item["rating"]])
    print(f"✅ جداول مقایسه در {out_path} ذخیره شد.")

def create_optimization_ranking(all_results, output_dir):
    folder_stats = {}
    for r in all_results:
        key = f"{r['folder_name']} (گروه {r['group']})"
        if key not in folder_stats:
            folder_stats[key] = {
                "win_rates": [], "sharpes": [], "max_losses": [], "special_returns": [],
                "total_files": 0, "returns_ge_1": 0, "returns_gt_0": 0
            }
        stats = folder_stats[key]
        stats["win_rates"].append(r["win_rate"])
        stats["sharpes"].append(r["sharpe_ratio"])
        stats["max_losses"].append(r["max_consecutive_loss"])
        stats["special_returns"].append(r["special_rounded_return"])
        stats["total_files"] += 1
        if r["special_rounded_return"] >= 1.0:
            stats["returns_ge_1"] += 1
        if r["special_rounded_return"] > 0:
            stats["returns_gt_0"] += 1

    summary_data = []
    for key, stats in folder_stats.items():
        avg_win_rate = statistics.mean(stats["win_rates"]) if stats["win_rates"] else 0
        avg_sharpe = statistics.mean(stats["sharpes"]) if stats["sharpes"] else 0
        avg_max_loss = statistics.mean(stats["max_losses"]) if stats["max_losses"] else 0
        avg_special = statistics.mean(stats["special_returns"]) if stats["special_returns"] else 0
        folder_data = {
            "returns_ge_1": stats["returns_ge_1"], "returns_gt_0": stats["returns_gt_0"],
            "avg_max_loss": avg_max_loss, "total_files": stats["total_files"]
        }
        score_res = calculate_strategy_score_new(folder_data)
        summary_data.append({
            "نام_پوشه": key, "تعداد_فایل‌ها": stats["total_files"],
            "بازدهی≥۱": stats["returns_ge_1"],
            "درصد_بازدهی≥۱": round((stats["returns_ge_1"]/stats["total_files"])*100, 1) if stats["total_files"]>0 else 0,
            "بازدهی>۰": stats["returns_gt_0"],
            "درصد_بازدهی>۰": round((stats["returns_gt_0"]/stats["total_files"])*100, 1) if stats["total_files"]>0 else 0,
            "میانگین_وین‌ریت": round(avg_win_rate, 2), "میانگین_شارپ": round(avg_sharpe, 3),
            "میانگین_ضرر_متوالی": round(avg_max_loss, 2), "میانگین_بازدهی_ویژه": round(avg_special, 4),
            "امتیاز_نهایی": score_res["امتیاز_نهایی"], "رتبه": score_res["رتبه"],
        })
    summary_data.sort(key=lambda x: x["امتیاز_نهایی"], reverse=True)
    out_path = os.path.join(output_dir, "رتبه‌بندی_استراتژی‌ها.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["رتبه", "نام پوشه (گروه)", "امتیاز نهایی", "بازدهی≥۱", "بازدهی>۰", "میانگین ضرر متوالی",
                         "میانگین وین‌ریت", "میانگین شارپ", "میانگین بازدهی ویژه", "وضعیت"])
        for i, d in enumerate(summary_data, 1):
            writer.writerow([i, d["نام_پوشه"], f"{d['امتیاز_نهایی']}/100",
                             f"{d['بازدهی≥۱']} ({d['درصد_بازدهی≥۱']}%)",
                             f"{d['بازدهی>۰']} ({d['درصد_بازدهی>۰']}%)",
                             f"{d['میانگین_ضرر_متوالی']}%", f"{d['میانگین_وین‌ریت']}%",
                             f"{d['میانگین_شارپ']:.3f}", f"{d['میانگین_بازدهی_ویژه']:.4f}%", d["رتبه"]])
    print(f"✅ رتبه‌بندی استراتژی‌ها در {out_path} ذخیره شد.")

def create_monthly_summary_table(all_results, output_dir):
    strategy_map = defaultdict(list)
    for r in all_results:
        strategy_map[(r["folder_name"], r["group"])].append(r)
    all_monthly = []
    for (folder, group), results in strategy_map.items():
        monthly = compute_monthly_records_for_strategy(results, group)
        for rec in monthly:
            rec["folder"] = folder
            rec["group"] = group
            all_monthly.append(rec)
    if not all_monthly:
        return
    for rec in all_monthly:
        returns_ge_1 = sum(1 for f in rec["file_objects"] if f["special_rounded_return"] >= 1.0)
        returns_gt_0 = sum(1 for f in rec["file_objects"] if f["special_rounded_return"] > 0)
        max_loss = max((f["max_consecutive_loss"] for f in rec["file_objects"] if f["max_consecutive_loss"] is not None), default=0)
        score_data = {"returns_ge_1": returns_ge_1, "returns_gt_0": returns_gt_0, "avg_max_loss": max_loss, "total_files": 3}
        score = calculate_strategy_score_new(score_data)
        rec["score"] = score["امتیاز_نهایی"]
        rec["rating"] = score["رتبه"]
    monthly_sorted = sorted(all_monthly, key=lambda x: x["score"], reverse=True)
    out_path = os.path.join(output_dir, "جدول_خلاصه_ماهانه.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["رتبه", "نام پوشه", "گروه", "ماه", "بازده ماه (%)", "وین‌ریت ماه (%)",
                         "حداکثر ضرر متوالی ماه (%)", "میانگین شارپ ماه", "امتیاز ماه", "وضعیت"])
        for i, rec in enumerate(monthly_sorted, 1):
            total_trades = sum(f["total_trades"] for f in rec["file_objects"])
            win_trades = sum(f["win_trades"] for f in rec["file_objects"])
            win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
            avg_sharpe = statistics.mean([f["sharpe_ratio"] for f in rec["file_objects"] if f["sharpe_ratio"] is not None]) or 0
            max_loss = max((f["max_consecutive_loss"] for f in rec["file_objects"] if f["max_consecutive_loss"] is not None), default=0)
            writer.writerow([i, rec["folder"], rec["group"], rec["year_month"], f"{rec['total_return']:.4f}",
                             f"{win_rate:.2f}", f"{max_loss:.2f}", f"{avg_sharpe:.3f}",
                             f"{rec['score']:.1f}/100", rec["rating"]])
    print(f"✅ خلاصه ماهانه در {out_path} ذخیره شد.")

def create_monthly_detailed_analysis(all_results, output_dir):
    strategy_map = defaultdict(list)
    for r in all_results:
        strategy_map[(r["folder_name"], r["group"])].append(r)
    all_monthly = []
    for (folder, group), results in strategy_map.items():
        monthly = compute_monthly_records_for_strategy(results, group)
        for rec in monthly:
            rec["folder"] = folder
            rec["group"] = group
            all_monthly.append(rec)
    if not all_monthly:
        return
    out_path = os.path.join(output_dir, "جزئیات_عملکرد_ماهانه.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ردیف", "نام پوشه", "گروه", "ماه", "بازده ماه (%)", "وین‌ریت ماه (%)",
                         "حداکثر ضرر متوالی ماه (%)", "میانگین شارپ ماه", "فایل‌های تشکیل‌دهنده"])
        for i, rec in enumerate(sorted(all_monthly, key=lambda x: (x["folder"], x["year_month"])), 1):
            total_trades = sum(f["total_trades"] for f in rec["file_objects"])
            win_trades = sum(f["win_trades"] for f in rec["file_objects"])
            win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
            avg_sharpe = statistics.mean([f["sharpe_ratio"] for f in rec["file_objects"] if f["sharpe_ratio"] is not None]) or 0
            max_loss = max((f["max_consecutive_loss"] for f in rec["file_objects"] if f["max_consecutive_loss"] is not None), default=0)
            writer.writerow([i, rec["folder"], rec["group"], rec["year_month"], f"{rec['total_return']:.4f}",
                             f"{win_rate:.2f}", f"{max_loss:.2f}", f"{avg_sharpe:.3f}",
                             "; ".join(f["file_name"] for f in rec["file_objects"])])
    print(f"✅ جزئیات ماهانه در {out_path} ذخیره شد.")

def merge_all(temp_dir, output_dir):
    import glob
    pattern = os.path.join(temp_dir, "combo10_*_chunk*.csv")
    chunk_files = glob.glob(pattern)
    groups = defaultdict(list)
    for cf in chunk_files:
        base = os.path.basename(cf)
        parts = base.split('_')
        if len(parts) >= 2:
            strat = parts[1]
            groups[strat].append(cf)
    for strat, files in groups.items():
        out_path = os.path.join(output_dir, f"تحلیل_ترکیبی_الگوهای_خبری_10روزه_{strat}.csv")
        header_written = False
        with open(out_path, "w", encoding="utf-8-sig", newline="") as out_f:
            writer = None
            for cf in sorted(files):
                with open(cf, "r", encoding="utf-8-sig") as in_f:
                    reader = csv.DictReader(in_f)
                    if not header_written and reader.fieldnames:
                        writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        header_written = True
                    if writer:
                        for row in reader:
                            writer.writerow(row)
        print(f"✅ ادغام {strat} انجام شد: {out_path}")
    print("ادغام کامل شد.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=["comparison_tables", "optimization_ranking",
                          "monthly_summary", "monthly_details", "merge_all"])
    parser.add_argument("--returns-cache", help="مسیر returns_cache.json")
    parser.add_argument("--strategies-json", help="مسیر strategies_metadata.json")
    parser.add_argument("--risk-cache", help="مسیر risk_cache.json")
    parser.add_argument("--results-base", help="مسیر پایه نتایج (برای خواندن فایل‌های JSON اصلی)")
    parser.add_argument("--temp-dir", help="پوشه temp (برای merge_all)")
    parser.add_argument("--output-dir", required=True, help="پوشه خروجی")
    args = parser.parse_args()

    if args.mode in ["comparison_tables", "optimization_ranking", "monthly_summary", "monthly_details"]:
        if not args.returns_cache or not args.strategies_json or not args.risk_cache or not args.results_base:
            print("❌ برای این حالت نیاز به --returns-cache --strategies-json --risk-cache --results-base دارید.")
            sys.exit(1)
        all_results = build_all_results(args.returns_cache, args.strategies_json, args.risk_cache, args.results_base)
        if args.mode == "comparison_tables":
            create_comparison_tables(all_results, args.output_dir)
        elif args.mode == "optimization_ranking":
            create_optimization_ranking(all_results, args.output_dir)
        elif args.mode == "monthly_summary":
            create_monthly_summary_table(all_results, args.output_dir)
        elif args.mode == "monthly_details":
            create_monthly_detailed_analysis(all_results, args.output_dir)
    elif args.mode == "merge_all":
        if not args.temp_dir:
            print("❌ برای merge_all نیاز به --temp-dir دارید.")
            sys.exit(1)
        merge_all(args.temp_dir, args.output_dir)

if __name__ == "__main__":
    main()
