#!/usr/bin/env python3
import sys, json, itertools

def generate_new_combos(existing_strategies, new_strategies, current_k, completed_sets):
    """
    existing_strategies: لیست تمام استراتژی‌ها (به ترتیب)
    new_strategies: لیست استراتژی‌های جدید که قبلاً نبودند
    current_k: عدد k فعلی برای ترکیب‌ها (۲, ۳, ...)
    completed_sets: دیکشنری از ترکیب‌های انجام‌شده
    """
    all_combos = []
    if current_k < 2:
        current_k = 2

    # تولید ترکیب‌های k عضوی که حداقل یکی از اعضای new_strategies را داشته باشند
    for combo in itertools.combinations(existing_strategies, current_k):
        # تبدیل به tuple برای جستجو
        if any(s in new_strategies for s in combo):
            combo_key = ",".join(combo)
            if combo_key not in completed_sets.get(str(current_k), {}):
                all_combos.append(combo_key)
    return all_combos

def main():
    if len(sys.argv) != 5:
        print("Usage: python portfolio_checkpoint.py <all_strategies.json> <checkpoint.json> <output_combos.json> <new_strategies.json>")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        all_strategies = json.load(f)  # لیست نام استراتژی‌ها

    with open(sys.argv[2], 'r') as f:
        checkpoint = json.load(f)  # portfolio_checkpoint.json

    with open(sys.argv[3], 'w') as fout:
        # اگر new_strategies.json خالی بود، یعنی هیچ استراتژی جدیدی نیست
        new_strategies = []
        if sys.argv[4] != "empty":
            with open(sys.argv[4], 'r') as fnew:
                new_strategies = json.load(fnew)

        last_n = checkpoint.get("last_n", 0)
        current_k = checkpoint.get("current_k", 2)
        completed_sets = checkpoint.get("completed_sets", {})

        # اگر N > last_n، یعنی استراتژی جدید داریم
        N = len(all_strategies)
        new_combos = []
        if N > last_n:
            new_strats = all_strategies[last_n:]  # استراتژی‌های جدید
            new_combos = generate_new_combos(all_strategies, new_strats, current_k, completed_sets)
            # به‌روزرسانی checkpoint
            checkpoint["last_n"] = N
            # ترکیب‌های جدید را ذخیره کن
        else:
            # اگر استراتژی جدید نداشتیم، ممکن است ترکیب‌های مانده از k قبلی باشد
            # یا k را افزایش دهیم
            # اما اینجا فقط برای اجرای اولیه است
            pass

        # خروجی: لیست ترکیب‌های جدید
        json.dump({"new_combos": new_combos, "updated_checkpoint": checkpoint}, fout)

if __name__ == "__main__":
    main()