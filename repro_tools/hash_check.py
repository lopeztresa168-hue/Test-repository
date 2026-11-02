#!/usr/bin/env python3
"""
repro_tools/hash_check.py

قاعده سخت اعتبارسنجی #1: قبل از هر پردازش، SHA-256 ماژول(های) استفاده‌شده
(در چک‌اوتِ قفل‌شده روی commit مشخص) را با هش‌های مرجعِ کاربر مقایسه می‌کند.
اگر فرق داشت، خطا را بلند و واضح چاپ می‌کند (ولی طبق درخواست کاربر، اجرا
متوقف نمی‌شود؛ چون خودِ این اجرا برای پیدا کردن منشأ اختلاف داده است، نه
برای اجرای «تمیز»).

Usage:
    python3 hash_check.py <path-to-combo_10day.py> [<path-to-combo_monthly.py>]

خروجی: همیشه exit 0 (طبق درخواست کاربر: هشدار بده و ادامه بده)، مگر این‌که
خودِ فایل یافت نشود که آن‌گاه exit 1 می‌شود (چون بدون فایل، پردازش هم اصلا
ممکن نیست).
"""
import hashlib
import sys
import os

REFERENCE_HASHES = {
    "combo_10day.py":   "c3aa827684c65f46947453e20eda1b2c1404fe6774d13bd15022836a32d64135",
    "combo_monthly.py": "9a906b8aedbc661dbca0055f2f3d6022ee474452fce1cbab6b5412d796bbc77a",
}


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(paths):
    any_mismatch = False
    for path in paths:
        base = os.path.basename(path)
        if not os.path.exists(path):
            print(f"❌ فایل یافت نشد: {path}")
            return 1
        actual = sha256_of(path)
        expected = REFERENCE_HASHES.get(base)
        print(f"::group::[HASH CHECK] {base}")
        print(f"  مسیر: {path}")
        print(f"  sha256 محاسبه‌شده : {actual}")
        print(f"  sha256 مرجع (کاربر): {expected}")
        if expected is None:
            print("  ⚠️ برای این فایل هش مرجعی ثبت نشده — رد شد.")
        elif actual == expected:
            print("  ✅ مطابق است — منطق ماژول نسبت به نسخه‌ی مرجع کاربر تغییر نکرده.")
        else:
            any_mismatch = True
            print("  ‼️‼️‼️ عدم تطابق! این ماژول با نسخه‌ای که کاربر مرجع قرار داده متفاوت است.")
            print("  ‼️‼️‼️ اجرا طبق قاعده‌ی کاربر متوقف نمی‌شود، ولی نتیجه‌ی این اجرا دیگر")
            print("  ‼️‼️‼️ قابل‌مقایسه‌ی مستقیم با اجرای run 34950738285 نیست — در گزارش نهایی علامت بزن.")
        print("::endgroup::")

    if any_mismatch:
        # کد خروج غیرصفرِ جداگانه تا CI بتواند این حالت را در summary مشخص کند،
        # بدون این‌که step را با continue-on-error خالی از اطلاع fail کند.
        print("HASH_MISMATCH=true")
    else:
        print("HASH_MISMATCH=false")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: hash_check.py <file1.py> [<file2.py> ...]")
        sys.exit(2)
    sys.exit(main(sys.argv[1:]))
