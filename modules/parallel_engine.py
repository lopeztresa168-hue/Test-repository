"""
parallel_engine.py
موتور موازی‌سازی خودتنظیم (self-tuning) برای پردازش کارهای مستقل (ترکیب‌ها)
داخل هر رانر GitHub Actions.

فقط این فایل + تغییرات کوچک در combo_10day.py / combo_monthly.py اضافه می‌شوند؛
هیچ YAML ای دست‌خورده نمی‌شود.
"""

import os
import json
import time
import hashlib
import multiprocessing as mp
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

CACHE_FILENAME = ".parallel_worker_cache.json"
MEMORY_DANGER_PCT = 85.0
SLOWDOWN_FACTOR = 1.35
MIN_WORKERS = 1

_CTX = mp.get_context("fork") if "fork" in mp.get_all_start_methods() else mp.get_context()


def _mem_percent():
    if _HAS_PSUTIL:
        return psutil.virtual_memory().percent
    try:
        with open("/proc/meminfo") as f:
            info = {}
            for line in f:
                k, v = line.split(":")
                info[k.strip()] = int(v.strip().split()[0])
        total = info.get("MemTotal", 1)
        avail = info.get("MemAvailable", total)
        return 100.0 * (1 - avail / total)
    except Exception:
        return 0.0


def _cache_path(base_dir):
    return os.path.join(base_dir, CACHE_FILENAME)


def _cache_key(total_tasks):
    cpu = os.cpu_count() or 4
    raw = f"cpu={cpu}|tasks_bucket={total_tasks // 100}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def load_cached_workers(base_dir, total_tasks):
    path = _cache_path(base_dir)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entry = data.get(_cache_key(total_tasks))
        if entry and isinstance(entry.get("workers"), int):
            print(f"⚡ [parallel_engine] تعداد کارگر بهینه از کش خوانده شد: {entry['workers']}")
            return entry["workers"]
    except Exception:
        pass
    return None


def save_cached_workers(base_dir, total_tasks, workers):
    path = _cache_path(base_dir)
    data = {}
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    data[_cache_key(total_tasks)] = {"workers": workers, "ts": time.time(), "cpu_count": os.cpu_count()}
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ [parallel_engine] ذخیره کش ممکن نشد: {e}")


def _run_batch(worker_fn, batch_items, n_workers):
    """batch_items: [(idx, item), ...]. برمی‌گرداند (results_dict, failed_idx_list)."""
    results = {}
    failed = []

    if n_workers <= 1:
        for idx, item in batch_items:
            try:
                results[idx] = worker_fn(item)
            except MemoryError:
                print(f"⚠️ [parallel_engine] MemoryError در کار {idx} (سریال) → ناموفق")
                failed.append(idx)
            except Exception as e:
                print(f"⚠️ [parallel_engine] خطا در کار {idx} (سریال): {e}")
                failed.append(idx)
        return results, failed

    try:
        with ProcessPoolExecutor(max_workers=n_workers, mp_context=_CTX) as ex:
            future_to_idx = {ex.submit(worker_fn, item): idx for idx, item in batch_items}
            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    results[idx] = fut.result()
                except MemoryError:
                    print(f"⚠️ [parallel_engine] MemoryError در کار {idx} → ناموفق")
                    failed.append(idx)
                except Exception as e:
                    print(f"⚠️ [parallel_engine] خطا در کار {idx}: {e}")
                    failed.append(idx)
    except BrokenProcessPool as e:
        print(f"❌ [parallel_engine] Pool خراب شد (احتمالاً OOM-kill سیستم‌عامل): {e}")
        done_idx = set(results.keys())
        for idx, _ in batch_items:
            if idx not in done_idx:
                failed.append(idx)

    return results, failed


def benchmark_workers(worker_fn, sample_items, max_workers, sample_size=12):
    """با نمونه‌ی کوچک، تعداد کارگر بهینه را پیدا می‌کند (بدون MemoryError و بدون کاهش سرعت)."""
    sample = sample_items[:min(sample_size, len(sample_items))]
    if not sample:
        return 1

    best_workers, best_rate = 1, None

    for n in range(1, max_workers + 1):
        start_mem = _mem_percent()
        start = time.time()
        _results, failed = _run_batch(worker_fn, list(enumerate(sample)), n)
        elapsed = max(time.time() - start, 1e-6)
        end_mem = _mem_percent()
        rate = len(sample) / elapsed

        print(f"   🔧 تست {n} کارگر → {elapsed:.2f}s ({rate:.2f} کار/ثانیه), "
              f"حافظه {start_mem:.0f}%→{end_mem:.0f}%, ناموفق={len(failed)}")

        if end_mem >= MEMORY_DANGER_PCT or failed:
            print(f"   ⛔ {n} کارگر رد شد (حافظه/خطا) → توقف بنچمارک")
            break

        if best_rate is None or rate > best_rate:
            best_rate, best_workers = rate, n
        else:
            print(f"   ⛔ کارگر بیشتر = سرعت کمتر → توقف روی {best_workers} کارگر")
            break

    print(f"✅ [parallel_engine] تعداد کارگر بهینه: {best_workers}")
    return best_workers


def run_adaptive(worker_fn, items, base_dir=".", force_workers=None, batch_multiplier=2, sample_size=12):
    """
    اجرای موازی خودتنظیم روی items (هر آیتم یک ترکیب/کار مستقل).
    worker_fn باید در سطح ماژول تعریف شده باشد (قابل pickle)، نه lambda/closure.
    خروجی: (results_in_order, failed_indices)
    """
    total = len(items)
    if total == 0:
        return [], []

    cpu_count = os.cpu_count() or 4
    max_workers = max(1, min(cpu_count, total))

    if force_workers is not None:
        n_workers = max(MIN_WORKERS, min(force_workers, max_workers))
        print(f"⚙️ [parallel_engine] تعداد کارگر دستی: {n_workers}")
    else:
        n_workers = load_cached_workers(base_dir, total)
        if n_workers is None:
            print("🔍 [parallel_engine] بنچمارک اولیه روی نمونه کوچک...")
            n_workers = benchmark_workers(worker_fn, items, max_workers, sample_size=sample_size)
            save_cached_workers(base_dir, total, n_workers)
        n_workers = max(MIN_WORKERS, min(n_workers, max_workers))

    results = [None] * total
    failed_indices = []
    pending = list(enumerate(items))
    batch_size = max(1, n_workers * batch_multiplier)

    HISTORY_WINDOW = 5
    rate_history = deque(maxlen=HISTORY_WINDOW)  # نرخ (کار/ثانیه) دسته‌های اخیر
    overall_start = time.time()
    total_done = 0

    while pending:
        batch, pending = pending[:batch_size], pending[batch_size:]
        start_mem = _mem_percent()
        start = time.time()
        batch_results, batch_failed = _run_batch(worker_fn, batch, n_workers)
        elapsed = max(time.time() - start, 1e-6)
        end_mem = _mem_percent()
        rate = len(batch) / elapsed

        for idx, res in batch_results.items():
            results[idx] = res
        failed_indices.extend(batch_failed)
        total_done += len(batch)

        avg_recent = sum(rate_history) / len(rate_history) if rate_history else None
        trend = ""
        if avg_recent is not None:
            pct_change = (rate - avg_recent) / avg_recent * 100
            arrow = "🔺" if pct_change > 5 else ("🔻" if pct_change < -5 else "➡️")
            trend = f", میانگین {len(rate_history)} دسته اخیر={avg_recent:.2f}/s {arrow}{pct_change:+.0f}%"

        print(f"📦 [parallel_engine] دسته: {len(batch)} کار در {elapsed:.2f}s ({rate:.2f}/s) "
              f"با {n_workers} کارگر, حافظه={end_mem:.0f}%, ناموفق={len(batch_failed)}{trend}")

        should_shrink = False
        if end_mem >= MEMORY_DANGER_PCT:
            print(f"⚠️ [parallel_engine] حافظه به آستانه خطر رسید ({end_mem:.0f}%) → کاهش کارگر")
            should_shrink = True
        elif len(rate_history) >= 2 and avg_recent is not None and rate < avg_recent / SLOWDOWN_FACTOR:
            # فقط وقتی حداقل ۲ دسته‌ی قبلی در تاریخچه باشد قضاوت می‌کنیم، تا نویز
            # یک دسته‌ی تکی باعث کاهش نادرست کارگرها نشود.
            print(f"⚠️ [parallel_engine] کندی محسوس نسبت به میانگین {len(rate_history)} "
                  f"دسته‌ی اخیر → کاهش کارگر")
            should_shrink = True

        if should_shrink and n_workers > MIN_WORKERS:
            n_workers = max(MIN_WORKERS, n_workers - 1)
            batch_size = max(1, n_workers * batch_multiplier)
            save_cached_workers(base_dir, total, n_workers)
            rate_history.clear()  # بعد از تغییر تعداد کارگر، تاریخچه قبلی قابل مقایسه نیست

        rate_history.append(rate)

    overall_elapsed = max(time.time() - overall_start, 1e-6)
    overall_rate = total_done / overall_elapsed
    print(f"🏁 [parallel_engine] پایان: موفق={total - len(failed_indices)}, "
          f"ناموفق={len(failed_indices)} از {total}, "
          f"زمان کل={overall_elapsed:.2f}s, میانگین سرعت کل={overall_rate:.2f} کار/ثانیه")
    return results, failed_indices
