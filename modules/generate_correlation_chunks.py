#!/usr/bin/env python3
"""
generate_correlation_chunks.py

ورودی‌ها:
  --signatures-dir DIR   پوشه حاوی فایل‌های .jsonl استخراج‌شده از archive‌ها
  --queue-file FILE       all_combinations_correlation.json (آرایه‌ای از آیتم‌های صف)
  --output-dir DIR        پوشه خروجی برای chunk‌ها

خروجی‌ها:
  chunk_0.json, chunk_1.json, ...   هر chunk آرایه‌ای از آیتم‌های صف
  num_chunks.txt                    تعداد chunk‌های تولیدشده
  empty.flag                        اگر هیچ آیتم قابل پردازشی وجود نداشته باشد

منطق:
  - صف (queue-file) را می‌خواند؛ هر آیتم می‌تواند یکی از دو فرمت داشته باشد:
      A) {"signature": "...", "coin_composition": "...", ...}
      B) {"signature_path": "module/strategy/base_name", ...}
  - فرمت B را به فرمت A تبدیل می‌کند:
      signature      = base_name (آخرین قسمت مسیر)
      coin_composition = اولین قسمت base_name قبل از اولین "_"
  - آیتم‌هایی که فایل .jsonl متناظرشان در signatures-dir وجود ندارد حذف می‌شوند
    (این آیتم‌ها در صف‌اند ولی archive هنوز extract نشده — دور بعدی پردازش می‌شوند)
  - آیتم‌های باقی‌مانده را unique_by(coin_composition + "|||" + signature) می‌کند
  - در chunk‌های MAX_CHUNK_SIZE تایی تقسیم می‌کند

FIX (2026-06):
  - build_jsonl_index اکنون همه سطوح مسیر نسبی را index می‌کند نه فقط یه سطح parent.
  - normalize_item فرمت B: کل signature_path در signature نگه داشته می‌شه (نه فقط base_name)
    تا combo paths مثل combo_10day/Best_15m/Best_15m_chunk_0_... قابل match باشند.
  - find_jsonl_for_item دو مرحله match دارد:
      1. exact match (فایل): بررسی stems و rel_paths برای non-combo
      2. prefix match (دایرکتوری): برای combo chunks که sig یه chunk-name است
         و فایل‌های individual داخلش در index هستن — اگر هر rel_path با
         sig (بدون timestamp) شروع بشه، chunk موجود تلقی می‌شه.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

MAX_CHUNK_SIZE = 500

# regex برای strip کردن timestamp از انتهای stem
TS_RE = re.compile(r'_\d{8}T\d{6}Z$')


def strip_ts(name: str) -> str:
    """حذف timestamp از انتهای نام فایل (e.g. chunk_0_20260628T224502Z → chunk_0)"""
    return TS_RE.sub('', name)


def build_jsonl_index(signatures_dir: Path) -> tuple:
    """
    یک‌بار همه فایل‌های .jsonl را index می‌کند.

    برمی‌گرداند:
      - stems: مجموعه stem نام فایل‌ها (بدون پسوند، بدون مسیر) + نسخه بدون timestamp
      - rel_paths: مجموعه همه زیرمسیرهای نسبی ممکن (بدون پسوند)
                   مثلاً برای combo_monthly/Best_15m/SOLUSDT/monthly_fibonacci_full.jsonl:
                     "combo_monthly/Best_15m/SOLUSDT/monthly_fibonacci_full"
                     "Best_15m/SOLUSDT/monthly_fibonacci_full"
                     "SOLUSDT/monthly_fibonacci_full"
                     "monthly_fibonacci_full"
                   + نسخه‌های بدون timestamp برای هر کدام
    """
    stems = set()
    rel_paths = set()

    for p in signatures_dir.rglob("*.jsonl"):
        stem = p.stem
        stem_no_ts = strip_ts(stem)

        # ① stems دقیق و بدون timestamp
        stems.add(stem)
        if stem_no_ts != stem:
            stems.add(stem_no_ts)

        # ② همه زیرمسیرهای نسبی ممکن
        try:
            rel_parts = p.relative_to(signatures_dir).with_suffix("").parts
        except ValueError:
            rel_parts = (stem,)

        for i in range(len(rel_parts)):
            sub = "/".join(rel_parts[i:])
            rel_paths.add(sub)
            # نسخه بدون timestamp فقط برای آخرین بخش (stem)
            sub_no_ts = "/".join(rel_parts[i:-1] + (strip_ts(rel_parts[-1]),)) if rel_parts else sub
            if sub_no_ts != sub:
                rel_paths.add(sub_no_ts)

    return stems, rel_paths


def find_jsonl_for_item(item: dict, stems: set, rel_paths: set,
                        rel_paths_list: list = None) -> bool:
    """
    بررسی می‌کند که فایل .jsonl متناظر آیتم در index وجود دارد.
    چندین روش match برای پوشش ساختارهای مختلف sig:
      - match دقیق فایل (non-combo)
      - match prefix دایرکتوری (combo): sig یه chunk-name هست که
        فایل‌های individual داخلش در index هستن
    """
    sig = item.get("signature", "")
    coin = item.get("coin_composition", "")
    if not sig:
        return False

    # نرمال‌سازی پسوند
    sig_no_ext = sig[:-6] if sig.endswith(".jsonl") else sig

    # basename آخر sig (بعد از آخرین /)
    sig_basename = sig_no_ext.split("/")[-1]
    sig_basename_no_ts = strip_ts(sig_basename)
    sig_parts = sig_no_ext.split("/")
    sig_no_ts = "/".join(sig_parts[:-1] + [strip_ts(sig_parts[-1])]) if sig_parts else sig_no_ext

    candidates_stems = {sig_no_ext, sig_basename, sig_basename_no_ts}
    candidates_rel = {sig_no_ext, sig_no_ts, sig_basename, sig_basename_no_ts}

    if coin:
        candidates_rel.add(f"{coin}/{sig_no_ext}")
        candidates_rel.add(f"{coin}/{sig_basename}")
        candidates_rel.add(f"{coin}/{sig_basename_no_ts}")

    # همه زیرمسیرهای sig برای match در rel_paths
    for i in range(len(sig_parts)):
        sub = "/".join(sig_parts[i:])
        candidates_rel.add(sub)
        sub_no_ts = "/".join(sig_parts[i:-1] + [strip_ts(sig_parts[-1])]) if sig_parts else sub
        if sub_no_ts != sub:
            candidates_rel.add(sub_no_ts)

    # بررسی در stems (match دقیق فایل)
    for c in candidates_stems:
        if c and c in stems:
            return True

    # بررسی در rel_paths (match دقیق فایل)
    for c in candidates_rel:
        if c and c in rel_paths:
            return True

    # ── PREFIX CHECK برای combo chunks ──────────────────────────────
    # sig یه chunk-name هست مثل: combo_10day/Best_15m/Best_15m_chunk_0_20260628T224502Z
    # فایل‌های individual در index: combo_10day/Best_15m/BTCUSDT/monthly_fibonacci_full
    # prefix مشترک = parent dir sig = combo_10day/Best_15m
    #
    # ⚠️ این روش فقط وقتی sig حداقل ۲ سطح داشته باشه و basename شامل "chunk" باشه
    #    فعال می‌شه تا false positive جلوگیری بشه.
    if rel_paths_list is None:
        rel_paths_list = list(rel_paths)

    if len(sig_parts) >= 2 and "chunk" in sig_basename.lower():
        # parent = همه parts به جز آخری (chunk-name)
        parent = "/".join(sig_parts[:-1])
        if len(parent) >= 3:
            prefix_slash = parent + "/"
            for rp in rel_paths_list:
                if rp.startswith(prefix_slash):
                    return True


def normalize_item(raw_item: dict):
    """
    هر دو فرمت آیتم صف را به فرمت استاندارد {coin_composition, signature, ...}
    تبدیل می‌کند. None برمی‌گرداند اگر آیتم قابل تبدیل نباشد.
    """
    if not isinstance(raw_item, dict):
        return None

    # فرمت A: از پیش normalized
    if "signature" in raw_item and "coin_composition" in raw_item:
        sig = str(raw_item["signature"]).strip()
        coin = str(raw_item["coin_composition"]).strip()
        if sig and coin:
            return {**raw_item, "signature": sig, "coin_composition": coin}
        return None

    # فرمت B: signature_path = "module/strategy/base_name"
    if "signature_path" in raw_item:
        sig_path = str(raw_item["signature_path"]).strip()
        if not sig_path:
            return None
        parts = sig_path.split("/")
        base_name = parts[-1] if parts else sig_path
        # coin_composition = اولین قسمت base_name قبل از اولین "_"
        coin = base_name.split("_")[0] if "_" in base_name else base_name
        # FIX: کل مسیر رو در signature نگه می‌داریم تا find_jsonl_for_item
        # بتونه combo paths مثل combo_10day/Best_15m/Best_15m_chunk_0_... رو پیدا کنه.
        # base_name فقط برای coin_composition استفاده می‌شه.
        return {
            **raw_item,
            "signature": sig_path,
            "coin_composition": coin,
        }

    return None


def main():
    parser = argparse.ArgumentParser(description="تولید chunk‌های correlation از صف")
    parser.add_argument("--signatures-dir", required=True,
                        help="پوشه حاوی فایل‌های .jsonl")
    parser.add_argument("--queue-file", required=True,
                        help="مسیر all_combinations_correlation.json")
    parser.add_argument("--output-dir", required=True,
                        help="پوشه خروجی chunk‌ها")
    args = parser.parse_args()

    signatures_dir = Path(args.signatures_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── خواندن صف ──────────────────────────────────────────────────
    queue_path = Path(args.queue_file)
    if not queue_path.exists() or queue_path.stat().st_size == 0:
        print("⚠️ فایل صف یافت نشد یا خالی است → empty.flag", flush=True)
        (output_dir / "empty.flag").touch()
        return

    with open(queue_path, encoding="utf-8") as f:
        try:
            raw_queue = json.load(f)
        except json.JSONDecodeError as e:
            print(f"❌ خطا در parse صف: {e}", flush=True)
            (output_dir / "empty.flag").touch()
            return

    if not isinstance(raw_queue, list) or len(raw_queue) == 0:
        print("ℹ️ صف خالی است → empty.flag", flush=True)
        (output_dir / "empty.flag").touch()
        return

    print(f"[DEBUG] تعداد آیتم‌های خام صف: {len(raw_queue)}", flush=True)

    # ── normalize ──────────────────────────────────────────────────
    normalized = []
    for item in raw_queue:
        n = normalize_item(item)
        if n is not None:
            normalized.append(n)

    print(f"[DEBUG] تعداد آیتم‌های normalize‌شده: {len(normalized)}", flush=True)

    # ── حذف duplicates با کلید ترکیبی ─────────────────────────────
    seen_keys = set()
    deduped = []
    for item in normalized:
        key = item["coin_composition"] + "|||" + item["signature"]
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(item)

    print(f"[DEBUG] تعداد آیتم‌های یکتا (بعد از dedup): {len(deduped)}", flush=True)

    # ── ساخت index یک‌باره از همه فایل‌های .jsonl ─────────────────
    if signatures_dir.exists():
        print(f"[DEBUG] در حال ساخت index از {signatures_dir} ...", flush=True)
        stems, rel_paths = build_jsonl_index(signatures_dir)
        print(f"[DEBUG] index آماده شد: {len(stems)} stem و {len(rel_paths)} مسیر نسبی منحصربه‌فرد", flush=True)

        # [DIAG] نمونه از stems و rel_paths و آیتم‌های صف برای تشخیص mismatch
        sample_stems = sorted(stems)[:5]
        sample_rels = sorted(rel_paths)[:5]
        print(f"[DIAG] نمونه stems در index: {sample_stems}", flush=True)
        print(f"[DIAG] نمونه rel_paths در index: {sample_rels}", flush=True)
        sample_items = deduped[:3]
        for it in sample_items:
            print(f"[DIAG] آیتم صف — coin={it.get('coin_composition')} sig={it.get('signature')}", flush=True)

        before_filter = len(deduped)
        matched = []
        missed = []
        rel_paths_list = list(rel_paths)  # یک‌بار list می‌کنیم برای prefix check کارآمد
        for item in deduped:
            if find_jsonl_for_item(item, stems, rel_paths, rel_paths_list):
                matched.append(item)
            else:
                missed.append(item)

        skipped = before_filter - len(matched)
        if skipped > 0:
            print(f"[DEBUG] {skipped} آیتم بدون فایل .jsonl حذف شدند "
                  f"(archive هنوز extract نشده)", flush=True)
            for it in missed[:5]:
                print(f"[DIAG] miss — coin={it.get('coin_composition')} "
                      f"sig={it.get('signature')}", flush=True)
        else:
            print(f"[DEBUG] همه {before_filter} آیتم فایل .jsonl متناظر دارند ✅", flush=True)

        deduped = matched
    else:
        print(f"⚠️ signatures-dir وجود ندارد: {signatures_dir} — "
              f"فیلتر .jsonl رد می‌شود", flush=True)

    print(f"[DEBUG] تعداد آیتم‌های نهایی برای پردازش: {len(deduped)}", flush=True)

    if len(deduped) == 0:
        print("ℹ️ هیچ آیتم قابل پردازشی یافت نشد → empty.flag", flush=True)
        (output_dir / "empty.flag").touch()
        return

    # ── تقسیم به chunk ────────────────────────────────────────────
    chunks = [deduped[i:i + MAX_CHUNK_SIZE]
              for i in range(0, len(deduped), MAX_CHUNK_SIZE)]
    num_chunks = len(chunks)

    for idx, chunk in enumerate(chunks):
        chunk_path = output_dir / f"chunk_{idx}.json"
        with open(chunk_path, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)
        print(f"[DEBUG] chunk_{idx}.json: {len(chunk)} آیتم", flush=True)

    (output_dir / "num_chunks.txt").write_text(str(num_chunks), encoding="utf-8")

    print(f"✅ {num_chunks} chunk تولید شد — مجموع {len(deduped)} آیتم", flush=True)


if __name__ == "__main__":
    main()
