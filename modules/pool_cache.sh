#!/usr/bin/env bash
# pool_cache.sh  restore|pack  <REPO_DIR>
# آرشیو کش‌های pool_search: <repo>/pool_cache/pool_cache.tar.gz.enc.partNNN (هر تکه حداکثر ۹۰ MiB، رمزنگاری AES-256 + tar.gz)
# محتوای آرشیو: پوشه‌ی pool_candidates/ با فایل‌های *.npz.enc
# رمز از متغیر محیطی POOL_CACHE_PASSWORD خوانده می‌شود (روی خط فرمان نمی‌آید).
set -euo pipefail
CMD="${1:?restore|pack}"; REPO="${2:?repo dir}"
ARCH=pool_cache; BASE=pool_cache.tar.gz.enc; LEGACY=pool_candidates
: "${POOL_CACHE_PASSWORD:?POOL_CACHE_PASSWORD not set}"
OSSL=(openssl enc -aes-256-cbc -pbkdf2 -pass env:POOL_CACHE_PASSWORD)

case "$CMD" in
restore)
  mkdir -p "$REPO/$LEGACY"
  if git -C "$REPO" ls-tree -r --name-only HEAD -- "$ARCH" 2>/dev/null | grep -q "$BASE.part"; then
    git -C "$REPO" checkout HEAD -- "$ARCH"
    cat "$REPO/$ARCH/$BASE".part* | "${OSSL[@]}" -d | tar -xzf - -C "$REPO"
    echo "📦 آرشیو کش بازیابی شد: $(ls "$REPO/$ARCH" | wc -l) تکه، $(find "$REPO/$LEGACY" -name '*.npz.enc' | wc -l) فایل کش"
  elif git -C "$REPO" ls-tree -r --name-only HEAD -- "$LEGACY" 2>/dev/null | grep -q .; then
    # مهاجرت: هنوز آرشیو نیست، فایل‌های جداگانه‌ی قدیمی را بگیر
    git -C "$REPO" checkout HEAD -- "$LEGACY"
    echo "📦 (مهاجرت) کش‌های جداگانه‌ی قدیمی: $(find "$REPO/$LEGACY" -name '*.npz.enc' | wc -l) فایل"
  else
    echo "ℹ️ هیچ کشی هنوز وجود ندارد."
  fi
  ;;
pack)
  cd "$REPO"
  N=$(find "$LEGACY" -name '*.npz.enc' 2>/dev/null | wc -l)
  rm -rf "$ARCH"; mkdir -p "$ARCH"
  if [ "$N" -eq 0 ]; then echo "ℹ️ کشی برای بسته‌بندی نیست؛ آرشیو خالی می‌ماند."; exit 0; fi
  tar -czf - "$LEGACY" | "${OSSL[@]}" -salt | split -b 90M -d -a 3 - "$ARCH/$BASE.part"
  # راستی‌آزمایی: آرشیو باید قابل رمزگشایی باشد و همان تعداد فایل داشته باشد
  V=$(cat "$ARCH/$BASE".part* | "${OSSL[@]}" -d | tar -tzf - | grep -c '\.npz\.enc$' || true)
  if [ "$V" != "$N" ]; then echo "❌ راستی‌آزمایی آرشیو ناموفق: $V از $N"; exit 1; fi
  echo "✅ آرشیو ساخته شد: $N فایل در $(ls "$ARCH" | wc -l) تکه ($(du -sh "$ARCH" | cut -f1))"
  ;;
*) echo "usage: $0 restore|pack REPO_DIR"; exit 2;;
esac
