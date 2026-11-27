#!/usr/bin/env bash
# استفاده:  bash modules/pool_ensure_cache.sh FEE MIN_TRADES [END]
#
# برای اجراهای tagدار (ماتریکس) که کش ترکیب fee/min_trades/end‌شان به‌روز نیست:
# یک اجرای «فقط-کش» از analysis.yml (pool_cache_only=true، بدون tag) می‌سازد و تا پایان کل زنجیره‌اش
# (pool-continue ها) صبر می‌کند. اجرای فقط-کش از همان مسیر معمولی plan → batch → collect (تنها نویسنده‌ی آرشیو
# pool_cache/، فشرده و تقسیم‌شده با modules/pool_cache.sh) استفاده می‌کند.
#
# کد خروج: 0 = کش آماده | 1 = اجرای فقط-کش شکست خورد | 75 = زمان انتظار تمام شد (workflow اجرا را دوباره صف می‌کند)
#
# نام اجرا باید دقیقاً با run-name در analysis.yml یکی باشد:
#   pool_search cache [fee=<FEE> min=<MIN> end=<END|default>]
set -uo pipefail

FEE="${1:?fee}"; MIN="${2:?min_trades}"; END="${3:-}"
WF="analysis.yml"
REPO="${GITHUB_REPOSITORY:?}"
REF="${GITHUB_REF_NAME:-main}"
TITLE="pool_search cache [fee=${FEE} min=${MIN} end=${END:-default}]"
PREFIX="pool_search cache ["
DEADLINE_SEC=$(( ${ENSURE_DEADLINE_MIN:-335} * 60 ))
T0=$(date +%s)
START_ISO=$(date -u -d "@$T0" +%Y-%m-%dT%H:%M:%SZ)
DISPATCHES=0

echo "🔧 هدف: $TITLE"
sleep $(( RANDOM % 25 ))   # چند اجرای tagدارِ هم‌ترکیب هم‌زمان شروع می‌شوند؛ کمی پخش‌شان کن

list_runs() { gh run list --repo "$REPO" --workflow "$WF" --event workflow_dispatch --limit 200 \
                --json displayTitle,status,conclusion,createdAt,updatedAt 2>/dev/null; }

while :; do
  if [ $(( $(date +%s) - T0 )) -ge "$DEADLINE_SEC" ]; then
    echo "⏳ سقف انتظار تمام شد؛ کش هنوز کامل نیست (اجرای فقط-کش ادامه دارد)."
    exit 75
  fi

  RUNS=$(list_runs) || { echo "⚠️ خواندن لیست اجراها ناموفق بود؛ دوباره..."; sleep 30; continue; }

  OWN_ACTIVE=$(jq --arg t "$TITLE" '[.[] | select(.displayTitle==$t and .status!="completed")] | length' <<<"$RUNS")
  ANY_ACTIVE=$(jq --arg p "$PREFIX" '[.[] | select((.displayTitle|startswith($p)) and .status!="completed")] | length' <<<"$RUNS")
  # آخرین اجرای تمام‌شده‌ی همین ترکیب بعد از شروع ما (cancelled/skipped یعنی «جایگزین شد»، نه نتیجه)
  LAST=$(jq -r --arg t "$TITLE" --arg s "$START_ISO" '
    [.[] | select(.displayTitle==$t and .status=="completed" and .updatedAt>=$s
                  and (.conclusion=="success" or .conclusion=="failure" or .conclusion=="timed_out"))]
    | sort_by(.updatedAt) | last | .conclusion // ""' <<<"$RUNS")

  if [ "$OWN_ACTIVE" -gt 0 ]; then
    echo "⏳ اجرای فقط-کش این ترکیب در جریان/صف است ($OWN_ACTIVE)..."; sleep 60; continue
  fi

  if [ "$LAST" = "success" ]; then
    # آخرین حلقه‌ی زنجیره‌ی pool-continue قبل از پایان خودش اجرای بعدی را dispatch می‌کند؛ ولی لیست API ممکن است چند ثانیه عقب باشد → دوبار تأیید
    sleep 25
    AGAIN=$(list_runs | jq --arg t "$TITLE" '[.[] | select(.displayTitle==$t and .status!="completed")] | length' 2>/dev/null || echo 1)
    if [ "${AGAIN:-1}" -eq 0 ]; then
      echo "✅ کش این ترکیب کامل شد."
      exit 0
    fi
    continue
  fi

  if [ "$LAST" = "failure" ] || [ "$LAST" = "timed_out" ]; then
    echo "❌ اجرای فقط-کش این ترکیب شکست خورد ($LAST) — لاگ اجراهای «$TITLE» را ببین."
    exit 1
  fi

  # اجرای فعالی برای این ترکیب نیست (یا جایگزین/لغو شده). اگر ترکیب دیگری در حال ساخت است صبر کن:
  # همه‌ی اجراهای فقط-کش یک گروه همزمانی دارند (یک اجرا + یک صف‌شده) و dispatch هم‌زمان یکی را لغو می‌کند.
  if [ "$ANY_ACTIVE" -gt 0 ]; then
    echo "⏳ ساخت کش ترکیب دیگری در جریان است؛ نوبت این ترکیب بعداً می‌رسد..."
    sleep $(( 45 + RANDOM % 30 )); continue
  fi

  DISPATCHES=$((DISPATCHES + 1))
  if [ "$DISPATCHES" -gt 12 ]; then
    echo "❌ بیش از ۱۲ بار dispatch بدون نتیجه."
    exit 1
  fi
  ARGS=(-f analysis_module=pool_search -f pool_cache_only=true -f pool_fee="$FEE" -f pool_min_trades="$MIN")
  if [ -n "$END" ]; then ARGS+=(-f pool_end="$END"); fi
  if gh workflow run "$WF" --repo "$REPO" --ref "$REF" "${ARGS[@]}" 2>&1; then
    echo "🚀 اجرای فقط-کش dispatch شد (تلاش $DISPATCHES)."
  else
    echo "⚠️ dispatch ناموفق؛ دوباره..."
  fi
  sleep 30
done
