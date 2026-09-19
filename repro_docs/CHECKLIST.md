# چک‌لیست قواعد سخت

## قواعد سخت (نقض‌نشدنی)

| # | قاعده | کجا رعایت شده |
|---|---|---|
| 1 | فقط فایل جدید | همه‌ی مسیرهای تحویل‌شده جدیدند: `.github/workflows/repro_verbose.yml`, `modules_verbose/combo_10day_verbose.py`, `repro_tools/*`, `repro_data/rerun_queue_290.json`, `repro_docs/*`. هیچ فایلی از `analysis_fixed_batch.yml`, `combo_10day.py`, `combo_monthly.py` ویرایش نشده (sha256 هرکدام قبل/بعد یکسان است). |
| 2 | منطق دقیقاً همان | همان مراحل/ترتیب/آرگومان‌های CLI (`--trades-json`, `--news-dir data/news`, `--strategy-folder`, `--coin`, `--interval`, `--model`, `--session` در صورت وجود، `--ohlc-dir`, `--jsonl-out`, `--min-sample-count 2`) در هر دو job `witness` و `verbose`. دانلود trades، رمزگشایی AES-256-CBC/scrypt، تزریق متادیتا، حذف JSONLهای خالی، انتقال CSV pattern — کلمه‌به‌کلمه از `analysis_fixed_batch.yml` کپی شده در مرحله‌ی `prepare`/`witness`/`verbose`. هیچ «بهبود»/«اصلاح باگ» انجام نشده؛ نکته‌ی `time.time()` که کاربر اشاره کرد بدون تغییر باقی مانده (فقط برای لاگِ سرعت پیشرفت استفاده می‌شود، همان‌طور که در مرجع بود). |
| 3 | بدون نوشتن در مخزن‌ها | هیچ `git commit`/`git push` در ورکفلوی جدید نیست. `all_combinations.json`, `completed_*.json`, `batch_outputs/`, `analysis_errors/` هرگز لمس نمی‌شوند. `loop_analysis.yml` هرگز فراخوانی نمی‌شود. مخزن سوم فقط با `GET`/`curl -s` (بدون `-X PUT`/`-X POST`) خوانده می‌شود. `concurrency.group: repro-verbose-runner` جدا از `analysis-runner`. |
| 4 | چک‌اوت با ref قفل‌شده + ورودی‌های اختیاری | `modules/combo_10day.py` و `data/news` همیشه از `ref_repo` (چک‌اوت‌شده روی input `main_ref`, پیش‌فرض commit مرجع) خوانده می‌شوند. ورودی‌های `main_ref`/`ohlc_ref`/`third_repo_ref` در `workflow_dispatch.inputs` تعریف شده‌اند و مقدار واقعی + commit sha حل‌شده در گروه‌های لاگ `[VERBOSE][منشأ]` چاپ می‌شوند (step های «حل و لاگ …»). |
| 5 | خواندن صف با همان ترتیب + تقسیم چانک | `jq ".[$START:$END]"` روی آرایه‌ی اصلیِ `rerun_queue_290.json` بدون هیچ مرتب‌سازی مجدد — ترتیب دقیقاً حفظ می‌شود. تقسیم به ۱۰ چانک (هر چانک = یک مجموعه‌کوین، ۲۹ آیتم) در step «تقسیم صف». |
| 6 | بدون secret در لاگ | هیچ `echo $GH_TOKEN`/`echo $RESULTS_PASSWORD` در هیچ کجای ورکفلو نیست؛ فقط از این متغیرها در `-H "Authorization: ..."`/`crypto.scryptSync` استفاده می‌شود، هیچ‌جا چاپ نمی‌شوند. |

## اعتبارسنجی «منطق تغییر نکرده»

| # | قاعده | کجا رعایت شده |
|---|---|---|
| 1 | چاپ sha256 و مقایسه با هش‌های مرجع، بدون توقف اجرا | `repro_tools/hash_check.py`، فراخوانی‌شده در `prepare` step «بررسی SHA-256 …». همیشه `exit 0` (مگر فایل اصلاً نباشد)؛ هشدار بلند در صورت عدم تطابق. |
| 2 | verbose فقط چاپ/لاگ اضافه کرده؛ دیف + بررسی خودکار (AST) | `repro_docs/combo_10day_original_vs_verbose.diff` (unified diff کامل) + `repro_tools/ast_diff_check.py` که در `prepare` و در هر چانکِ `verbose` اجرا می‌شود و به‌صورت خودکار AST را بعد از حذف بلوک‌های `_VERBOSE_*` مقایسه می‌کند (اجرا و PASS شدن آن تایید شد). |
| 3 | job شاهد + مقایسه بایت‌به‌بایت | job جداگانه‌ی `witness` (ماژول اصلیِ دست‌نخورده، همان ۲۹۰ ترکیب) + `repro_tools/compare_outputs.py` در job `report` که CSV/JSONL هر دو را sha256 می‌کند و در `diff_report.json` ثبت می‌کند. |
| 4 | وابستگی به زمان جاری | تنها استفاده‌ی `time.time()` در `combo_10day.py` (خط لاگِ سرعتِ پیشرفت داخل حلقه‌ی ترکیب‌ها) بدون تغییر باقی مانده و در verbose هم دست‌نخورده کپی شده؛ جای دیگری وابسته به زمان جاری در ماژول یافت نشد. |

## آنچه باید لاگ شود

| بند | قاعده | کجا رعایت شده |
|---|---|---|
| الف | منشأ هر ورودی (مخزن/مسیر/ref/commit sha/blob sha/URL/HTTP/حجم/sha256/زمان) | مراحل «حل main_ref»، «حل third_repo_ref»، «دانلود trades.enc»، «دانلود OHLC» در `prepare` — همه‌ی این فیلدها را در گروه‌های `[VERBOSE][منشأ]` چاپ می‌کنند. |
| ب | معاملات خام کامل قبل/بعد تزریق، یک‌بار برای کل اجرا، با توضیح صریح سقف لاگ | مراحل «نکته درباره‌ی trades خام …» و «تزریق متادیتا …» در `prepare` — تعداد رکورد/حجم/sha256 + پیام صریح که محتوای کامل فقط در artifact فشرده است؛ فایل کامل با sha256 در `trades-injected-<run_id>` (`trades_artifact.tar.gz`). |
| پ | متادیتای استراتژی کامل + منبع + move_percents/stop_loss_initial/max_move_percent | مرحله‌ی «تعیین متادیتا» در `prepare` — محتوای کامل `.js`/`1.json` چاپ می‌شود، منبع نهایی و سه مقدار صریحاً لاگ می‌شوند. |
| ت | محتوای کامل data/news به ترتیب sorted + نام/حجم/sha256/blob sha + sha256 کل پوشه؛ هر ترکیب حداقل تعداد+هش رویدادهای parse‌شده؛ اولین ترکیب هر job کامل | مرحله‌ی «دامپ کامل محتوای data/news» در `prepare` (سطح فایل) + بلوک `_VERBOSE_` داخل `combo_10day_verbose.py` بعد از `load_news_from_directory` (سطح رویداد parse‌شده، همیشه‌فعال) + کنترل env `VERBOSE_FULL_NEWS_DUMP=1` برای `i==0` هر چانک در job `verbose`. |
| ث | OHLC بدون محتوا؛ commit/تاریخ/فهرست/حجم/sha256/تعداد فایل و روز کاری هر کوین در برابر مبنا؛ CSV سری رژیم بازار برای ۵ کوین با sha256 | مرحله‌ی «دانلود یک‌باره OHLC» در `prepare` (فهرست فایل + مقایسه‌ی تعداد کل با ۱۳۱۴) + بلوک `_VERBOSE_` بعد از `_build_ohlc_coin_index` در ماژول verbose (روز کاری هر کوین) + `_VERBOSE_export_regime_debug_csv` (فعال‌شده فقط برای اولین ترکیب کل اجرا، آپلود در artifact chunk 0). |
| ج | آرگومان‌ها، فیلتر سشن، تعداد/بازه معاملات، اتصال به اخبار، توزیع رژیم، آستانه‌ها/min-sample-count، آمار و الگوها، هر رکورد خروجی کامل؛ در صورت سرریز به artifact با نام/تعداد/sha256 | همه‌ی این‌ها در بلوک‌های `_VERBOSE_` داخل `combo_10day_verbose.py` (دامپ args، شمارش فیلتر کوین/سشن، توزیع رژیم هر دوره، شمارش `--min-sample-count`، چاپ کامل هر رکورد JSONL) — لاگ کامل هر ترکیب در فایل جدا (`vlogs/verbose_<chunk>_<i>.log`) و خط خلاصه (exit/lines/sha256) در لاگ اصلی GitHub Actions طبق طرح در README بند ۵. |
| چ | manifest کامل خروجی‌ها + خلاصه‌ی وضعیت/زمان هر ترکیب | `repro_tools/build_manifest.py` در پایان هر چانک (هر دو job) + `repro_tools/compare_outputs.py` → `diff_report.json` در job `report`. |

## آرتیفکت‌ها و محدودیت لاگ

- همه‌ی artifactها با `retention-days: 90` (بیشترین مقدار قابل‌تضمین بدون
  دانستن سقف سازمان کاربر — قابل افزایش در فایل ورکفلو، توضیح در README).
- تصمیم‌های مربوط به سقف لاگ GitHub Actions (چاپ کامل در برابر ارسال به
  artifact) و دلیل هرکدام در بخش «تصمیم‌های طراحی» در README مستند شده‌اند
  (موارد ۴ و ۵).
- هیچ خطایی به‌صورت خاموش رد نشده: هر `curl`/`node`/`git clone` در صورت
  شکست `exit 1` می‌کند با پیام صریح؛ تنها استثنا خودِ «hash mismatch» است
  که طبق درخواست صریح کاربر (بند اعتبارسنجی #۱) هشدار می‌دهد ولی متوقف
  نمی‌شود.
