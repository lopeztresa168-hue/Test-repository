#!/usr/bin/env python3
"""
pool_search.py - build the best portfolio out of a large pool of strategy files (e.g. 150 strategies x 6 tf = 900 files)

CANDIDATE = (file, coin, direction, session).  Each file is split into candidates:
    coins in the file x BUY/SELL (as found in trades) x sessions {all, asia, london, ny, overlap}
A candidate exists only if it has at least --min-trades trades (default 5).

STEP 1  select the portfolio on the WHOLE period, with NO market filter applied.
        forward selection (add the best candidate until gain < --min-gain), then refinement passes
        (remove / replace members until nothing improves).  Objective: monthly Sortino (or Sharpe).
        Constraints: one candidate per (file, coin, direction); at most --max-per-file candidates per file.
STEP 2  take the final portfolio and apply each market filter to ALL its trades, report each result and name the best:
            none  - no filter
            both  - drop trade if BTC 30d return > +30%  OR  coin 30d return > +25%   (previous-day close, no look-ahead)
            btc   - drop trade only if BTC 30d return > +30%
            coin  - drop trade only if the coin's 30d return > +25%
        (a trade whose price data is missing is kept)

Data rules: AES-256-CBC (key = scrypt(password,'salt',32), IV = first 16 bytes), duplicate rows per (symbol, entryTime) dropped,
session by ENTRY hour in UTC (start <= h < end), pnl = profitPercent + FEE (default 0 = no fee; positive adds, negative subtracts), monthly sum by EXIT month.

MODES (--mode)
  all     (default) everything in one process, exactly as before. Local use / small pools.
            python pool_search.py --password PASS --data POOL_DIR --regime market_regime.json --fee 0 --out pool_results
  plan    INCREMENTAL / QUEUED pipeline, step 1: look at a git checkout of the data repo (git ls-tree only, blobs are not
          needed), compare every source file (aggregated/<STRAT>/<STRAT>_trades.enc, by git blob sha) with the per-file
          caches in <repo>/pool_candidates/, and write the work queue:  chunk_<i>.json + plan.json  into --queue-dir.
          A cache is up to date when its NAME matches  <strategy>__<source-sha12>__<params-key>.npz.enc ; so a changed /
          new source file, or a change of fee / min-trades / start / end / regime file, makes it pending again.
            python pool_search.py --mode plan --repo-dir REPO --regime market_regime.json --queue-dir Q --chunk-size 10
  build   step 2 (run in parallel, one process per chunk): build the candidate cache of the files listed in one chunk json.
            python pool_search.py --mode build --password PASS --data ROOT --manifest Q/chunk_3.json --cache-dir OUT
          ROOT/<item.path> must exist (item.path = aggregated/<STRAT>/<STRAT>_trades.enc). Files that fail are NOT cached
          (so they are retried by the next plan); they are listed in OUT/_failed_<manifest>.json.
  search  step 3: when all caches exist, load them and run STEP 1 + STEP 2 above (same results as --mode all).
            python pool_search.py --mode search --password PASS --repo-dir REPO --regime market_regime.json --out pool_results
          Up to --max-missing-pct % of the files may be missing (failed files); more than that exits with code 3.

  polish  (flag, with --mode search or all) improve the portfolio that was already found instead of searching from scratch:
            python pool_search.py --mode search --polish --objective sortino --tune-weights --password PASS --repo-dir REPO --regime market_regime.json --out pool_polish
          starts from selected.csv of the LAST polish result (analysis_results/pool_search_polished.tar.gz.enc in REPO), or - if there is none yet -
          of the main search result, or --polish-from FILE; then add / remove / replace passes with --objective (+ --time-budget restarts);
          the result is never worse than the start under that objective. Writes polish_changes.csv next to the usual files.

POOL_DIR: any folder tree; every *.enc found recursively is one strategy file (name = parent folder, else file name).
Options: --max-members 25 --min-gain 0.01 --min-trades 5 --max-per-file 3 --objective green|sortino|sharpe --max-month-share 0.25
         --tune-weights --jobs 8 --start 2018-01 --end 2026-05 --rebuild
"""
import argparse, glob, hashlib, io, json, os, pickle, re, subprocess, sys, tarfile, time
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
import numpy as np
import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

SESSIONS = {'all': (0, 24), 'asia': (0, 9), 'london': (8, 17), 'ny': (13, 22), 'overlap': (13, 17)}
FILTERS = ['none', 'both', 'btc', 'coin']
CACHE_VERSION = 1                      # bump when the candidate-building logic changes -> all caches become stale
CACHE_DIR_IN_REPO = 'pool_candidates'
SOURCE_DIR_IN_REPO = 'aggregated'


# ---------------------------------------------------------------- crypto / loading
@lru_cache(maxsize=4)
def _key(password):
    return hashlib.scrypt(password.encode(), salt=b'salt', n=16384, r=8, p=1, dklen=32, maxmem=64 * 1024 * 1024)


def _dec_bytes(d, password):
    c = Cipher(algorithms.AES(_key(password)), modes.CBC(d[:16])).decryptor()
    p = c.update(d[16:]) + c.finalize()
    return p[:-p[-1]]


def _enc_bytes(data, password):
    iv = os.urandom(16)
    pad = 16 - len(data) % 16
    c = Cipher(algorithms.AES(_key(password)), modes.CBC(iv)).encryptor()
    return iv + c.update(data + bytes([pad]) * pad) + c.finalize()


def _dec(path, password):
    with open(path, 'rb') as f:
        return json.loads(_dec_bytes(f.read(), password))


def _name(path):
    parent = os.path.basename(os.path.dirname(path))
    base = re.sub(r'_trades\.enc$|\.enc$', '', os.path.basename(path))
    return parent if parent and parent != '.' else base


def _safe(name):
    return re.sub(r'[^a-zA-Z0-9._-]', '_', name)


def load_returns(regime_path):
    r = pd.DataFrame(json.load(open(regime_path))['regimes'])
    r['d'] = pd.to_datetime(r.as_of_date)
    px = {c.replace('USDT', ''): g.set_index('d').close for c, g in r.groupby('coin')}
    return {c: s / s.shift(30) - 1 for c, s in px.items()}


def _one_file_raw(args):
    """-> list of (key, {filter: monthly array float32}, n_trades_unfiltered).  Raises on any problem."""
    path, password, fee, ret, month_index, min_trades = args
    j = _dec(path, password)
    t = pd.DataFrame(j['trades'] if isinstance(j, dict) else j)
    if t.empty:
        return []
    t = t.drop_duplicates(['symbol', 'entryTime'], keep='first')
    t['coin'] = t.symbol.str.replace('-1m', '', regex=False).str.replace('USDT', '', regex=False)
    et = pd.to_datetime(t.entryTime, utc=True)
    xm = pd.to_datetime(t.exitTime, utc=True).dt.tz_localize(None).dt.to_period('M')
    day = et.dt.tz_localize(None).dt.normalize() - pd.Timedelta(days=1)
    btc = ret['BTC'].reindex(day).values
    cr = np.full(len(t), np.nan)
    for c in t.coin.unique():
        if c in ret:
            m = (t.coin == c).values
            cr[m] = ret[c].reindex(day[m]).values
    t['pnl'] = t.profitPercent.values + fee
    t['h'] = et.dt.hour.values
    t['xm'] = xm.values
    t['keep_none'] = True
    t['keep_both'] = ~((btc > .30) | (cr > .25))      # NaN comparisons are False -> trade kept
    t['keep_btc'] = ~(btc > .30)
    t['keep_coin'] = ~(cr > .25)
    out = []
    name = _name(path)
    for (coin, typ), g in t.groupby(['coin', 'type']):
        for sname, (a, b) in SESSIONS.items():
            s = g[(g.h >= a) & (g.h < b)]
            if len(s) < min_trades:
                continue
            series = {}
            for f in FILTERS:
                sf = s[s['keep_' + f]]
                series[f] = sf.groupby('xm').pnl.sum().reindex(month_index, fill_value=0.0).values.astype(np.float32)
            out.append(((name, str(coin), str(typ), sname), series, len(s)))
    return out


def _one_file(args):
    try:
        return _one_file_raw(args)
    except Exception as e:
        print(f'  ! {args[0]}: {e}', file=sys.stderr)
        return []


def build_candidates(a, month_index):
    ret = load_returns(a.regime)
    files = sorted(glob.glob(os.path.join(a.data, '**', '*.enc'), recursive=True))
    print(f'{len(files)} files found, building candidates with {a.jobs} jobs ...')
    jobs = [(f, a.password, a.fee, ret, month_index, a.min_trades) for f in files]
    keys, ns = [], []
    mats = {f: [] for f in FILTERS}
    with ProcessPoolExecutor(a.jobs) as ex:
        for k, res in enumerate(ex.map(_one_file, jobs, chunksize=4), 1):
            for key, series, n in res:
                keys.append(key); ns.append(n)
                for f in FILTERS:
                    mats[f].append(series[f])
            if k % 50 == 0:
                print(f'  {k}/{len(files)} files, {len(keys)} candidates')
    return keys, {f: np.vstack(v) for f, v in mats.items()}, np.array(ns)


# ---------------------------------------------------------------- objective & search
CAP = 0.25                              # green objective: best month should be <= CAP of the total profit (soft penalty)


def score_rows(P, objective):
    """P: (n, T) portfolio monthly series -> (n,) scores"""
    mean = P.mean(1)
    if objective == 'sharpe':
        sd = P.std(1)
        return np.where(sd > 0, mean / np.where(sd > 0, sd, 1), -9)
    dd = np.sqrt((np.minimum(P, 0) ** 2).mean(1))
    sortino = np.where(dd > 0, mean / np.where(dd > 0, dd, 1), np.where(mean > 0, 50, -9))
    if objective == 'sortino':
        return sortino
    # 'green': (profitable months - losing months), months with no trades (0) are neutral.
    #   - soft penalty when the best month carries more than CAP of the total profit (spread of profit)
    #   - total <= 0 gets the maximum penalty
    #   - Sortino only breaks ties (tanh-squashed, < 0.5)
    T = P.shape[1]
    pos = (P > 0).sum(1)
    neg = (P < 0).sum(1)
    tot = P.sum(1)
    share = np.where(tot > 0, P.max(1) / np.where(tot > 0, tot, 1), 1.0)
    pen = np.where(tot > 0, np.maximum(0.0, share - CAP), 1.0) * T
    tie = 0.4 * np.tanh(np.clip(sortino, -9, 50) / 10.0)
    return (pos - neg) + 2 * T - pen + tie


def search(C, keys, a, init=None, init_w=None, X=None, kick=0, rng=None, rand_top=30):
    """init: indices of an existing portfolio to continue from (polish); init_w: {index: weight} of those members.
    Members keep their weight, newly added / replacing candidates get weight 1.0. The result is never worse than init."""
    X = C.astype(np.float64) if X is None else X
    n = len(keys)
    fk = np.array([hash(k[:3]) for k in keys])          # (file, coin, direction)
    fl = np.array([hash(k[0]) for k in keys])           # file
    wt = {i: float((init_w or {}).get(i, 1.0)) for i in (init or [])}
    sel = list(init or [])
    cur = sum((X[i] * wt[i] for i in sel), np.zeros(X.shape[1]))
    best = score_rows(cur[None, :], a.objective)[0] if sel else -9

    def allowed(excl=None):
        ok = np.ones(n, bool)
        chosen = [i for i in sel if i != excl]
        if chosen:
            ok &= ~np.isin(fk, fk[chosen])
            cnt = pd.Series(fl[chosen]).value_counts()
            full = cnt[cnt >= a.max_per_file].index.values
            ok &= ~np.isin(fl, full)
        return ok

    def scores(base, ok):
        sc = np.full(n, -9.0)
        idx = np.where(ok)[0]
        for s in range(0, len(idx), 4000):
            ch = idx[s:s + 4000]
            sc[ch] = score_rows(base[None, :] + X[ch], a.objective)
        return sc

    step = 0
    while len(sel) < a.max_members:                       # forward selection
        sc = scores(cur, allowed())
        if kick and step < kick and rng is not None:      # randomized start (multi-start): pick among the top candidates
            top = np.argsort(sc)[-rand_top:]
            top = top[sc[top] > -9]
            if len(top) == 0:
                break
            j = int(rng.choice(top))
        else:
            j = int(np.argmax(sc))
            if sc[j] < best + a.min_gain:
                break
        step += 1
        sel.append(j); wt[j] = 1.0; cur = cur + X[j]; best = sc[j]
    improved = True                                       # refinement: remove / replace
    while improved:
        improved = False
        for pos in range(len(sel)):
            i = sel[pos]
            base = cur - X[i] * wt[i]
            sc = scores(base, allowed(excl=i))
            j = int(np.argmax(sc))
            rem = score_rows(base[None, :], a.objective)[0] if len(sel) > 1 else -9
            if rem >= best + 1e-9 and rem >= sc[j]:        # removing is better
                sel.pop(pos); wt.pop(i, None); cur = base; best = rem; improved = True; break
            if j != i and sc[j] > best + 1e-6:             # replacing is better
                wt.pop(i, None); wt[j] = 1.0; sel[pos] = j; cur = base + X[j]; best = sc[j]; improved = True; break
    return sel, wt


def search_multi(C, keys, a, init=None, init_w=None):
    """search() once (deterministic local optimum), then - while --time-budget minutes remain - iterated local search:
    drop a few random members, force 1-3 random picks from the top candidates, re-run forward selection + refinement,
    keep the result only if it beats the best so far. Never worse than the plain search()."""
    X = C.astype(np.float64)
    sel, wt = search(C, keys, a, init, init_w, X=X)
    if a.time_budget <= 0:
        return sel, wt
    rng = np.random.default_rng(a.seed)

    def val(s, w):
        return float(score_rows(sum(X[i] * w[i] for i in s)[None, :], a.objective)[0])

    best_sel, best_w, best_s = list(sel), dict(wt), val(sel, wt)
    t_end = time.time() + a.time_budget * 60
    print(f'multi-start: first local optimum {best_s:.4f} ({len(best_sel)} members), searching {a.time_budget} min for better ...', flush=True)
    it = fails = 0
    while time.time() < t_end:
        it += 1
        kmax = max(1, min(len(best_sel) - 1, 3 + fails // 5))        # perturbation grows when stuck
        k = int(rng.integers(1, kmax + 1)) if len(best_sel) > 1 else 0
        order = [int(i) for i in rng.permutation(best_sel)]
        keep = order[k:]
        s2, w2 = search(C, keys, a, init=keep or None, init_w={i: best_w[i] for i in keep}, X=X,
                        kick=int(rng.integers(1, 4)), rng=rng, rand_top=int(rng.choice([10, 30, 100])))
        v = val(s2, w2)
        if v > best_s + 1e-9:
            best_sel, best_w, best_s, fails = s2, w2, v, 0
            print(f'  [{it}] improved -> {v:.4f} ({len(s2)} members, {int(t_end - time.time())}s left)', flush=True)
        else:
            fails += 1
    print(f'multi-start done: {it} restarts, best {best_s:.4f}', flush=True)
    return best_sel, best_w


def tune_weights(C, sel, a, grid=(0.5, 1.0, 1.5), w0=None):
    w = {i: float((w0 or {}).get(i, 1.0)) for i in sel}
    X = C.astype(np.float64)
    cur = sum(X[i] * w[i] for i in sel)
    best = score_rows(cur[None, :], a.objective)[0]
    imp = True
    while imp:
        imp = False
        for i in sel:
            for g in grid:
                if g == w[i]:
                    continue
                c2 = cur + X[i] * (g - w[i])
                s = score_rows(c2[None, :], a.objective)[0]
                if s > best + 1e-6:
                    best, cur, w[i], imp = s, c2, g, True
    return w


def describe(p):
    c = p.cumsum()
    return dict(months=len(p), win_pct=round(float((p > 0).mean() * 100), 1), losing_months=int((p <= 0).sum()),
                total=round(float(p.sum()), 1), mean=round(float(p.mean()), 2), median=round(float(np.median(p)), 2),
                worst=round(float(p.min()), 2), best=round(float(p.max()), 1),
                max_dd=round(float((c - np.maximum.accumulate(c)).min()), 1),
                sortino=round(float(score_rows(p[None, :], 'sortino')[0]), 2))


def report(keys, CF, ns, a, idx, sel=None, w=None):
    """STEP 1 (selection, no filter) + STEP 2 (filters on the final portfolio) + write csv files.
    sel / w given -> the portfolio is already decided (polish mode), only the report is produced."""
    if sel is None:
        sel, _ = search_multi(CF['none'], keys, a)
        w = tune_weights(CF['none'], sel, a) if a.tune_weights else {i: 1.0 for i in sel}
    rows = []
    for i in sel:
        s = CF['none'][i].astype(np.float64)
        rows.append(dict(file=keys[i][0], coin=keys[i][1], direction=keys[i][2], session=keys[i][3], weight=w[i],
                         trades=int(ns[i]), alone_total=round(float(s.sum()), 1),
                         alone_win_pct=round(float((s > 0).mean() * 100), 1), alone_worst=round(float(s.min()), 2)))
    sd = pd.DataFrame(rows)
    sd.to_csv(os.path.join(a.out, 'selected.csv'), index=False)
    print(f'\n== selected portfolio ({len(sel)} members, whole period, no filter) ==')
    print(sd.to_string(index=False))

    print('\n== filters applied to the final portfolio ==')
    res, series = [], {}
    for f in FILTERS:
        P = sum(CF[f][i].astype(np.float64) * w[i] for i in sel)
        series[f] = P
        d = describe(P); d['filter'] = f
        d['score'] = round(float(score_rows(P[None, :], a.objective)[0]), 2)
        d['best_month_share'] = round(float(P.max() / P.sum()), 2) if P.sum() > 0 else None
        res.append(d)
    R = pd.DataFrame(res).set_index('filter')
    print(R.to_string())
    best_f = R.score.idxmax()
    R.to_csv(os.path.join(a.out, 'filters_compare.csv'))
    pd.DataFrame({f: series[f] for f in FILTERS}, index=idx.astype(str)).to_csv(os.path.join(a.out, 'portfolio_monthly_by_filter.csv'))
    print(f"\nBEST FILTER: {best_f}   (highest score; win% {R.loc[best_f, 'win_pct']}, total {R.loc[best_f, 'total']}, "
          f"worst month {R.loc[best_f, 'worst']})")
    print({'none': 'no filter', 'both': 'drop if BTC 30d > +30% or coin 30d > +25%', 'btc': 'drop if BTC 30d > +30%',
           'coin': 'drop if coin 30d > +25%'}[best_f])
    P = series[best_f]
    yy = pd.Series(P, index=idx).groupby(idx.year).agg(total='sum', win_pct=lambda x: (x > 0).mean() * 100).round(1)
    print(yy.T.to_string())
    print(f'\nfiles written to {a.out}/')


# ---------------------------------------------------------------- polish: improve an already found portfolio
SEED_IN_REPO = os.path.join('analysis_results', 'pool_search_results.tar.gz.enc')
POLISHED_IN_REPO = os.path.join('analysis_results', 'pool_search_polished.tar.gz.enc')   # written by every polish run


def load_seed(a):
    """selected.csv of the portfolio to continue from: --polish-from (a csv file), else the result of the previous polish run
    (analysis_results/pool_search_polished.tar.gz.enc), else the main search result (analysis_results/pool_search_results.tar.gz.enc).
    So every polish run continues where the last polish run stopped."""
    if a.polish_from:
        return pd.read_csv(a.polish_from)
    p = os.path.join(a.repo_dir, SEED_IN_REPO)
    q = os.path.join(a.repo_dir, POLISHED_IN_REPO)
    if os.path.isfile(q):
        p = q
        print('polish: continuing from the previous polish result (pool_search_polished.tar.gz.enc)', flush=True)
    elif os.path.isfile(p):
        print('polish: no previous polish result yet - starting from the main search result (pool_search_results.tar.gz.enc)', flush=True)
    if not os.path.isfile(p):
        sys.exit(f'polish: no saved portfolio found ({p}); pass --polish-from selected.csv or run the search first')
    with open(p, 'rb') as f:
        raw = _dec_bytes(f.read(), a.password)
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as tf:
        m = next((x for x in tf.getmembers() if os.path.basename(x.name) == 'selected.csv'), None)
        if m is None:
            sys.exit('polish: selected.csv is not inside the saved results archive')
        return pd.read_csv(io.BytesIO(tf.extractfile(m).read()))


def polish_mode(a, keys, CF, ns, idx):
    seed = load_seed(a)
    pos = {k: i for i, k in enumerate(keys)}
    init, w0, lost = [], {}, []
    for r in seed.itertuples(index=False):
        k = (str(r.file), str(r.coin), str(r.direction), str(r.session))
        if k in pos and pos[k] not in init:
            init.append(pos[k]); w0[pos[k]] = float(getattr(r, 'weight', 1.0))
        else:
            lost.append(k)
    print(f'polish: saved portfolio has {len(seed)} members, {len(init)} found among the candidates')
    if lost:
        print(f'  ! {len(lost)} members no longer exist as candidates (data / params changed) and are dropped: ' + '; '.join('/'.join(k) for k in lost[:10]))
    if not init:
        sys.exit('polish: none of the saved members exist among the candidates')
    X = CF['none'].astype(np.float64)
    before = sum(X[i] * w0[i] for i in init)
    s0 = float(score_rows(before[None, :], a.objective)[0])
    sel, w = search_multi(CF['none'], keys, a, init=init, init_w=w0)
    if a.tune_weights:
        w = tune_weights(CF['none'], sel, a, w0=w)
    after = sum(X[i] * w[i] for i in sel)
    s1 = float(score_rows(after[None, :], a.objective)[0])
    print(f'\n== polish ({a.objective}): score {s0:.3f} -> {s1:.3f}   ({len(init)} -> {len(sel)} members) ==')
    ch = []
    for i in init:
        if i not in sel:
            ch.append(dict(change='removed', file=keys[i][0], coin=keys[i][1], direction=keys[i][2], session=keys[i][3], weight=w0[i]))
        elif abs(w[i] - w0[i]) > 1e-9:
            ch.append(dict(change='weight', file=keys[i][0], coin=keys[i][1], direction=keys[i][2], session=keys[i][3], weight=w[i]))
    for i in sel:
        if i not in init:
            ch.append(dict(change='added', file=keys[i][0], coin=keys[i][1], direction=keys[i][2], session=keys[i][3], weight=w[i]))
    os.makedirs(a.out, exist_ok=True)
    cd = pd.DataFrame(ch, columns=['change', 'file', 'coin', 'direction', 'session', 'weight'])
    cd.to_csv(os.path.join(a.out, 'polish_changes.csv'), index=False)
    print(cd.to_string(index=False) if len(cd) else 'no change: the saved portfolio is already a local optimum for this objective')
    report(keys, CF, ns, a, idx, sel=sel, w=w)


# ---------------------------------------------------------------- incremental pipeline: plan
def params_key(a):
    """Changes whenever a cached candidate would come out different (fee / min-trades / period / regime data / logic)."""
    with open(a.regime, 'rb') as f:
        rh = hashlib.sha256(f.read()).hexdigest()
    s = f'v{CACHE_VERSION}|fee={a.fee:.8f}|min={a.min_trades}|{a.start}|{a.end}|{rh}'
    return hashlib.sha256(s.encode()).hexdigest()[:8]


def _git_ls(repo, sub):
    """[(blob_sha, path)] of every file under <sub>/ at HEAD. Needs only tree objects (works on --filter=blob:none clones)."""
    out = subprocess.run(['git', '-C', repo, 'ls-tree', '-r', '-z', 'HEAD', '--', sub],
                         capture_output=True, check=True).stdout.decode('utf-8', 'surrogateescape')
    rows = []
    for rec in out.split('\0'):
        if not rec:
            continue
        meta, path = rec.split('\t', 1)
        rows.append((meta.split()[2], path))
    return rows


def make_plan(a):
    pk = params_key(a)
    sources, seen = [], set()
    for sha, path in sorted(_git_ls(a.repo_dir, SOURCE_DIR_IN_REPO), key=lambda r: r[1]):
        if not path.endswith('.enc'):
            continue
        name = _name(path)
        if name in seen:                                   # two .enc in one folder -> keep names unique
            name = f'{name}~{re.sub(r"[.]enc$", "", os.path.basename(path))}'
        seen.add(name)
        sources.append(dict(name=name, path=path, sha=sha, cache=f'{_safe(name)}__{sha[:12]}__{pk}.npz.enc'))
    present = {os.path.basename(p): p for _, p in _git_ls(a.repo_dir, CACHE_DIR_IN_REPO)}
    expected = {s['cache'] for s in sources}
    pending = [s for s in sources if s['cache'] not in present]
    stale = sorted(p for b, p in present.items() if b not in expected and b.endswith('.npz.enc'))
    return dict(params_key=pk, sources=sources, pending=pending, stale=stale, cached=len(sources) - len(pending))


def plan_mode(a):
    pl = make_plan(a)
    os.makedirs(a.queue_dir, exist_ok=True)
    run = pl['pending'][:a.max_files]
    n_chunks = (len(run) + a.chunk_size - 1) // a.chunk_size
    for i in range(n_chunks):
        json.dump(run[i * a.chunk_size:(i + 1) * a.chunk_size], open(os.path.join(a.queue_dir, f'chunk_{i}.json'), 'w'))
    summary = dict(params_key=pl['params_key'], sources=len(pl['sources']), cached=pl['cached'],
                   pending_total=len(pl['pending']), run_files=len(run), chunk_count=n_chunks,
                   pending_names=[s['name'] for s in pl['pending']], stale=pl['stale'])
    json.dump(summary, open(os.path.join(a.queue_dir, 'plan.json'), 'w'))
    print(f"plan: params_key={pl['params_key']}  sources={len(pl['sources'])}  cached={pl['cached']}  "
          f"pending={len(pl['pending'])}  this run={len(run)} files in {n_chunks} chunks  stale caches={len(pl['stale'])}")


# ---------------------------------------------------------------- incremental pipeline: build
def _build_one(args):
    item, local, password, fee, ret, month_index, min_trades, pk, cache_dir = args
    try:
        res = _one_file_raw((local, password, fee, ret, month_index, min_trades))
    except Exception as e:
        print(f"  ! {item['name']}: {e}", file=sys.stderr)
        return item['name'], f'error: {e}', 0
    T = len(month_index)
    arrays = {f: (np.vstack([r[1][f] for r in res]) if res else np.zeros((0, T), np.float32)) for f in FILTERS}
    meta = dict(name=item['name'], src_sha=item['sha'], params_key=pk, months=T, candidates=len(res), version=CACHE_VERSION)
    buf = io.BytesIO()
    np.savez_compressed(buf, keys=np.array([json.dumps(list(r[0])) for r in res], dtype='U'),
                        ns=np.array([r[2] for r in res], dtype=np.int64), meta=np.array(json.dumps(meta)), **arrays)
    with open(os.path.join(cache_dir, item['cache']), 'wb') as f:
        f.write(_enc_bytes(buf.getvalue(), password))
    return item['name'], 'ok', len(res)


def build_mode(a, idx):
    manifest = json.load(open(a.manifest))
    pk = params_key(a)
    os.makedirs(a.cache_dir, exist_ok=True)
    ret = load_returns(a.regime)
    jobs, failed = [], []
    for it in manifest:
        if it['cache'].split('__')[-1] != f'{pk}.npz.enc':
            sys.exit(f"manifest item {it['name']} was planned with a different params key than the current arguments")
        local = os.path.join(a.data, it['path'])
        if os.path.isfile(local):
            jobs.append((it, local, a.password, a.fee, ret, idx, a.min_trades, pk, a.cache_dir))
        else:
            print(f"  ! {it['name']}: source file missing ({local})", file=sys.stderr)
            failed.append(it['name'])
    with ProcessPoolExecutor(a.jobs) as ex:
        for name, status, n in ex.map(_build_one, jobs):
            print(f'  {name}: {status} ({n} candidates)')
            if status != 'ok':
                failed.append(name)
    tag = os.path.splitext(os.path.basename(a.manifest))[0]
    json.dump(sorted(failed), open(os.path.join(a.cache_dir, f'_failed_{tag}.json'), 'w'))
    print(f'build: {len(manifest) - len(failed)} ok, {len(failed)} failed')


# ---------------------------------------------------------------- incremental pipeline: search
def search_mode(a, idx):
    pl = make_plan(a)
    keys, ns, mats, missing = [], [], {f: [] for f in FILTERS}, []
    for s in pl['sources']:
        p = os.path.join(a.repo_dir, CACHE_DIR_IN_REPO, s['cache'])
        if not os.path.isfile(p):
            missing.append(s['name'])
            continue
        with open(p, 'rb') as f:
            z = np.load(io.BytesIO(_dec_bytes(f.read(), a.password)), allow_pickle=False)
        if z['none'].shape[1] != len(idx):
            missing.append(s['name'])
            continue
        keys += [tuple(json.loads(k)) for k in z['keys']]
        ns += [int(x) for x in z['ns']]
        for fl in FILTERS:
            mats[fl].append(z[fl])
    n_src = len(pl['sources'])
    pct = 100.0 * len(missing) / max(1, n_src)
    print(f'caches loaded: {n_src - len(missing)}/{n_src} source files, {len(keys)} candidates, {len(idx)} months')
    if missing:
        print(f'WARNING: {len(missing)} files have no cache ({pct:.1f}%): ' + ', '.join(missing[:50]) + (' ...' if len(missing) > 50 else ''))
    os.makedirs(a.out, exist_ok=True)
    json.dump(dict(sources=n_src, used=n_src - len(missing), missing=missing, candidates=len(keys), params_key=pl['params_key']),
              open(os.path.join(a.out, 'coverage.json'), 'w'), indent=1)
    if pct > a.max_missing_pct:
        print(f'ERROR: more than {a.max_missing_pct}% of the files are missing - refusing to build a portfolio', file=sys.stderr)
        sys.exit(3)
    if not keys:
        print('ERROR: no candidates at all', file=sys.stderr)
        sys.exit(4)
    CF = {f: np.vstack(v) for f, v in mats.items()}
    if a.polish:
        return polish_mode(a, keys, CF, np.array(ns), idx)
    report(keys, CF, np.array(ns), a, idx)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['all', 'plan', 'build', 'search'], default='all')
    ap.add_argument('--password')
    ap.add_argument('--data', help='all: folder tree of *.enc ; build: root so that <data>/<item.path> exists')
    ap.add_argument('--regime')
    ap.add_argument('--fee', type=float, default=0.0)
    ap.add_argument('--out', default='pool_results')
    ap.add_argument('--start', default='2018-01')
    ap.add_argument('--end', default='2026-05')
    ap.add_argument('--jobs', type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument('--min-trades', type=int, default=5, help='min trades for a candidate to exist')
    ap.add_argument('--max-members', type=int, default=25)
    ap.add_argument('--max-per-file', type=int, default=3)
    ap.add_argument('--min-gain', type=float, default=0.01, help='stop adding when objective gain is below this')
    ap.add_argument('--objective', choices=['green', 'sortino', 'sharpe'], default='green',
                    help='green = profitable months - losing months, with a profit-spread penalty (see --max-month-share)')
    ap.add_argument('--max-month-share', type=float, default=0.25,
                    help='green objective: best month should not exceed this share of the total profit')
    ap.add_argument('--time-budget', type=float, default=0.0,
                    help='minutes of extra randomized restarts (iterated local search) after the first local optimum; 0 = off')
    ap.add_argument('--seed', type=int, default=0, help='random seed of the restarts')
    ap.add_argument('--tune-weights', action='store_true')
    ap.add_argument('--rebuild', action='store_true')
    ap.add_argument('--polish', action='store_true',
                    help='search/all: do not search from scratch; start from the portfolio already found (selected.csv) and keep improving it '
                         'with --objective (add / remove / replace, never worse than the start); with --tune-weights the weights are tuned from the saved ones')
    ap.add_argument('--polish-from', help='polish: selected.csv to start from (default: analysis_results/pool_search_polished.tar.gz.enc if present, else pool_search_results.tar.gz.enc, in --repo-dir; mode all needs this)')
    ap.add_argument('--repo-dir', help='plan/search: git checkout of the data repo (aggregated/ + pool_candidates/)')
    ap.add_argument('--queue-dir', default='pool_queue', help='plan: where chunk_<i>.json and plan.json are written')
    ap.add_argument('--chunk-size', type=int, default=10, help='plan: files per chunk')
    ap.add_argument('--max-files', type=int, default=1000, help='plan: max files queued in one run')
    ap.add_argument('--manifest', help='build: one chunk json written by plan')
    ap.add_argument('--cache-dir', default='pool_cache', help='build: where the per-file caches are written')
    ap.add_argument('--max-missing-pct', type=float, default=5.0, help='search: tolerated share of files without cache')
    a = ap.parse_args()
    global CAP
    CAP = a.max_month_share

    need = {'all': ['password', 'data', 'regime'], 'plan': ['repo_dir', 'regime'],
            'build': ['password', 'data', 'regime', 'manifest'], 'search': ['password', 'repo_dir', 'regime']}[a.mode]
    miss = [n for n in need if not getattr(a, n)]
    if a.polish and a.mode == 'all' and not a.polish_from:
        miss.append('polish_from')
    if a.polish and a.mode in ('plan', 'build'):
        ap.error('--polish works with --mode search or all')
    if miss:
        ap.error(f'--mode {a.mode} needs ' + ', '.join('--' + n.replace('_', '-') for n in miss))
    idx = pd.period_range(a.start, a.end, freq='M')

    if a.mode == 'plan':
        return plan_mode(a)
    if a.mode == 'build':
        return build_mode(a, idx)
    if a.mode == 'search':
        return search_mode(a, idx)

    os.makedirs(a.out, exist_ok=True)
    cache = os.path.join(a.out, f'candidates_fee{a.fee}_min{a.min_trades}.pkl')
    if os.path.exists(cache) and not a.rebuild:
        keys, CF, ns = pickle.load(open(cache, 'rb'))
        print(f'loaded {len(keys)} cached candidates ({cache})')
    else:
        keys, CF, ns = build_candidates(a, idx)
        pickle.dump((keys, CF, ns), open(cache, 'wb'))
    print(f'{len(keys)} candidates, {CF["none"].shape[1]} months')
    if a.polish:
        return polish_mode(a, keys, CF, ns, idx)
    report(keys, CF, ns, a, idx)


if __name__ == '__main__':
    main()
