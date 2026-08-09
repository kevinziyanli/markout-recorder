#!/usr/bin/env python3
"""Quiet Favorite Maker v2 — public-data recorder. [claude:markout]  (rev 2)

Rev 2 (2026-08-09): the v1-style full-universe sweep is not viable — Kalshi's
/markets endpoint returns as few as ~16 markets per page regardless of `limit`,
making a full sweep take 10+ minutes. Per K's "hone in on a specific market",
the sweep is now SERIES-SCOPED: discover all Climate & Weather series, filter to
daily temperature families (KXHIGH*/KXLOW*), and sweep exactly those markets.

Runs UNAUTHENTICATED on public endpoints only. Places no orders. Python 3.9+ stdlib.

    python3 scripts/record_markets.py            # foreground
Stop:   touch data/recordings/STOP   ·   or: pkill -f record_markets.py

- Every SWEEP_SECONDS: one /markets call per focus series (~1-2s total) →
  top-of-book row for every open weather market + metadata upsert.
- Every HOT_SECONDS: real /orderbook polls (with sizes) for markets passing the
  cheap pre-gate (52-88c favored mid, 2-10c spread). This is markout-grade data.
- SQLite WAL at data/recordings/market_data.sqlite
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.elections.kalshi.com/trade-api/v2"
OUT_DIR = os.environ.get("MARKOUT_OUT_DIR") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "recordings")
DB_PATH = os.path.join(OUT_DIR, "market_data.sqlite")
STOP_FILE = os.path.join(OUT_DIR, "STOP")
DURATION = 0  # seconds; 0 = run forever (set via --duration for CI shifts)

SWEEP_SECONDS = 60
HOT_SECONDS = 10
SERIES_TTL = 6 * 3600
CATEGORY = "Climate and Weather"

# Daily temperature families only. Excludes hourly-directional series (KXTEMP*),
# which the charter's A4 next-day rule bars anyway.
FOCUS_PREFIXES = ("KXHIGH", "KXLOW", "HIGHNY", "HIGHAUS", "MINNYC", "LOWNY")
FAV_MIN, FAV_MAX = 52.0, 88.0
SPREAD_MIN, SPREAD_MAX = 2.0, 10.0


def log(msg):
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


def get(path, params=None, retries=3):
    q = ""
    if params:
        from urllib.parse import urlencode
        q = "?" + urlencode(params)
    req = urllib.request.Request(BASE + path + q, headers={"User-Agent": "markout-recorder/2.1"})
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2.0 * (i + 1))
                continue
            raise
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.0 * (i + 1))
    raise RuntimeError("rate limited")


def paged(path, list_key, params=None, max_pages=50):
    params = dict(params or {})
    params.setdefault("limit", 200)
    for _ in range(max_pages):
        data = get(path, params)
        for x in data.get(list_key, []):
            yield x
        cur = data.get("cursor")
        if not cur:
            return
        params["cursor"] = cur


def cents(obj, key):
    """Price fields may be KEY (cents) or KEY_dollars. None-safe."""
    v = obj.get(key + "_dollars")
    if v is not None:
        try:
            return float(v) * 100.0
        except (TypeError, ValueError):
            return None
    v = obj.get(key)
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def acquire_lock():
    """One recorder at a time — overlapping writers are how the DB got corrupted once."""
    import fcntl
    os.makedirs(OUT_DIR, exist_ok=True)
    fh = open(os.path.join(OUT_DIR, "recorder.lock"), "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("another recorder instance holds the lock — exiting (this is a guard, not an error)")
        sys.exit(0)
    fh.write(str(os.getpid()))
    fh.flush()
    return fh  # keep the handle alive for the process lifetime


def db_connect(path=None):
    path = path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.DatabaseError:
        # corrupt main file (torn by overlapping/killed writers) — set it aside, start fresh
        stamp = int(time.time())
        for suffix in ("", "-wal", "-shm"):
            p = path + suffix
            if os.path.exists(p):
                os.replace(p, f"{path}.corrupt-{stamp}{suffix}")
        log(f"database was corrupt — rotated to {os.path.basename(path)}.corrupt-{stamp}, starting fresh")
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("""CREATE TABLE IF NOT EXISTS markets (
        ticker TEXT PRIMARY KEY, event_ticker TEXT, series TEXT, category TEXT,
        title TEXT, close_time TEXT, expected_settlement_time TEXT,
        fee_json TEXT, first_seen REAL, last_seen REAL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS book_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, wall REAL NOT NULL, ticker TEXT NOT NULL,
        yes_bid REAL, yes_ask REAL, yes_bid_size REAL, yes_ask_size REAL,
        volume_24h REAL, source TEXT)""")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_book ON book_events (ticker, wall)")
    return conn


def top_of_book(ob):
    """Best yes bid / yes ask (= 100 - best no bid) with sizes.

    Current API: {"orderbook_fp": {"yes_dollars": [["0.6300","1.00"], ...], "no_dollars": [...]}}
    Legacy:      {"orderbook": {"yes": [[63, 1], ...], "no": [...]}}  (prices in cents)
    """
    o = ob.get("orderbook_fp") or ob.get("orderbook") or ob
    def best(side):
        levels, scale = o.get(side + "_dollars"), 100.0
        if levels is None:
            levels, scale = o.get(side), 1.0
        best_px, best_sz = None, None
        for level in (levels or []):
            try:
                px, sz = float(level[0]) * scale, float(level[1])
            except (TypeError, ValueError, IndexError):
                continue
            if best_px is None or px > best_px:
                best_px, best_sz = px, sz
        return best_px, best_sz
    yb, ybs = best("yes")
    nb, nbs = best("no")
    ya = None if nb is None else 100.0 - nb
    return yb, ya, ybs, nbs


def is_focus_series(ticker):
    return any(ticker.startswith(p) for p in FOCUS_PREFIXES) and not ticker.startswith("KXTEMP")


def pregate(yb, ya):
    if yb is None or ya is None or ya <= yb:
        return False
    mid = (yb + ya) / 2
    fav = mid if mid >= 50 else 100 - mid
    return FAV_MIN <= fav <= FAV_MAX and SPREAD_MIN <= (ya - yb) <= SPREAD_MAX


class Recorder:
    def __init__(self):
        self.conn = db_connect()
        self.series = []
        self.series_at = 0.0
        self.hot = set()
        self.running = True

    def focus_series(self):
        if self.series and time.time() - self.series_at < SERIES_TTL:
            return self.series
        try:
            found = []
            for s in paged("/series", "series", {"category": CATEGORY}):
                t = s.get("ticker") or ""
                if is_focus_series(t):
                    found.append(t)
            if found:
                self.series = sorted(set(found))
                self.series_at = time.time()
                log(f"focus series refreshed: {len(self.series)} daily temperature series")
        except Exception as e:
            log(f"series discovery failed ({e}); using {len(self.series)} cached")
        return self.series

    def sweep(self):
        wall = time.time()
        new_hot, n = set(), 0
        for st in self.focus_series():
            if not self.running or os.path.exists(STOP_FILE):
                break
            try:
                markets = list(paged("/markets", "markets", {"series_ticker": st, "status": "open"}, max_pages=5))
            except Exception as e:
                log(f"sweep {st}: {e}")
                continue
            for m in markets:
                t = m.get("ticker")
                if not t:
                    continue
                yb, ya = cents(m, "yes_bid"), cents(m, "yes_ask")
                self.conn.execute(
                    """INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(ticker) DO UPDATE SET last_seen=excluded.last_seen,
                       close_time=excluded.close_time""",
                    (t, m.get("event_ticker"), st, CATEGORY, m.get("title"),
                     m.get("close_time"), m.get("expected_expiration_time") or m.get("expiration_time"),
                     json.dumps({k: m.get(k) for k in ("fee_waiver_expiration_time", "maker_fee", "fee_type") if k in m}) or None,
                     wall, wall))
                self.conn.execute(
                    "INSERT INTO book_events (wall,ticker,yes_bid,yes_ask,yes_bid_size,yes_ask_size,volume_24h,source) VALUES (?,?,?,?,?,?,?,?)",
                    (wall, t, yb, ya, None, None, m.get("volume_24h"), "sweep"))
                if pregate(yb, ya):
                    new_hot.add(t)
                n += 1
            time.sleep(0.1)
        self.conn.commit()
        self.hot = new_hot
        return n

    def poll_hot(self):
        wall = time.time()
        for t in list(self.hot):
            if not self.running:
                break
            try:
                yb, ya, ybs, nbs = top_of_book(get(f"/markets/{t}/orderbook", {"depth": 8}))
            except Exception as e:
                log(f"orderbook {t}: {e}")
                continue
            if yb is None and ya is None:
                continue  # nothing parseable — never store empty rows
            self.conn.execute(
                "INSERT INTO book_events (wall,ticker,yes_bid,yes_ask,yes_bid_size,yes_ask_size,volume_24h,source) VALUES (?,?,?,?,?,?,?,?)",
                (wall, t, yb, ya, ybs, nbs, None, "hot"))
            time.sleep(0.15)
        self.conn.commit()

    def run(self):
        signal.signal(signal.SIGINT, lambda *a: setattr(self, "running", False))
        signal.signal(signal.SIGTERM, lambda *a: setattr(self, "running", False))
        log(f"recorder v2.2 starting → {DB_PATH}")
        log(f"scope: '{CATEGORY}' series matching {FOCUS_PREFIXES} (hourly KXTEMP* excluded); sweep {SWEEP_SECONDS}s, hot {HOT_SECONDS}s")
        started = time.time()
        last_sweep = 0.0
        while self.running:
            if os.path.exists(STOP_FILE):
                log("STOP file found — exiting cleanly")
                break
            if DURATION and time.time() - started >= DURATION:
                log(f"duration {DURATION}s reached — exiting cleanly")
                break
            t0 = time.time()
            try:
                if t0 - last_sweep >= SWEEP_SECONDS:
                    n = self.sweep()
                    last_sweep = t0
                    hot_preview = ", ".join(sorted(self.hot)[:5])
                    log(f"sweep: {n} weather markets, {len(self.hot)} hot [{hot_preview}{'…' if len(self.hot) > 5 else ''}]")
                self.poll_hot()
            except Exception as e:
                log(f"cycle error: {e}")
                time.sleep(5)
            time.sleep(max(0.5, HOT_SECONDS - (time.time() - t0)))
        self.conn.commit()
        self.conn.close()
        log("recorder stopped cleanly")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        assert cents({"yes_bid_dollars": "0.63"}, "yes_bid") == 63.0
        assert cents({"yes_bid": 63}, "yes_bid") == 63.0
        assert cents({}, "yes_bid") is None
        yb, ya, ybs, nbs = top_of_book({"orderbook": {"yes": [[60, 100], [61, 40]], "no": [[35, 50], [36, 20]]}})
        assert (yb, ya, ybs, nbs) == (61.0, 64.0, 40.0, 20.0), (yb, ya, ybs, nbs)
        yb, ya, ybs, nbs = top_of_book({"orderbook_fp": {"yes_dollars": [["0.6300", "1.00"], ["0.6400", "100.00"]],
                                                          "no_dollars": [["0.1800", "15.00"], ["0.2000", "227.93"]]}})
        assert (yb, ya, ybs, nbs) == (64.0, 80.0, 100.0, 227.93), (yb, ya, ybs, nbs)
        assert pregate(61, 64) and pregate(20, 24)      # NO-side favorite at 78c is valid
        assert not pregate(48, 52) and not pregate(61, 61) and not pregate(95, 97)
        assert is_focus_series("KXHIGHNY") and is_focus_series("KXLOWTSEA") and is_focus_series("KXHIGHTDAL")
        assert not is_focus_series("KXTEMPNYCH") and not is_focus_series("KXNBAGAME") and not is_focus_series("KXRAINSEA")
        # corruption recovery: garbage file must rotate aside and reopen clean
        import tempfile
        tmp = os.path.join(tempfile.mkdtemp(), "market_data.sqlite")
        with open(tmp, "w") as f:
            f.write("this is not a sqlite database")
        conn = db_connect(tmp)
        conn.execute("CREATE TABLE t (x)")
        conn.close()
        assert any(".corrupt-" in p for p in os.listdir(os.path.dirname(tmp)))
        print("recorder selftest: all checks passed")
        sys.exit(0)
    for i, a in enumerate(sys.argv):
        if a == "--duration" and i + 1 < len(sys.argv):
            DURATION = int(sys.argv[i + 1])
        if a == "--db" and i + 1 < len(sys.argv):
            DB_PATH = os.path.abspath(sys.argv[i + 1])
            OUT_DIR = os.path.dirname(DB_PATH)
            STOP_FILE = os.path.join(OUT_DIR, "STOP")
    _lock = acquire_lock()
    Recorder().run()
