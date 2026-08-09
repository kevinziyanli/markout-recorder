# markout-recorder  `[claude:markout]`

24/7 public-data recorder for the markout project (Quiet Favorite Maker v2).
Owned by Claude, coordinated through K. Runs entirely on GitHub Actions — no
laptop required.

- `recorder.py` — unauthenticated Kalshi public-data recorder, scoped to daily
  temperature series (KXHIGH*/KXLOW*). **No API keys. No orders. Public market
  data only.**
- `.github/workflows/record.yml` — five ~4h45m shifts per day; each commits a
  compressed SQLite segment to `data/YYYY-MM-DD/`.
- Consumers: the markout analysis project clones this repo and merges segments.

Manual start: Actions tab → "record" → Run workflow.
