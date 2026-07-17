# python/ — NCAA baseball pbp producer (Python-side)

Python-side capture of NCAA baseball (`sport_code=MBA`) play-by-play from
`stats.ncaa.org`, the replacement for the legacy R scrape path (`R/ncaa_0*.R` via
`baseballr::ncaa_*`) per the standing directive to move -data extraction to Python.

Discovers a season's contests (team list → team pages → contest_ids) and captures
the raw pbp HTML as idempotent, resumable gzip bundles. **Parsing lives in sdv-py**
(`sportsdataverse.baseball.college_baseball.parse_college_baseball_ncaa_pbp`,
validated on 3 real D1 games: 0 unknown play types, `runs_scored` reconciles to the
final score); this producer is capture-only.

```
discover.py       # team list (MBA) -> team pages -> contest_ids
capture.py        # /contests/{id}/play_by_play (+ box_score) -> json/{id}.json.gz
run.py            # live runner (holds one browser session; NCAA_PROXY_POOL env)
test_discover.py  # offline (pure parsers + injected fetch)
test_capture.py   # offline (real fixture + tmp out_dir)
tests/fixtures/   # a real captured pbp page
```

## Run

```sh
NCAA_PROXY_POOL="$(cat proxies.txt)" \
  python python/run.py --sport MBA --year 2025 --out ./ncaa --max 200
```

Transport = `sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher.with_browser`
(patchright + `--headless=new` + a **US-residential** proxy pool — datacenter IPs
get an instant edge 403). Hold ONE session (no per-call relaunch). stats.ncaa.org
IP-bans scrapers — run sparingly, paced, from a residential IP.

Softball (`WSB`) has its own producer repo (`ncaa-softball-raw`).
