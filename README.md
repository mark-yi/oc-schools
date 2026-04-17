# oc-schools

## Setup

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Dashboard public data

Fetch district-level California School Dashboard public API data separately from
the LCAP downloader:

```sh
python3 scripts/fetch_dashboard_public_data.py --county Orange --limit 2
```

By default, per-district JSON is written under
`data/dashboard_public/<year>/by_county/`, with a CDS-code index under
`data/dashboard_public/<year>/by_cds_code/`. A run manifest is written to
`outputs/dashboard_public/`.

Statewide run:

```sh
python3 scripts/fetch_dashboard_public_data.py --year 2025
```

The script is resumable by default. Existing district JSON files are skipped;
pass `--overwrite` to refresh them.
