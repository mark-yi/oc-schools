# Public Repo Hygiene

This repository is designed to be shared without committing generated statewide
PDFs or large derived data.

## Tracked

- Source scripts in `scripts/`
- Documentation in `README.md`, `docs/`, and `county_runs/README.md`
- Python dependencies in `requirements.txt`
- A small CDE district directory snapshot in `data/cde/`

## Ignored

- Virtual environments and Python caches
- Downloaded LCAP PDFs under `lcaps_statewide/`
- Dashboard API payloads under `data/dashboard_public/`
- Extraction outputs, analytics databases, CSVs, and reports under `outputs/`
- County scratch outputs under `county_runs/*/outputs/`
- SQLite/DB/log/env files

## Before Publishing

Run:

```sh
git status --short
git ls-files
```

Also run your preferred credential scanner before publishing. This repo should not
need credentials because the data sources are public.

Generated files can be recreated with the README pipeline. Do not commit local
`outputs/`, `lcaps_statewide/`, or `data/dashboard_public/` directories unless
you intentionally want to publish a data release separately.

## License

Choose and add a license before public release. If no license is added, GitHub
viewers can inspect the code, but reuse rights are not explicit.
