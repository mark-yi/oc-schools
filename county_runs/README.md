# County Runs

Prior county-specific work lives here so the repository root can be used for the statewide California run.

## Orange County

- PDFs: `orange_county/lcaps/`
- Extracted JSON: `orange_county/outputs/lcaps_json/`
- Cross-district analysis: `orange_county/outputs/research/`
- Service opportunity analysis: `orange_county/outputs/opportunity_research/`

Useful rerun commands:

```bash
python3 scripts/extract_lcaps.py \
  --input-dir county_runs/orange_county/lcaps \
  --output-dir county_runs/orange_county/outputs/lcaps_json

python3 scripts/analyze_lcaps_report.py \
  --input-path county_runs/orange_county/outputs/lcaps_json/all_lcaps.json \
  --output-dir county_runs/orange_county/outputs/research

python3 scripts/analyze_service_opportunities.py \
  --input-path county_runs/orange_county/outputs/lcaps_json/all_lcaps.json \
  --output-dir county_runs/orange_county/outputs/opportunity_research
```

## Santa Clara

- PDFs: `santa_clara/lcaps/`
- Extracted JSON: `santa_clara/outputs/lcaps_json/`
- Service opportunity analysis: `santa_clara/outputs/opportunity_research/`

## Statewide

Recommended layout for the next run:

```bash
python3 scripts/fetch_cde_districts.py

python3 scripts/download_lcaps.py --year 2025 --download

python3 scripts/extract_lcaps.py \
  --input-dir lcaps_statewide/2025/by_county \
  --recursive \
  --output-dir outputs/statewide_2025/lcaps_json
```

The CDE directory snapshot is saved in:

```text
data/cde/public_districts_raw.txt
data/cde/public_districts.json
data/cde/public_districts.csv
```

`public_districts.json` is the canonical programmatic reference. Refresh it whenever you want the latest CDE directory state:

```bash
python3 scripts/fetch_cde_districts.py
```

Downloaded PDFs are organized as:

```text
lcaps_statewide/
  2025/
    by_county/
      Alameda/
        01611190000000 - Alameda Unified/
          LCAP 2025 - 01611190000000.pdf
    by_cds_code/
      01611190000000.pdf
```

The manifest files in `outputs/lcap_downloads/` keep the original `county`, `district`, `cd_code`, and `cds_code` fields, plus the exact PDF output path.
Districts with no LCAP remain in the manifest with `has_lcap=false` and no PDF path.
