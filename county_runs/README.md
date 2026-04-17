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
python3 scripts/download_lcaps.py --year 2025 --download

python3 scripts/extract_lcaps.py \
  --input-dir lcaps_statewide/2025 \
  --recursive \
  --output-dir outputs/statewide_2025/lcaps_json
```
