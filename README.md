# California LCAP Intelligence

This repository turns California Local Control and Accountability Plan (LCAP)
PDFs plus California School Dashboard public data into queryable account
intelligence.

The motivating use case is go-to-market research for education companies:

> Which districts are already investing in a problem area, which districts still
> have measurable pain, and what evidence can an account executive cite before
> outreach?

The current pipeline downloads public LCAP PDFs, extracts goals/actions/metrics,
fetches district Dashboard outcomes, flattens both sources into analytics tables,
and produces reports such as:

> Show districts where chronic absenteeism is declining, then rank the best sales
> candidates by attendance-related LCAP dollars and remaining need.

The intended workflow is coding-agent friendly: a tool like Codex, Claude Code,
Cursor, or another repo-aware coding agent can read the schema, write SQL against
`analytics.sqlite`, generate one-off reports, and turn query results into
evidence-backed account summaries. The agent should reason over deterministic
tables for numeric claims, then use the optional narrative retrieval layer to
find section-cited LCAP evidence.

## What Is In This Repo

Tracked source files:

- `scripts/fetch_cde_districts.py` - refreshes the California public district directory.
- `scripts/download_lcaps.py` - discovers and downloads public LCAP PDFs.
- `scripts/extract_lcaps.py` - parses LCAP PDFs into structured JSON.
- `scripts/fetch_dashboard_public_data.py` - fetches district-level California School Dashboard public API data.
- `scripts/build_analytics_tables.py` - flattens nested LCAP/Dashboard JSON into CSVs and SQLite.
- `scripts/report_declining_chronic_absenteeism.py` - example GTM report over the analytics database.
- `scripts/extract_lcap_narratives.py` - extracts section-tagged LCAP narrative chunks.
- `scripts/build_lcap_retrieval_index.py` - builds SQLite BM25 plus Chroma dense retrieval indexes.
- `scripts/search_lcap_retrieval.py` - searches LCAP narratives with BM25, dense retrieval, and reranking.
- `scripts/find_lcap_opportunities.py` - reusable account-opportunity query CLI for AE/GTM scans.
- `scripts/lcap_mcp_server.py` - exposes local LCAP retrieval tools to MCP-capable agents.
- `scripts/analyze_*.py` - earlier exploratory research scripts over extracted LCAP data.
- `skills/lcap-gtm-analyst/` - shareable Codex skill that teaches an agent how to route AE questions across Dashboard, LCAP, and narrative sources.
- `data/cde/public_districts.*` - a checked-in CDE district snapshot used as a seed.

Generated artifacts are intentionally ignored:

- downloaded PDFs under `lcaps_statewide/`
- raw Dashboard JSON under `data/dashboard_public/`
- extracted JSON and analytics outputs under `outputs/`
- county scratch runs under `county_runs/*/outputs/`

This keeps the public repo light while leaving the full pipeline reproducible.

## Setup

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

The public-data pipeline does not require API keys. Dense narrative retrieval
uses `OPENAI_API_KEY` to create/query embeddings; BM25-only narrative search
works without an API key.

For scanned LCAP PDFs, install the `tesseract` binary if you want OCR fallback
during narrative extraction. The extractor still works without it for normal
text PDFs.

## End-To-End Pipeline

### 1. Refresh The District Directory

```sh
.venv/bin/python scripts/fetch_cde_districts.py
```

This writes:

```text
data/cde/public_districts_raw.txt
data/cde/public_districts.json
data/cde/public_districts.csv
```

### 2. Download LCAP PDFs

Small county test:

```sh
.venv/bin/python scripts/download_lcaps.py --year 2025 --county Orange --download
```

Statewide run:

```sh
.venv/bin/python scripts/download_lcaps.py --year 2025 --download
```

Outputs are written under:

```text
lcaps_statewide/2025/by_county/
lcaps_statewide/2025/by_cds_code/
outputs/lcap_downloads/
```

The download manifest preserves canonical `county`, `district`, `cd_code`,
`cds_code`, PDF URL, and local output path.

### 3. Extract LCAP Goals, Metrics, Actions, And Dollars

```sh
.venv/bin/python scripts/extract_lcaps.py \
  --input-dir lcaps_statewide/2025/by_county \
  --recursive \
  --workers 4 \
  --output-dir outputs/statewide_2025/lcaps_json
```

Main output:

```text
outputs/statewide_2025/lcaps_json/all_lcaps.json
```

The extractor keeps source pages so downstream reports can cite where a goal,
metric, or action came from.

### 4. Fetch California School Dashboard Public Data

Small county test:

```sh
.venv/bin/python scripts/fetch_dashboard_public_data.py --year 2025 --county Orange
```

Statewide run:

```sh
.venv/bin/python scripts/fetch_dashboard_public_data.py --year 2025
```

Outputs are written under:

```text
data/dashboard_public/2025/by_county/
data/dashboard_public/2025/by_cds_code/
outputs/dashboard_public/
```

The script is resumable by default. Existing district JSON files are skipped
unless `--overwrite` is provided.

### 5. Build Flat Analytics Tables

```sh
.venv/bin/python scripts/build_analytics_tables.py --year 2025
```

Outputs:

```text
outputs/analytics/2025/analytics.sqlite
outputs/analytics/2025/districts.csv
outputs/analytics/2025/lcap_documents.csv
outputs/analytics/2025/lcap_goals.csv
outputs/analytics/2025/lcap_actions.csv
outputs/analytics/2025/lcap_metrics.csv
outputs/analytics/2025/dashboard_indicators.csv
outputs/analytics/2025/dashboard_student_groups.csv
outputs/analytics/2025/dashboard_trends.csv
```

The SQLite database is easiest for ad hoc analysis. The CSVs are useful for
inspection, sharing, and loading into Postgres/DuckDB.

### 6. Build Optional Narrative Retrieval

After `analytics.sqlite` exists, extract section-tagged narrative chunks:

```sh
.venv/bin/python scripts/extract_lcap_narratives.py
```

The narrative extractor removes table/template noise, supports Spanish LCAP
section headings, and uses local Tesseract OCR when a PDF has scanned or
garbled text. Validate chunks before embedding:

```sh
.venv/bin/python scripts/validate_lcap_chunks.py --strict
```

Build the local BM25 index only:

```sh
.venv/bin/python scripts/build_lcap_retrieval_index.py --bm25-only --rebuild
```

Build the Chroma dense index:

```sh
OPENAI_API_KEY=... .venv/bin/python scripts/build_lcap_retrieval_index.py --rebuild
```

Search the narrative layer:

```sh
.venv/bin/python scripts/search_lcap_retrieval.py \
  "chronic absenteeism family outreach attendance barriers" \
  --bm25-only \
  --declining-chronic-absenteeism
```

The retrieval artifacts live under `outputs/rag/2025/`. See
`docs/lcap_retrieval.md` for the chunking, hybrid retrieval, reranking, and MCP
workflow.

QA artifacts live under `outputs/rag/2025/qa/`. The validator writes
`validation_summary.json`, issue CSVs, review samples, and a
`no_chunk_documents.csv` audit for source PDFs that do not contain usable LCAP
narrative bodies.

## Example: Chronic Absenteeism GTM Report

After building `analytics.sqlite`, generate the example report:

```sh
.venv/bin/python scripts/report_declining_chronic_absenteeism.py
```

Outputs:

```text
outputs/analytics/2025/reports/declining_chronic_absenteeism_sales_report.md
outputs/analytics/2025/reports/declining_chronic_absenteeism_top_candidates.csv
outputs/analytics/2025/reports/declining_chronic_absenteeism_all_districts.csv
```

The report answers:

> Which districts have declining chronic absenteeism, but still have residual
> chronic absenteeism need and LCAP dollars attached to attendance-related work?

For interactive AE-style querying, use the reusable opportunity CLI instead of
starting from raw SQL:

```sh
.venv/bin/python scripts/find_lcap_opportunities.py \
  --topic chronic_absenteeism \
  --outcome-trend worsening \
  --rank-by strict_action_funds \
  --limit 25
```

For chronic absenteeism, `worsening` means the rate increased. Use
`--outcome-trend decreasing_rate` when the user literally means the chronic
absenteeism rate declined.

This example came from an agent-style analysis loop:

1. Interpret the business question.
2. Map "chronic absenteeism" to Dashboard `indicator_name = chronic_absenteeism`.
3. Map "declining" to `change < 0` because lower is better for this indicator.
4. Join those districts to LCAP actions that mention attendance, absenteeism,
   truancy, re-engagement, home visits, or SARB.
5. Rank candidates by LCAP dollars, residual chronic absenteeism rate, chronic
   student count, and evidence quality.
6. Write a markdown report plus CSVs that an account executive can inspect.

It uses:

- Dashboard `indicator_name = chronic_absenteeism`
- `student_group = ALL`
- `change < 0` as the definition of declining/improving
- LCAP actions whose title or description mentions attendance, absenteeism,
  truancy, re-engagement, home visits, or SARB

The report includes two spend views:

- **Broad attendance-adjacent spend**: title or description matches the attendance terms.
- **Strict attendance-titled spend**: only the action title matches the attendance terms.

Broad spend catches large whole-child and student-support investments that
mention attendance. Strict spend is cleaner but misses bundled investments. The
report keeps both because sales research needs signal and skepticism.

In one local statewide run, the report surfaced:

- `636` districts with declining chronic absenteeism.
- `579` declining districts with broad attendance-adjacent LCAP actions.
- `437` declining districts with strict attendance-titled LCAP actions.
- `$4.36B` in broad attendance-adjacent LCAP dollars.
- `$356.7M` in strict attendance-titled LCAP dollars.

The top broad-spend candidates in that run were Los Angeles Unified, Fresno
Unified, Clovis Unified, San Diego Unified, and Oakland Unified. These numbers
are generated artifacts, not checked into the repo; rerun the pipeline to refresh
them.

The part that makes the example useful for GTM is the account-ranking view:

| District | Chronic absenteeism rate | Chronically absent students | Broad attendance-adjacent LCAP $ | Strict/actionable attendance $ | Actionable share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Los Angeles Unified | 21.9% | 58,700 | $690.2M | $13.4M | 1.9% |
| Fresno Unified | 29.4% | 14,878 | $609.4M | $1.7M | 0.3% |
| Clovis Unified | 14.1% | 4,319 | $583.1M | $0.9M | 0.2% |
| San Diego Unified | 19.0% | 13,423 | $201.8M | $1.5M | 0.8% |
| Oakland Unified | 27.9% | 7,062 | $93.7M | $10.9M | 11.6% |

The read is different by account. Los Angeles Unified and Fresno Unified show
huge broad budgets but relatively small strict attendance-titled spend, so the
sales motion should inspect whether attendance is buried inside large whole-child
or base-program investments. Oakland has high residual need and a much larger
strict/actionable share, making it a cleaner attendance-specific opportunity.

Example SQL behind the core Dashboard filter:

```sql
select county, district, status, change, count, chronic_count
from dashboard_indicators
where indicator_name = 'chronic_absenteeism'
  and student_group = 'ALL'
  and change < 0
order by change asc;
```

Example SQL joining Dashboard outcomes to LCAP evidence:

```sql
select
  d.county,
  d.district,
  di.status as chronic_absenteeism_rate,
  di.change as chronic_absenteeism_change,
  sum(coalesce(a.total_funds, 0)) as attendance_related_lcap_spend,
  count(a.action_id) as attendance_related_actions
from dashboard_indicators di
join districts d on d.cds_code = di.cds_code
join lcap_actions a on a.cds_code = di.cds_code
join lcap_documents ld
  on ld.cds_code = a.cds_code
 and coalesce(ld.district_name_match, 1) != 0
where di.indicator_name = 'chronic_absenteeism'
  and di.student_group = 'ALL'
  and di.change < 0
  and (
    lower(a.title || ' ' || a.description) like '%attendance%'
    or lower(a.title || ' ' || a.description) like '%absen%'
    or lower(a.title || ' ' || a.description) like '%truanc%'
    or lower(a.title || ' ' || a.description) like '%re-engagement%'
    or lower(a.title || ' ' || a.description) like '%home visit%'
    or lower(a.title || ' ' || a.description) like '%sarb%'
  )
group by d.county, d.district, di.status, di.change
order by attendance_related_lcap_spend desc;
```

## Analytics Schema

The flattened tables are intentionally simple:

- `districts` - canonical CDE district metadata keyed by `cds_code`.
- `lcap_documents` - one row per parsed LCAP PDF, including extraction counts and QA fields.
- `lcap_goals` - one row per LCAP goal.
- `lcap_actions` - one row per LCAP action, including parsed dollars and source pages.
- `lcap_metrics` - one row per LCAP metric with raw baseline/outcome/target text.
- `dashboard_indicators` - one row per district-level Dashboard summary indicator.
- `dashboard_student_groups` - one row per district, indicator, and student group.
- `dashboard_trends` - multi-year trend values where the Dashboard API provides them.

The `lcap_documents.district_name_match` field is a QA flag. It is `0` when the
district name parsed from the PDF obviously conflicts with the manifest district.
Downstream reports should exclude those rows from LCAP spend joins until the PDF
is repaired or redownloaded.

See `docs/analytics_schema.md` for a longer schema reference.
See `docs/agent_workflow.md` for the intended Codex/Claude Code-style analysis
workflow.
See `skills/lcap-gtm-analyst/` for the shareable Codex skill that packages the
AE analyst playbook.

## Design Notes

This repo does not start with embeddings or a chatbot. It first builds a
deterministic evidence layer:

```text
public PDFs + public Dashboard API
-> extracted LCAP evidence
-> flattened facts
-> SQL/reporting
-> account intelligence
```

That makes it possible to answer measured-outcome questions with reproducible
queries. The narrative retrieval layer sits beside those tables for qualitative
questions where the district's own language matters.

The coding agent sits above that layer:

```text
user asks a GTM question
-> agent maps the question to Dashboard indicators, LCAP fields, and SQL joins
-> agent runs deterministic queries over analytics.sqlite
-> agent searches section-tagged LCAP chunks when narrative evidence is needed
-> agent writes a sourced markdown/CSV report
-> human inspects source pages before customer-facing use
```

For example, "show me districts with declining chronic absenteeism and lots of
LCAP attendance spend" is not an embedding problem. It is a reasoning-and-query
problem:

```text
chronic absenteeism = dashboard_indicators.indicator_name = 'chronic_absenteeism'
declining = dashboard_indicators.change < 0
money pouring in = sum(lcap_actions.total_funds) for attendance-related actions
evidence = lcap_actions.source_pages + action title/description
```

Embeddings and reranking are for fuzzy semantic discovery. They sit on top of
the flat facts rather than replacing them.

## Public Data Caveats

- LCAP PDFs are public documents, but they are generated by many districts and
  are not perfectly uniform.
- Parsed dollar totals are only as reliable as the PDF tables and the current
  extraction heuristics.
- Keyword-based spend categories are useful for triage, not final market sizing.
- Always inspect source pages before making a customer-facing claim.
