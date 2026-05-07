# Coding Agent Workflow

This repository is meant to work well with Codex, Claude Code, Cursor, or any
other coding agent that can inspect files, run scripts, and write reports.

The coding agent is not the PDF parser. The parser and analytics builder create
deterministic evidence tables first. The agent is the analyst layer above those
tables.

## Division Of Labor

Deterministic pipeline:

```text
LCAP PDFs
-> scripts/extract_lcaps.py
-> all_lcaps.json
-> scripts/build_analytics_tables.py
-> analytics.sqlite
```

Coding agent:

```text
business question
-> interpret terms and assumptions
-> map to tables, fields, indicators, and joins
-> run SQL
-> inspect outliers and quality flags
-> produce markdown/CSV outputs
```

This pattern keeps the answers reproducible. If a report says a district has a
declining chronic absenteeism rate, that claim should come from
`dashboard_indicators`, not from an LLM's memory or PDF summary.

## Example: Chronic Absenteeism Sales Candidates

User question:

> Build a report of districts with declining chronic absenteeism rates and the
> top candidates to sell on based on how much money they are pouring in via LCAP
> data.

Agent interpretation:

- "chronic absenteeism" maps to `dashboard_indicators.indicator_name = 'chronic_absenteeism'`
- "declining" maps to `dashboard_indicators.change < 0`
- lower is better for chronic absenteeism
- "money pouring in" maps to `sum(lcap_actions.total_funds)`
- attendance-related LCAP actions are detected using title/description terms:
  attendance, absenteeism, truancy, re-engagement, home visit, SARB
- obvious PDF/district mismatches are excluded with `lcap_documents.district_name_match`

Report script:

```sh
.venv/bin/python scripts/report_declining_chronic_absenteeism.py
```

Outputs:

```text
outputs/analytics/2025/reports/declining_chronic_absenteeism_sales_report.md
outputs/analytics/2025/reports/declining_chronic_absenteeism_top_candidates.csv
outputs/analytics/2025/reports/declining_chronic_absenteeism_all_districts.csv
```

In one local statewide run, the report found:

- 636 districts with declining chronic absenteeism
- 579 declining districts with broad attendance-adjacent LCAP actions
- 437 declining districts with strict attendance-titled LCAP actions
- $4.36B in broad attendance-adjacent LCAP dollars
- $356.7M in strict attendance-titled LCAP dollars

Top broad-spend candidates in that run:

| District | Chronic absenteeism rate | Chronically absent students | Broad attendance-adjacent LCAP $ | Strict/actionable attendance $ | Actionable share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Los Angeles Unified | 21.9% | 58,700 | $690.2M | $13.4M | 1.9% |
| Fresno Unified | 29.4% | 14,878 | $609.4M | $1.7M | 0.3% |
| Clovis Unified | 14.1% | 4,319 | $583.1M | $0.9M | 0.2% |
| San Diego Unified | 19.0% | 13,423 | $201.8M | $1.5M | 0.8% |
| Oakland Unified | 27.9% | 7,062 | $93.7M | $10.9M | 11.6% |

This table is deliberately not just "biggest budget." It combines:

- residual absenteeism pain: current chronic absenteeism rate and affected student count
- total strategic motion: broad attendance-adjacent LCAP dollars
- cleaner actionable budget: strict attendance-titled dollars
- signal quality: strict dollars as a share of broad dollars

That distinction changes the sales read. Los Angeles Unified and Fresno Unified
have massive broad funding, but the strict/actionable share is small, so an AE
should inspect whether attendance is buried inside broad whole-child/base-program
investments. Oakland Unified has high residual need and a much larger actionable
share, which is a cleaner attendance-specific wedge.

These numbers are generated outputs and are not committed. Rerun the pipeline to
refresh them.

## Prompt Shape For Future Reports

Good agent prompts:

```text
Using outputs/analytics/2025/analytics.sqlite, find districts with worsening
suspension rates and LCAP actions that mention restorative practices, PBIS, or
behavior intervention. Rank by residual need and planned spend, then write a
markdown report with source-page evidence.
```

```text
Find districts with weak EL progress on the Dashboard and LCAP actions that
mention ELD, newcomer supports, reclassification, or multilingual family
engagement. Separate software/data-system opportunities from staffing/base
programs.
```

The agent should always state:

- how it mapped the business question to fields
- what filters it used
- whether spend is broad/heuristic or strict/clean
- which source pages support the account notes
- what quality flags or caveats remain
