# LCAP GTM Source Map

## Core Artifacts

```text
outputs/analytics/2025/analytics.sqlite
outputs/rag/2025/lcap_retrieval.sqlite
outputs/rag/2025/chroma/
outputs/rag/2025/chunks.jsonl
```

`analytics.sqlite` is the source of truth for structured facts. `lcap_retrieval.sqlite` and `chroma/` are narrative retrieval indexes.

## Tables

- `districts`: `cds_code`, `county`, `district`, contact/admin metadata, `has_lcap`, `has_dashboard`.
- `lcap_documents`: parsed PDF metadata and QA flags. Exclude spend joins where `district_name_match = 0`.
- `lcap_goals`: goal descriptions and source pages.
- `lcap_actions`: action title, description, `total_funds`, contributing flag, source pages.
- `lcap_metrics`: raw baseline/outcome/target text from LCAP metric tables.
- `dashboard_indicators`: district-level Dashboard indicators: `status`, `change`, `count`, `chronic_count`, performance colors.
- `dashboard_student_groups`: subgroup Dashboard rows.
- `dashboard_trends`: multi-year trend fields when available.

## Metric Polarity

For `chronic_absenteeism` and `suspension_rate`, lower is better:

- improving -> `change < 0`
- worsening -> `change > 0`

For metrics such as graduation, college/career, ELA, math, science, and EL progress, verify polarity before filtering. Do not assume "declining" means the same thing across indicators.

## Opportunity CLI

```sh
.venv/bin/python scripts/find_lcap_opportunities.py \
  --topic chronic_absenteeism \
  --outcome-trend worsening \
  --rank-by strict_action_funds \
  --limit 25
```

Useful variants:

```sh
.venv/bin/python scripts/find_lcap_opportunities.py --outcome-trend improving --rank-by broad_action_funds
.venv/bin/python scripts/find_lcap_opportunities.py --outcome-trend decreasing_rate --rank-by opportunity_score
.venv/bin/python scripts/find_lcap_opportunities.py --county "Los Angeles" --json
```

## Narrative Search

BM25-only:

```sh
.venv/bin/python scripts/search_lcap_retrieval.py \
  "attendance barriers family outreach re-engagement" \
  --bm25-only \
  --cds-code 01612590000000 \
  --limit 8
```

Hybrid retrieval:

```sh
set -a; source .env; set +a
.venv/bin/python scripts/search_lcap_retrieval.py \
  "students missing too much school family outreach barriers" \
  --cds-code 01612590000000 \
  --limit 8
```

## MCP Tools

When the MCP server is running, use:

- `lcap_find_opportunities`: ranked structured opportunities from Dashboard outcomes plus LCAP action budgets.
- `lcap_explain_account`: structured account context plus narrative evidence for one district.
- `lcap_search_narratives`: BM25/hybrid retrieval over section-tagged LCAP chunks.
- `lcap_search_with_dashboard_cohort`: narrative search after a Dashboard-derived cohort filter.
- `lcap_get_district_context`: Dashboard/LCAP facts and chunk coverage.
- `lcap_get_chunk`: one narrative chunk plus neighbors.
- `lcap_collection_stats`: retrieval collection counts.

Start MCP:

```sh
.venv/bin/python scripts/lcap_mcp_server.py --bm25-only
```

Omit `--bm25-only` and set `OPENAI_API_KEY` for hybrid dense search.

## Caveats To Surface

- Broad spend includes action-description matches and can include huge base-program budgets.
- Strict spend only counts action-title matches and can miss embedded attendance work.
- Parsed LCAP tables are not final market sizing without source-page review.
- Narrative retrieval is evidence discovery, not the source of numeric truth.
- Always identify whether the account has residual pain after improvement or worsening pain with budgeted response.
