---
name: lcap-gtm-analyst
description: Answer account-executive and GTM strategy questions over California LCAP and California School Dashboard data. Use when Codex needs to find, rank, or explain school district opportunities using Dashboard outcomes, LCAP goals/actions/metrics, action budgets, section-cited narrative chunks, or the repo's local MCP/query scripts.
---

# LCAP GTM Analyst

Use this skill as the routing and reasoning layer for AE-facing questions such as:

- "Find districts with worsening chronic absenteeism and funded attendance actions."
- "Which districts are investing in family engagement but still have weak outcomes?"
- "Explain why Oakland Unified is a good or bad attendance opportunity."
- "Build a territory scan with source-backed account notes."

Do not answer from memory. Map each claim to the strongest local source, run the query/tool, then synthesize the sales read.

## Source Precedence

Use deterministic tables for numeric claims:

| Claim | Source |
| --- | --- |
| District/account metadata | `districts` |
| Dashboard outcome status/change/counts | `dashboard_indicators`, then `dashboard_student_groups` for subgroups |
| LCAP action budgets | `lcap_actions.total_funds` |
| LCAP stated goals | `lcap_goals` |
| LCAP metric text | `lcap_metrics` |
| PDF/source quality | `lcap_documents.district_name_match`, extraction warnings |
| District-authored context/evidence | narrative chunks via BM25/hybrid retrieval |

Use narrative retrieval for qualitative signal, not for authoritative budgets or Dashboard trends.

## Fast Path

For chronic absenteeism opportunity scans, prefer the reusable query script:

```sh
.venv/bin/python scripts/find_lcap_opportunities.py \
  --topic chronic_absenteeism \
  --outcome-trend worsening \
  --rank-by strict_action_funds \
  --limit 25
```

Use `--outcome-trend improving` for improving outcomes. Use `--outcome-trend decreasing_rate` when the user literally means the chronic absenteeism rate declined.

If MCP is available, prefer:

- `lcap_find_opportunities`
- `lcap_explain_account`
- `lcap_search_narratives`
- `lcap_get_district_context`

Otherwise run the scripts directly in the repo.

## Interpret AE Language

Normalize business terms before querying:

- "chronic absenteeism", "attendance", "truancy", "SARB", "SART", "re-engagement", "home visits" -> `topic=chronic_absenteeism`
- "money", "budget", "pouring in", "funding" -> `lcap_actions.total_funds`
- "goals pertaining to..." -> match `lcap_goals.description`, `lcap_metrics.*`, and relevant `lcap_actions`
- "actionable spend" -> prefer strict action-title matches and actions classified as software/data, outreach workflow, case management, home visits, intervention, or attendance systems
- "broad spend" -> allow action description matches, but warn that large base/staffing bundles can inflate spend

For chronic absenteeism, lower is better:

- `change < 0` means the rate decreased and the Dashboard outcome improved.
- `change > 0` means the rate increased and the Dashboard outcome worsened.
- If a user says "outcomes are declining," usually treat that as `worsening`; if they say "rates are declining," treat that as `decreasing_rate`.

## Workflow

1. Restate the field mapping briefly: topic, outcome trend, budget definition, and ranking.
2. Query structured data first. Use `scripts/find_lcap_opportunities.py` for supported chronic absenteeism scans or SQL over `outputs/analytics/2025/analytics.sqlite` for custom scans.
3. Pull narrative evidence only after identifying candidate districts. Use `lcap_explain_account` or `scripts/search_lcap_retrieval.py` filtered by `cds_code`.
4. Inspect caveats: district-name mismatch, broad vs strict spend, bundled base operations, missing narrative chunks, and whether the requested metric is positive or negative polarity.
5. Return an AE-ready ranking, not raw rows.

## Output Contract

For opportunity lists, include:

- rank
- district and county
- current outcome status/rate
- outcome change
- affected student count when available
- broad topic action dollars
- strict/actionable topic dollars
- top funded action and source pages
- sales read
- caveat

For account briefs, include:

- Dashboard fact pattern
- relevant LCAP goals/metrics/actions
- narrative evidence with pages/chunk IDs
- "why now" and "what to pitch"
- what to verify before outreach

Always state whether the ranking is by broad spend, strict spend, affected students, current rate, trend, or opportunity score.

## References

Read `references/source-map.md` when you need schema details, command examples, metric polarity, or the MCP/script surface.
