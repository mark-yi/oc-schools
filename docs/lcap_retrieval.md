# LCAP Narrative Retrieval

This layer lets a coding agent search district-authored LCAP narrative sections with citations. It complements the structured analytics database; numeric claims should still come from `analytics.sqlite`.

## Build Order

1. Build the existing structured tables:

```bash
.venv/bin/python scripts/build_analytics_tables.py --year 2025
```

2. Extract narrative sections and chunks:

```bash
.venv/bin/python scripts/extract_lcap_narratives.py
```

The extractor uses normal PDF text extraction first. If a PDF has missing or
garbled text and `tesseract` is installed, it falls back to rotation-aware OCR
for that document. Documents whose manifest district name conflicts with the PDF
text are excluded instead of being embedded under the wrong account.

Smoke test one district:

```bash
.venv/bin/python scripts/extract_lcap_narratives.py --cds-code 01611190000000
```

3. Validate chunks before embedding:

```bash
.venv/bin/python scripts/validate_lcap_chunks.py --strict
```

This writes QA files under `outputs/rag/2025/qa/`, including table/template
issue rows, manual review samples, and `no_chunk_documents.csv`.

4. Build the local BM25 index only:

```bash
.venv/bin/python scripts/build_lcap_retrieval_index.py --bm25-only --rebuild
```

5. Build dense Chroma embeddings:

```bash
OPENAI_API_KEY=... .venv/bin/python scripts/build_lcap_retrieval_index.py --rebuild
```

The default embedding model is `text-embedding-3-small`.

## Artifacts

```text
outputs/rag/2025/sections.jsonl
outputs/rag/2025/chunks.jsonl
outputs/rag/2025/extraction_summary.csv
outputs/rag/2025/qa/validation_summary.json
outputs/rag/2025/qa/no_chunk_documents.csv
outputs/rag/2025/lcap_retrieval.sqlite
outputs/rag/2025/chroma/
```

`chunks.jsonl` is the portable source artifact. `lcap_retrieval.sqlite` contains chunk metadata, previous/next chunk links, an embedding cache, and an FTS5 BM25 index. `chroma/` contains the persistent dense vector collection.

## Querying

BM25-only local search:

```bash
.venv/bin/python scripts/search_lcap_retrieval.py \
  "chronic absenteeism family outreach attendance barriers" \
  --bm25-only \
  --limit 10
```

Hybrid BM25 + dense search:

```bash
OPENAI_API_KEY=... .venv/bin/python scripts/search_lcap_retrieval.py \
  "students missing too much school and family re-engagement" \
  --limit 10
```

Restrict to districts with declining Dashboard chronic absenteeism:

```bash
.venv/bin/python scripts/search_lcap_retrieval.py \
  "attendance barriers family outreach re-engagement" \
  --bm25-only \
  --declining-chronic-absenteeism
```

Useful filters:

```bash
--county "Los Angeles"
--district "Oakland Unified"
--cds-code 01612590000000
--section-type goal_analysis
--chunk-kind action_description
```

## MCP

Run the local MCP server:

```bash
.venv/bin/python scripts/lcap_mcp_server.py --bm25-only
```

For hybrid search, omit `--bm25-only` and set `OPENAI_API_KEY`.

Example Codex MCP command:

```bash
.venv/bin/python scripts/lcap_mcp_server.py
```

Tools exposed:

- `lcap_find_opportunities`
- `lcap_explain_account`
- `lcap_search_narratives`
- `lcap_search_with_dashboard_cohort`
- `lcap_get_chunk`
- `lcap_get_district_context`
- `lcap_collection_stats`

The MCP server is intentionally domain-specific. It returns cited chunks and can combine narrative search with Dashboard/LCAP facts instead of exposing raw Chroma access only.

## Design Notes

- Chunking never crosses district, major section, goal, or action boundaries.
- The extractor skips table-heavy text, LCFF budget tables, and the state template/instruction appendix.
- Spanish section headings and OCR-only PDFs are handled before validation.
- `search_text` includes breadcrumbs such as district, section, goal, action, and prompt; `body_text` preserves the cited narrative.
- BM25 catches exact terms like `SARB`, `truancy`, and `chronic absenteeism`.
- Dense search catches semantic language like `students missing too much school`.
- RRF rank fusion is the default reranker. Optional LLM reranking is available with `--rerank llm`.
