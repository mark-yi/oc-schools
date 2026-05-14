#!/usr/bin/env python3
"""Search LCAP narrative chunks with BM25 + dense retrieval + optional reranking."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAG_DIR = ROOT / "outputs" / "rag" / "2025"
DEFAULT_DB_PATH = DEFAULT_RAG_DIR / "lcap_retrieval.sqlite"
DEFAULT_CHROMA_DIR = DEFAULT_RAG_DIR / "chroma"
DEFAULT_COLLECTION = "lcap_narrative_chunks"
DEFAULT_ANALYTICS_DB = ROOT / "outputs" / "analytics" / "2025" / "analytics.sqlite"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "is",
    "of",
    "on",
    "or",
    "our",
    "show",
    "that",
    "the",
    "their",
    "to",
    "with",
}


def stable_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:24]


def connect_db(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def fts_query(query: str) -> str:
    tokens = [
        token.casefold()
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{1,}", query)
        if token.casefold() not in STOPWORDS
    ]
    if not tokens:
        return ""
    # OR keeps recall high; the RRF/rerank stages handle precision.
    return " OR ".join(f'"{token}"' for token in tokens[:24])


def filter_sql(filters: dict[str, Any], cds_codes: list[str] | None = None, alias: str = "c") -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for field in ["county", "district", "cds_code", "school_year", "section_type", "chunk_kind", "goal_number"]:
        value = filters.get(field)
        if value in (None, ""):
            continue
        if field == "district" and "*" in str(value):
            clauses.append(f"{alias}.{field} like ?")
            params.append(str(value).replace("*", "%"))
        else:
            clauses.append(f"{alias}.{field} = ?")
            params.append(value)
    if cds_codes:
        placeholders = ", ".join("?" for _ in cds_codes)
        clauses.append(f"{alias}.cds_code in ({placeholders})")
        params.extend(cds_codes)
    if not clauses:
        return "", []
    return " and " + " and ".join(clauses), params


def chroma_where(filters: dict[str, Any], cds_codes: list[str] | None = None) -> dict[str, Any] | None:
    clauses: list[dict[str, Any]] = []
    for field in ["county", "district", "cds_code", "school_year", "section_type", "chunk_kind", "goal_number"]:
        value = filters.get(field)
        if value in (None, "") or "*" in str(value):
            continue
        clauses.append({field: {"$eq": value}})
    if cds_codes:
        clauses.append({"cds_code": {"$in": cds_codes}})
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def embed_query(query: str, model: str, dimensions: int | None) -> list[float]:
    try:
        from openai import OpenAI
    except ImportError as error:  # pragma: no cover - import availability is environment-specific.
        raise RuntimeError("Install the `openai` package to run dense search.") from error
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required for dense search.")
    client = OpenAI()
    kwargs: dict[str, Any] = {"model": model, "input": query}
    if dimensions:
        kwargs["dimensions"] = dimensions
    response = client.embeddings.create(**kwargs)
    return response.data[0].embedding


def bm25_search(
    connection: sqlite3.Connection,
    query: str,
    filters: dict[str, Any],
    cds_codes: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    match_query = fts_query(query)
    if not match_query:
        return []
    where, params = filter_sql(filters, cds_codes)
    sql = f"""
        select
          c.*,
          bm25(chunks_fts) as bm25_score
        from chunks_fts
        join chunks c on c.chunk_id = chunks_fts.chunk_id
        where chunks_fts match ?
          {where}
        order by bm25_score asc
        limit ?
    """
    rows = connection.execute(sql, [match_query, *params, limit]).fetchall()
    results: list[dict[str, Any]] = []
    for rank, row in enumerate(rows, start=1):
        item = dict(row)
        item["bm25_rank"] = rank
        item["bm25_score"] = row["bm25_score"]
        results.append(item)
    return results


def dense_search(
    query: str,
    filters: dict[str, Any],
    cds_codes: list[str] | None,
    chroma_dir: Path,
    collection_name: str,
    embedding_model: str,
    dimensions: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    try:
        import chromadb
    except ImportError as error:  # pragma: no cover - import availability is environment-specific.
        raise RuntimeError("Install the `chromadb` package to run dense search.") from error

    query_embedding = embed_query(query, embedding_model, dimensions)
    client = chromadb.PersistentClient(path=str(chroma_dir))
    collection = client.get_collection(collection_name)
    where = chroma_where(filters, cds_codes)
    response = collection.query(
        query_embeddings=[query_embedding],
        n_results=limit,
        where=where,
        include=["metadatas", "documents", "distances"],
    )
    ids = response.get("ids", [[]])[0]
    metadatas = response.get("metadatas", [[]])[0]
    documents = response.get("documents", [[]])[0]
    distances = response.get("distances", [[]])[0]
    results: list[dict[str, Any]] = []
    for rank, chunk_id in enumerate(ids, start=1):
        metadata = dict(metadatas[rank - 1] or {})
        metadata["chunk_id"] = chunk_id
        metadata["search_text"] = documents[rank - 1] if rank - 1 < len(documents) else ""
        metadata["dense_distance"] = distances[rank - 1] if rank - 1 < len(distances) else None
        metadata["dense_rank"] = rank
        results.append(metadata)
    return results


def load_chunks(connection: sqlite3.Connection, chunk_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not chunk_ids:
        return {}
    placeholders = ", ".join("?" for _ in chunk_ids)
    rows = connection.execute(f"select * from chunks where chunk_id in ({placeholders})", chunk_ids).fetchall()
    return {row["chunk_id"]: dict(row) for row in rows}


def rrf_fuse(
    connection: sqlite3.Connection,
    bm25_results: list[dict[str, Any]],
    dense_results: list[dict[str, Any]],
    k: int = 60,
) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    for row in bm25_results:
        chunk_id = row["chunk_id"]
        candidates.setdefault(chunk_id, {"chunk_id": chunk_id})
        candidates[chunk_id].update(
            {
                "bm25_rank": row.get("bm25_rank"),
                "bm25_score": row.get("bm25_score"),
            }
        )
    for row in dense_results:
        chunk_id = row["chunk_id"]
        candidates.setdefault(chunk_id, {"chunk_id": chunk_id})
        candidates[chunk_id].update(
            {
                "dense_rank": row.get("dense_rank"),
                "dense_distance": row.get("dense_distance"),
            }
        )

    chunk_rows = load_chunks(connection, list(candidates))
    fused: list[dict[str, Any]] = []
    for chunk_id, signals in candidates.items():
        score = 0.0
        if signals.get("bm25_rank"):
            score += 1.0 / (k + int(signals["bm25_rank"]))
        if signals.get("dense_rank"):
            score += 1.0 / (k + int(signals["dense_rank"]))
        row = chunk_rows.get(chunk_id, {"chunk_id": chunk_id})
        row.update(signals)
        row["rrf_score"] = score
        fused.append(row)
    fused.sort(key=lambda item: (-float(item.get("rrf_score") or 0), item.get("chunk_id", "")))
    return fused


def fetch_declining_chronic_absenteeism_cds(analytics_db: Path) -> list[str]:
    connection = sqlite3.connect(analytics_db)
    try:
        rows = connection.execute(
            """
            select distinct cds_code
            from dashboard_indicators
            where indicator_name = 'chronic_absenteeism'
              and student_group = 'ALL'
              and change < 0
            """
        ).fetchall()
        return [row[0] for row in rows if row[0]]
    finally:
        connection.close()


def llm_rerank(query: str, rows: list[dict[str, Any]], model: str, limit: int) -> list[dict[str, Any]]:
    if not rows:
        return rows
    try:
        from openai import OpenAI
    except ImportError:
        return rows
    if not os.environ.get("OPENAI_API_KEY"):
        return rows

    payload = [
        {
            "rank": index,
            "chunk_id": row["chunk_id"],
            "district": row.get("district"),
            "section": row.get("section_path"),
            "pages": f"{row.get('page_start')}-{row.get('page_end')}",
            "text": (row.get("body_text") or row.get("search_text") or "")[:1400],
        }
        for index, row in enumerate(rows[:40], start=1)
    ]
    prompt = {
        "query": query,
        "instruction": (
            "Return JSON only: a list of objects with chunk_id, score from 0 to 100, "
            "and rationale. Score relevance to the query, district-authored signal, "
            "and usefulness as cited LCAP evidence."
        ),
        "candidates": payload,
    }
    client = OpenAI()
    text = ""
    try:
        if hasattr(client, "responses"):
            response = client.responses.create(model=model, input=json.dumps(prompt, ensure_ascii=False))
            text = getattr(response, "output_text", "") or ""
        else:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You rerank retrieval results and return JSON only."},
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
                temperature=0,
            )
            text = response.choices[0].message.content or ""
        parsed = json.loads(text)
    except Exception:
        return rows

    scores = {
        item.get("chunk_id"): {
            "rerank_score": float(item.get("score") or 0),
            "rerank_rationale": str(item.get("rationale") or ""),
        }
        for item in parsed
        if isinstance(item, dict) and item.get("chunk_id")
    }
    for row in rows:
        row.update(scores.get(row["chunk_id"], {"rerank_score": 0.0, "rerank_rationale": ""}))
    rows.sort(
        key=lambda item: (
            -float(item.get("rerank_score") or 0),
            -float(item.get("rrf_score") or 0),
            item.get("chunk_id", ""),
        )
    )
    return rows[:limit]


def search_lcap_narratives(
    query: str,
    *,
    filters: dict[str, Any] | None = None,
    limit: int = 10,
    dense_k: int = 80,
    bm25_k: int = 80,
    db_path: Path = DEFAULT_DB_PATH,
    chroma_dir: Path = DEFAULT_CHROMA_DIR,
    collection: str = DEFAULT_COLLECTION,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    dimensions: int | None = None,
    bm25_only: bool = False,
    analytics_db: Path = DEFAULT_ANALYTICS_DB,
    dashboard_cohort: str | None = None,
    rerank: str = "none",
    rerank_model: str | None = None,
) -> list[dict[str, Any]]:
    filters = filters or {}
    cds_codes = None
    if dashboard_cohort == "declining_chronic_absenteeism":
        cds_codes = fetch_declining_chronic_absenteeism_cds(analytics_db)

    connection = connect_db(db_path)
    try:
        bm25_results = bm25_search(connection, query, filters, cds_codes, bm25_k)
        dense_results: list[dict[str, Any]] = []
        if not bm25_only:
            try:
                dense_results = dense_search(
                    query,
                    filters,
                    cds_codes,
                    chroma_dir,
                    collection,
                    embedding_model,
                    dimensions,
                    dense_k,
                )
            except Exception as error:
                if not bm25_results:
                    raise
                print(f"warning: dense search skipped: {error}")
        fused = rrf_fuse(connection, bm25_results, dense_results)
    finally:
        connection.close()

    if rerank == "llm":
        fused = llm_rerank(
            query,
            fused,
            rerank_model or os.environ.get("OPENAI_RERANK_MODEL", "gpt-4o-mini"),
            limit,
        )
    return fused[:limit]


def get_chunk(
    chunk_id: str,
    *,
    db_path: Path = DEFAULT_DB_PATH,
    include_neighbors: bool = False,
) -> dict[str, Any] | None:
    connection = connect_db(db_path)
    try:
        row = connection.execute("select * from chunks where chunk_id = ?", (chunk_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        if include_neighbors:
            neighbor_ids = [value for value in [result.get("prev_chunk_id"), result.get("next_chunk_id")] if value]
            result["neighbors"] = list(load_chunks(connection, neighbor_ids).values())
        return result
    finally:
        connection.close()


def collection_stats(db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    connection = connect_db(db_path)
    try:
        total = connection.execute("select count(*) from chunks").fetchone()[0]
        sections = connection.execute("select count(distinct section_id) from chunks").fetchone()[0]
        districts = connection.execute("select count(distinct cds_code) from chunks").fetchone()[0]
        by_section = [
            dict(row)
            for row in connection.execute(
                "select section_type, count(*) chunks from chunks group by section_type order by chunks desc"
            )
        ]
        return {"chunks": total, "sections": sections, "districts": districts, "by_section_type": by_section}
    finally:
        connection.close()


def snippet(text: str, max_chars: int = 360) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def format_markdown(rows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for index, row in enumerate(rows, start=1):
        pages = row.get("page_start")
        if row.get("page_end") and row.get("page_end") != pages:
            pages = f"{pages}-{row.get('page_end')}"
        lines.append(
            f"{index}. {row.get('district', '')} | {row.get('section_path', '')} | p. {pages} | "
            f"RRF {float(row.get('rrf_score') or 0):.4f}"
        )
        lines.append(f"   chunk_id: {row.get('chunk_id')}")
        lines.append(f"   {snippet(row.get('body_text') or row.get('search_text') or '')}")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", nargs="?", help="Natural-language retrieval query.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--chroma-dir", type=Path, default=DEFAULT_CHROMA_DIR)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--analytics-db", type=Path, default=DEFAULT_ANALYTICS_DB)
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--dense-k", type=int, default=80)
    parser.add_argument("--bm25-k", type=int, default=80)
    parser.add_argument("--bm25-only", action="store_true")
    parser.add_argument("--rerank", choices=["none", "llm"], default="none")
    parser.add_argument("--rerank-model")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of markdown.")
    parser.add_argument("--county")
    parser.add_argument("--district")
    parser.add_argument("--cds-code")
    parser.add_argument("--school-year")
    parser.add_argument("--section-type")
    parser.add_argument("--chunk-kind")
    parser.add_argument("--goal-number")
    parser.add_argument(
        "--declining-chronic-absenteeism",
        action="store_true",
        help="Restrict search to districts whose Dashboard chronic absenteeism change is negative.",
    )
    parser.add_argument("--stats", action="store_true", help="Print collection stats and exit.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.stats:
        print(json.dumps(collection_stats(args.db_path), indent=2))
        return
    if not args.query:
        raise SystemExit("Provide a query or use --stats.")
    filters = {
        "county": args.county,
        "district": args.district,
        "cds_code": args.cds_code,
        "school_year": args.school_year,
        "section_type": args.section_type,
        "chunk_kind": args.chunk_kind,
        "goal_number": args.goal_number,
    }
    rows = search_lcap_narratives(
        args.query,
        filters={key: value for key, value in filters.items() if value not in (None, "")},
        limit=args.limit,
        dense_k=args.dense_k,
        bm25_k=args.bm25_k,
        db_path=args.db_path,
        chroma_dir=args.chroma_dir,
        collection=args.collection,
        embedding_model=args.embedding_model,
        dimensions=args.dimensions,
        bm25_only=args.bm25_only,
        analytics_db=args.analytics_db,
        dashboard_cohort="declining_chronic_absenteeism" if args.declining_chronic_absenteeism else None,
        rerank=args.rerank,
        rerank_model=args.rerank_model,
    )
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    else:
        print(format_markdown(rows))


if __name__ == "__main__":
    main()
