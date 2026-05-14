#!/usr/bin/env python3
"""MCP server exposing local LCAP narrative retrieval tools to coding agents."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as error:  # pragma: no cover - import availability is environment-specific.
    raise SystemExit("Install the `mcp` package to run the LCAP MCP server.") from error

from search_lcap_retrieval import (
    DEFAULT_ANALYTICS_DB,
    DEFAULT_CHROMA_DIR,
    DEFAULT_COLLECTION,
    DEFAULT_DB_PATH,
    DEFAULT_EMBEDDING_MODEL,
    collection_stats,
    get_chunk,
    search_lcap_narratives,
)
from lcap_opportunities import find_opportunities, get_account_brief, topic_config


SERVER_NAME = "lcap-retrieval"


def parse_filters(filters: dict[str, Any] | str | None) -> dict[str, Any]:
    if filters is None:
        return {}
    if isinstance(filters, str):
        if not filters.strip():
            return {}
        return json.loads(filters)
    return {key: value for key, value in filters.items() if value not in (None, "")}


def district_context(cds_code: str, analytics_db: Path, retrieval_db: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"cds_code": cds_code}
    analytics = sqlite3.connect(analytics_db)
    analytics.row_factory = sqlite3.Row
    try:
        district = analytics.execute("select * from districts where cds_code = ?", (cds_code,)).fetchone()
        if district:
            result["district"] = dict(district)
        chronic = analytics.execute(
            """
            select *
            from dashboard_indicators
            where cds_code = ?
              and indicator_name = 'chronic_absenteeism'
              and student_group = 'ALL'
            limit 1
            """,
            (cds_code,),
        ).fetchone()
        if chronic:
            result["chronic_absenteeism"] = dict(chronic)
        funds = analytics.execute(
            """
            select
              count(*) action_count,
              round(sum(coalesce(total_funds, 0)), 0) total_lcap_action_funds
            from lcap_actions
            where cds_code = ?
            """,
            (cds_code,),
        ).fetchone()
        result["lcap_actions"] = dict(funds) if funds else {}
    finally:
        analytics.close()

    retrieval = sqlite3.connect(retrieval_db)
    retrieval.row_factory = sqlite3.Row
    try:
        rows = retrieval.execute(
            """
            select section_type, count(*) chunk_count
            from chunks
            where cds_code = ?
            group by section_type
            order by chunk_count desc
            """,
            (cds_code,),
        ).fetchall()
        result["narrative_chunks_by_section"] = [dict(row) for row in rows]
    finally:
        retrieval.close()
    return result


def build_server(args: argparse.Namespace) -> FastMCP:
    mcp = FastMCP(SERVER_NAME)

    @mcp.tool()
    def lcap_search_narratives(
        query: str,
        filters: dict[str, Any] | str | None = None,
        limit: int = 10,
        rerank: str = "none",
        include_context: bool = False,
    ) -> list[dict[str, Any]]:
        """Search section-tagged LCAP narrative chunks with hybrid retrieval."""
        rows = search_lcap_narratives(
            query,
            filters=parse_filters(filters),
            limit=limit,
            db_path=args.db_path,
            chroma_dir=args.chroma_dir,
            collection=args.collection,
            embedding_model=args.embedding_model,
            dimensions=args.dimensions,
            bm25_only=args.bm25_only,
            analytics_db=args.analytics_db,
            rerank=rerank,
            rerank_model=args.rerank_model,
        )
        if include_context:
            for row in rows:
                full = get_chunk(row["chunk_id"], db_path=args.db_path, include_neighbors=True)
                row["neighbors"] = (full or {}).get("neighbors", [])
        return rows

    @mcp.tool()
    def lcap_search_with_dashboard_cohort(
        query: str,
        dashboard_cohort: str = "declining_chronic_absenteeism",
        filters: dict[str, Any] | str | None = None,
        limit: int = 10,
        rerank: str = "none",
    ) -> list[dict[str, Any]]:
        """Search LCAP narratives after restricting to a Dashboard-derived district cohort."""
        return search_lcap_narratives(
            query,
            filters=parse_filters(filters),
            limit=limit,
            db_path=args.db_path,
            chroma_dir=args.chroma_dir,
            collection=args.collection,
            embedding_model=args.embedding_model,
            dimensions=args.dimensions,
            bm25_only=args.bm25_only,
            analytics_db=args.analytics_db,
            dashboard_cohort=dashboard_cohort,
            rerank=rerank,
            rerank_model=args.rerank_model,
        )

    @mcp.tool()
    def lcap_get_chunk(chunk_id: str, include_neighbors: bool = True) -> dict[str, Any] | None:
        """Fetch one chunk by ID, optionally with previous/next section neighbors."""
        return get_chunk(chunk_id, db_path=args.db_path, include_neighbors=include_neighbors)

    @mcp.tool()
    def lcap_get_district_context(cds_code: str) -> dict[str, Any]:
        """Fetch structured Dashboard/LCAP facts plus narrative chunk coverage for a district."""
        return district_context(cds_code, args.analytics_db, args.db_path)

    @mcp.tool()
    def lcap_find_opportunities(
        topic: str = "chronic_absenteeism",
        outcome_trend: str = "worsening",
        rank_by: str = "strict_action_funds",
        filters: dict[str, Any] | str | None = None,
        limit: int = 25,
        include_actions: bool = True,
        action_limit: int = 3,
    ) -> list[dict[str, Any]]:
        """Find ranked GTM opportunities by joining Dashboard outcomes to LCAP action budgets."""
        parsed_filters = parse_filters(filters)
        return find_opportunities(
            topic=topic,
            outcome_trend=outcome_trend,
            rank_by=rank_by,
            county=parsed_filters.get("county"),
            district=parsed_filters.get("district"),
            limit=limit,
            include_actions=include_actions,
            action_limit=action_limit,
            db_path=args.analytics_db,
        )

    @mcp.tool()
    def lcap_explain_account(
        cds_code: str,
        topic: str = "chronic_absenteeism",
        narrative_query: str | None = None,
        narrative_limit: int = 6,
        action_limit: int = 6,
        rerank: str = "none",
    ) -> dict[str, Any]:
        """Build an account brief with structured facts, action budgets, and narrative evidence."""
        config = topic_config(topic)
        query = narrative_query or config["default_narrative_query"]
        brief = get_account_brief(
            cds_code,
            topic=topic,
            db_path=args.analytics_db,
            action_limit=action_limit,
        )
        narratives = search_lcap_narratives(
            query,
            filters={"cds_code": cds_code},
            limit=narrative_limit,
            db_path=args.db_path,
            chroma_dir=args.chroma_dir,
            collection=args.collection,
            embedding_model=args.embedding_model,
            dimensions=args.dimensions,
            bm25_only=args.bm25_only,
            analytics_db=args.analytics_db,
            rerank=rerank,
            rerank_model=args.rerank_model,
        )
        return {"structured": brief, "narrative_evidence": narratives}

    @mcp.tool()
    def lcap_collection_stats() -> dict[str, Any]:
        """Return local retrieval collection counts and section distribution."""
        return collection_stats(args.db_path)

    return mcp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--chroma-dir", type=Path, default=DEFAULT_CHROMA_DIR)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--analytics-db", type=Path, default=DEFAULT_ANALYTICS_DB)
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--bm25-only", action="store_true")
    parser.add_argument("--rerank-model")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_server(args).run()


if __name__ == "__main__":
    main()
