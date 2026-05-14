#!/usr/bin/env python3
"""Build a local Chroma + SQLite BM25 retrieval index for LCAP chunks."""

from __future__ import annotations

import argparse
from array import array
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAG_DIR = ROOT / "outputs" / "rag" / "2025"
DEFAULT_CHUNKS_PATH = DEFAULT_RAG_DIR / "chunks.jsonl"
DEFAULT_DB_PATH = DEFAULT_RAG_DIR / "lcap_retrieval.sqlite"
DEFAULT_CHROMA_DIR = DEFAULT_RAG_DIR / "chroma"
DEFAULT_COLLECTION = "lcap_narrative_chunks"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"


CHUNK_FIELDS = [
    "chunk_id",
    "section_id",
    "cds_code",
    "county",
    "district",
    "school_year",
    "source_path",
    "pdf_url",
    "page_start",
    "page_end",
    "section_type",
    "section_path",
    "prompt_label",
    "goal_number",
    "action_number",
    "chunk_kind",
    "chunk_index",
    "token_count",
    "text_hash",
    "authored_confidence",
    "body_text",
    "search_text",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_jsonl(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows.append(json.loads(line))
            if limit and len(rows) >= limit:
                break
    return rows


def dedupe_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        if chunk_id in seen:
            continue
        seen.add(chunk_id)
        unique.append(chunk)
    return unique


def sqlite_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if value is None:
        return ""
    return value


def embedding_key(model: str, dimensions: int | None, text_hash: str) -> str:
    return f"{model}:{dimensions or 'default'}:{text_hash}"


def pack_embedding(values: list[float]) -> bytes:
    return array("f", values).tobytes()


def unpack_embedding(blob: bytes) -> list[float]:
    values = array("f")
    values.frombytes(blob)
    return list(values)


def init_sqlite(db_path: Path, chunks: list[dict[str, Any]], rebuild: bool) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("pragma journal_mode=wal")
    connection.execute("pragma synchronous=normal")
    connection.execute(
        """
        create table if not exists chunks (
          chunk_id text primary key,
          section_id text,
          cds_code text,
          county text,
          district text,
          school_year text,
          source_path text,
          pdf_url text,
          page_start integer,
          page_end integer,
          section_type text,
          section_path text,
          prompt_label text,
          goal_number text,
          action_number text,
          chunk_kind text,
          chunk_index integer,
          token_count integer,
          text_hash text,
          authored_confidence real,
          body_text text,
          search_text text,
          prev_chunk_id text,
          next_chunk_id text
        )
        """
    )
    connection.execute(
        """
        create virtual table if not exists chunks_fts using fts5(
          chunk_id unindexed,
          search_text,
          district,
          section_type,
          tokenize='porter unicode61'
        )
        """
    )
    connection.execute(
        """
        create table if not exists embedding_cache (
          cache_key text primary key,
          model text not null,
          dimensions integer,
          text_hash text not null,
          embedding blob not null,
          created_at text not null,
          token_count integer
        )
        """
    )
    connection.execute(
        """
        create table if not exists embedding_runs (
          run_id integer primary key autoincrement,
          created_at text not null,
          model text not null,
          dimensions integer,
          chunk_count integer not null,
          embedded_count integer not null,
          skipped_count integer not null,
          chroma_dir text,
          collection text
        )
        """
    )
    connection.execute("delete from chunks")
    connection.execute("delete from chunks_fts")

    neighbor_by_id = compute_neighbors(chunks)
    rows = []
    fts_rows = []
    for chunk in chunks:
        prev_chunk_id, next_chunk_id = neighbor_by_id.get(chunk["chunk_id"], ("", ""))
        row = [sqlite_value(chunk.get(field)) for field in CHUNK_FIELDS]
        row.extend([prev_chunk_id, next_chunk_id])
        rows.append(tuple(row))
        fts_rows.append(
            (
                chunk.get("chunk_id", ""),
                chunk.get("search_text", ""),
                chunk.get("district", ""),
                chunk.get("section_type", ""),
            )
        )

    fields = CHUNK_FIELDS + ["prev_chunk_id", "next_chunk_id"]
    placeholders = ", ".join("?" for _ in fields)
    quoted_fields = ", ".join(f'"{field}"' for field in fields)
    connection.executemany(
        f"insert or replace into chunks ({quoted_fields}) values ({placeholders})",
        rows,
    )
    connection.executemany(
        "insert into chunks_fts (chunk_id, search_text, district, section_type) values (?, ?, ?, ?)",
        fts_rows,
    )
    for statement in [
        "create index if not exists idx_chunks_cds on chunks(cds_code)",
        "create index if not exists idx_chunks_district on chunks(district)",
        "create index if not exists idx_chunks_county on chunks(county)",
        "create index if not exists idx_chunks_section on chunks(section_type)",
        "create index if not exists idx_chunks_text_hash on chunks(text_hash)",
    ]:
        connection.execute(statement)
    connection.commit()
    return connection


def compute_neighbors(chunks: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for chunk in chunks:
        grouped.setdefault(chunk.get("section_id", ""), []).append(chunk)

    result: dict[str, tuple[str, str]] = {}
    for section_chunks in grouped.values():
        section_chunks.sort(
            key=lambda item: (
                int(item.get("page_start") or 0),
                int(item.get("chunk_index") or 0),
                item.get("chunk_id", ""),
            )
        )
        for index, chunk in enumerate(section_chunks):
            prev_id = section_chunks[index - 1]["chunk_id"] if index > 0 else ""
            next_id = section_chunks[index + 1]["chunk_id"] if index + 1 < len(section_chunks) else ""
            result[chunk["chunk_id"]] = (prev_id, next_id)
    return result


def cached_embeddings(
    connection: sqlite3.Connection,
    chunks: list[dict[str, Any]],
    model: str,
    dimensions: int | None,
) -> tuple[dict[str, list[float]], list[dict[str, Any]]]:
    keys = [embedding_key(model, dimensions, chunk["text_hash"]) for chunk in chunks]
    cached: dict[str, list[float]] = {}
    missing: list[dict[str, Any]] = []
    if keys:
        placeholders = ", ".join("?" for _ in keys)
        rows = connection.execute(
            f"select cache_key, embedding from embedding_cache where cache_key in ({placeholders})",
            keys,
        ).fetchall()
        cached = {row[0]: unpack_embedding(row[1]) for row in rows}

    for chunk in chunks:
        key = embedding_key(model, dimensions, chunk["text_hash"])
        if key not in cached:
            missing.append(chunk)
    return cached, missing


def create_embeddings(
    texts: list[str],
    model: str,
    dimensions: int | None,
) -> list[list[float]]:
    try:
        from openai import OpenAI
    except ImportError as error:  # pragma: no cover - import availability is environment-specific.
        raise RuntimeError("Install the `openai` package to build dense embeddings.") from error

    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required to create embeddings.")

    client = OpenAI()
    kwargs: dict[str, Any] = {"model": model, "input": texts}
    if dimensions:
        kwargs["dimensions"] = dimensions
    last_error: Exception | None = None
    for attempt in range(8):
        try:
            response = client.embeddings.create(**kwargs)
            return [item.embedding for item in response.data]
        except Exception as error:
            last_error = error
            wait_seconds = min(60, 2**attempt)
            print(f"embedding request failed ({type(error).__name__}); retrying in {wait_seconds}s", flush=True)
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def store_cache(
    connection: sqlite3.Connection,
    chunks: list[dict[str, Any]],
    embeddings: list[list[float]],
    model: str,
    dimensions: int | None,
) -> None:
    rows = []
    for chunk, embedding in zip(chunks, embeddings, strict=True):
        rows.append(
            (
                embedding_key(model, dimensions, chunk["text_hash"]),
                model,
                dimensions,
                chunk["text_hash"],
                pack_embedding(embedding),
                utc_now(),
                int(chunk.get("token_count") or 0),
            )
        )
    connection.executemany(
        """
        insert or replace into embedding_cache
        (cache_key, model, dimensions, text_hash, embedding, created_at, token_count)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()


def chroma_metadata(chunk: dict[str, Any], model: str, dimensions: int | None) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for field in [
        "section_id",
        "cds_code",
        "county",
        "district",
        "school_year",
        "source_path",
        "pdf_url",
        "page_start",
        "page_end",
        "section_type",
        "section_path",
        "prompt_label",
        "goal_number",
        "action_number",
        "chunk_kind",
        "chunk_index",
        "token_count",
        "text_hash",
        "authored_confidence",
    ]:
        value = chunk.get(field)
        if value is None:
            value = ""
        metadata[field] = value
    metadata["embedding_model"] = model
    metadata["embedding_dimensions"] = dimensions or 0
    return metadata


def default_dimensions(model: str, dimensions: int | None) -> int:
    if dimensions:
        return dimensions
    if model == "text-embedding-3-large":
        return 3072
    return 1536


def upsert_chroma(
    chunks: list[dict[str, Any]],
    embeddings_by_chunk: dict[str, list[float]],
    chroma_dir: Path,
    collection_name: str,
    model: str,
    dimensions: int | None,
    rebuild: bool,
    batch_size: int,
) -> None:
    try:
        import chromadb
    except ImportError as error:  # pragma: no cover - import availability is environment-specific.
        raise RuntimeError("Install the `chromadb` package to build the dense retrieval store.") from error

    chroma_dir.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(chroma_dir))
    if rebuild:
        try:
            client.delete_collection(collection_name)
        except Exception:
            pass
    collection = client.get_or_create_collection(
        collection_name,
        metadata={
            "hnsw:space": "cosine",
            "embedding_model": model,
            "embedding_dimensions": default_dimensions(model, dimensions),
        },
    )

    batch_ids: list[str] = []
    batch_embeddings: list[list[float]] = []
    batch_documents: list[str] = []
    batch_metadatas: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal batch_ids, batch_embeddings, batch_documents, batch_metadatas
        if not batch_ids:
            return
        collection.upsert(
            ids=batch_ids,
            embeddings=batch_embeddings,
            documents=batch_documents,
            metadatas=batch_metadatas,
        )
        batch_ids = []
        batch_embeddings = []
        batch_documents = []
        batch_metadatas = []

    for chunk in chunks:
        chunk_id = chunk["chunk_id"]
        embedding = embeddings_by_chunk.get(chunk_id)
        if not embedding:
            continue
        batch_ids.append(chunk_id)
        batch_embeddings.append(embedding)
        batch_documents.append(chunk.get("search_text", ""))
        batch_metadatas.append(chroma_metadata(chunk, model, dimensions))
        if len(batch_ids) >= batch_size:
            flush()
    flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-path", type=Path, default=DEFAULT_CHUNKS_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--chroma-dir", type=Path, default=DEFAULT_CHROMA_DIR)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--embedding-model", default=DEFAULT_EMBEDDING_MODEL)
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--batch-size", type=int, default=96)
    parser.add_argument("--batch-delay", type=float, default=1.6, help="Seconds to wait between embedding batches.")
    parser.add_argument("--limit", type=int, help="Limit chunks for smoke testing.")
    parser.add_argument("--rebuild", action="store_true", help="Recreate SQLite rows and Chroma collection.")
    parser.add_argument("--bm25-only", action="store_true", help="Build SQLite/FTS only; skip Chroma and embeddings.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    chunks = read_jsonl(args.chunks_path, args.limit)
    if not chunks:
        raise SystemExit(f"No chunks found at {args.chunks_path}")
    raw_chunk_count = len(chunks)
    chunks = dedupe_chunks(chunks)
    if len(chunks) != raw_chunk_count:
        print(f"deduplicated chunks by chunk_id: {raw_chunk_count} -> {len(chunks)}", flush=True)

    connection = init_sqlite(args.db_path, chunks, args.rebuild)
    embedded_count = 0
    skipped_count = len(chunks)
    try:
        if not args.bm25_only:
            cached, missing = cached_embeddings(connection, chunks, args.embedding_model, args.dimensions)
            embeddings_by_cache_key = dict(cached)
            for start in range(0, len(missing), args.batch_size):
                batch = missing[start : start + args.batch_size]
                texts = [chunk.get("search_text", "") for chunk in batch]
                embeddings = create_embeddings(texts, args.embedding_model, args.dimensions)
                store_cache(connection, batch, embeddings, args.embedding_model, args.dimensions)
                for chunk, embedding in zip(batch, embeddings, strict=True):
                    embeddings_by_cache_key[
                        embedding_key(args.embedding_model, args.dimensions, chunk["text_hash"])
                    ] = embedding
                embedded_count += len(batch)
                print(f"embedded {min(start + len(batch), len(missing))}/{len(missing)} missing chunks", flush=True)
                if args.batch_delay > 0 and start + len(batch) < len(missing):
                    time.sleep(args.batch_delay)

            embeddings_by_chunk = {
                chunk["chunk_id"]: embeddings_by_cache_key[
                    embedding_key(args.embedding_model, args.dimensions, chunk["text_hash"])
                ]
                for chunk in chunks
                if embedding_key(args.embedding_model, args.dimensions, chunk["text_hash"]) in embeddings_by_cache_key
            }
            skipped_count = len(chunks) - embedded_count
            upsert_chroma(
                chunks,
                embeddings_by_chunk,
                args.chroma_dir,
                args.collection,
                args.embedding_model,
                args.dimensions,
                args.rebuild,
                args.batch_size,
            )

        connection.execute(
            """
            insert into embedding_runs
            (created_at, model, dimensions, chunk_count, embedded_count, skipped_count, chroma_dir, collection)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now(),
                args.embedding_model,
                args.dimensions,
                len(chunks),
                embedded_count,
                skipped_count,
                "" if args.bm25_only else str(args.chroma_dir),
                "" if args.bm25_only else args.collection,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    print(
        json.dumps(
            {
                "chunks": len(chunks),
                "db_path": str(args.db_path),
                "chroma_dir": "" if args.bm25_only else str(args.chroma_dir),
                "embedded": embedded_count,
                "embedding_model": args.embedding_model,
                "dimensions": args.dimensions or "default",
                "bm25_only": args.bm25_only,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
