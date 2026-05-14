#!/usr/bin/env python3
"""Validate extracted LCAP narrative chunks before embedding."""

from __future__ import annotations

import argparse
import collections
import csv
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHUNKS_PATH = ROOT / "outputs" / "rag" / "2025" / "chunks.jsonl"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "rag" / "2025" / "qa"
DEFAULT_EXTRACTION_SUMMARY = ROOT / "outputs" / "rag" / "2025" / "extraction_summary.csv"

TEMPLATE_MARKERS = (
    "local control and accountability plan instructions",
    "lcap instructions",
    "instructions: introduction",
    "requirements and instructions",
    "for additional questions or technical assistance",
    "the lcap template must be completed by all leas",
    "california department of education november",
    "california department of education, july",
)

TABLE_MARKERS = (
    "action # title",
    "action # total funds",
    "contributing actions table",
    "annual update table",
    "lcff carryover",
    "scope unduplicated student group",
    "prior action/service title",
    "local control and accountability plan template action #",
)

SOFT_TABLE_MARKERS = (
    "goal # description",
    "metric #",
    "year 1 outcome",
    "year 2 outcome",
    "target for year 3",
    "planned percentage of improved services",
    "identified need(s) how the action(s)",
    "metric(s) to monitor",
)

BUDGET_MARKERS = (
    "total projected lcff supplemental",
    "projected additional 15 percent",
    "required percentage to increase or improve services",
    "total planned expenditures",
    "total estimated actual expenditures",
)

WANTED_SIGNAL_MARKERS = (
    "chronic absenteeism",
    "attendance",
    "re-engagement",
    "home visit",
    "sarb",
    "family",
    "community schools",
    "implementation challenges",
    "effectiveness",
    "educational partners",
    "equity multiplier",
    "student support",
    "mental health",
    "english learner",
)


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def estimate_tokens(text: str) -> int:
    return max(1, int(len(text) / 4))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def marker_hits(text: str, markers: tuple[str, ...]) -> list[str]:
    lowered = compact_text(text).casefold()
    return [marker for marker in markers if marker in lowered]


def line_table_score(text: str) -> float:
    lines = [compact_text(line) for line in text.split("\n") if compact_text(line)]
    if not lines:
        return 1.0
    tableish = 0
    for line in lines:
        lowered = line.casefold()
        digit_ratio = sum(char.isdigit() for char in line) / max(len(line), 1)
        if (
            any(marker in lowered for marker in TABLE_MARKERS + SOFT_TABLE_MARKERS)
            or digit_ratio > 0.22
            or (("$" in line or "%" in line) and len(line.split()) <= 14)
            or (len(lines) >= 5 and len(line.split()) <= 3)
        ):
            tableish += 1
    return tableish / len(lines)


def classify_issues(chunk: dict[str, Any]) -> list[str]:
    text = chunk.get("body_text") or chunk.get("search_text") or ""
    issues: list[str] = []
    if marker_hits(text, TEMPLATE_MARKERS):
        issues.append("template_marker")
    budget_hits = marker_hits(text, BUDGET_MARKERS)
    if budget_hits:
        issues.append("budget_marker")
    table_score = line_table_score(text)
    table_hits = [
        marker
        for marker in marker_hits(text, TABLE_MARKERS)
        if marker != "contributing actions table" or table_score >= 0.2
    ]
    if table_hits:
        issues.append("table_marker")
    line_count = len([line for line in text.split("\n") if compact_text(line)])
    if line_count >= 4 and table_score >= 0.42:
        issues.append("table_like")
    if "budget_marker" in issues and "table_like" not in issues and "table_marker" not in issues:
        issues.remove("budget_marker")
    if "table_like" in issues and not table_hits and not marker_hits(text, TEMPLATE_MARKERS):
        # Many LCAP narrative cells are list-heavy because they cite student groups,
        # metrics, and planned supports. Flag hard failures only when structural
        # table/template markers remain.
        issues.remove("table_like")
    if estimate_tokens(text) < 45:
        issues.append("too_short")
    if estimate_tokens(text) > 1100:
        issues.append("too_long")
    if not chunk.get("district") or not chunk.get("section_path") or not chunk.get("page_start"):
        issues.append("missing_citation_metadata")
    if float(chunk.get("authored_confidence") or 0) < 0.75:
        issues.append("low_confidence")
    return issues


def issue_row(chunk: dict[str, Any], issues: list[str]) -> dict[str, Any]:
    text = chunk.get("body_text") or chunk.get("search_text") or ""
    return {
        "chunk_id": chunk.get("chunk_id", ""),
        "cds_code": chunk.get("cds_code", ""),
        "district": chunk.get("district", ""),
        "section_type": chunk.get("section_type", ""),
        "section_path": chunk.get("section_path", ""),
        "chunk_kind": chunk.get("chunk_kind", ""),
        "page_start": chunk.get("page_start", ""),
        "page_end": chunk.get("page_end", ""),
        "token_count": chunk.get("token_count", ""),
        "authored_confidence": chunk.get("authored_confidence", ""),
        "issues": ";".join(issues),
        "template_markers": ";".join(marker_hits(text, TEMPLATE_MARKERS)),
        "table_markers": ";".join(marker_hits(text, TABLE_MARKERS)),
        "soft_table_markers": ";".join(marker_hits(text, SOFT_TABLE_MARKERS)),
        "budget_markers": ";".join(marker_hits(text, BUDGET_MARKERS)),
        "table_score": round(line_table_score(text), 3),
        "snippet": compact_text(text)[:500],
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sample_rows(chunks: list[dict[str, Any]], limit_per_bucket: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = collections.defaultdict(list)
    for chunk in chunks:
        key = (chunk.get("district", ""), chunk.get("section_type", ""))
        grouped[key].append(chunk)
    rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        for chunk in grouped[key][:limit_per_bucket]:
            rows.append(
                issue_row(
                    chunk,
                    ["sample"],
                )
            )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-path", type=Path, default=DEFAULT_CHUNKS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--extraction-summary", type=Path, default=DEFAULT_EXTRACTION_SUMMARY)
    parser.add_argument("--max-bad-rate", type=float, default=0.03)
    parser.add_argument("--max-table-like-rate", type=float, default=0.02)
    parser.add_argument("--sample-per-bucket", type=int, default=3)
    parser.add_argument("--strict", action="store_true", help="Exit nonzero when thresholds fail.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    chunks = read_jsonl(args.chunks_path)
    issue_rows: list[dict[str, Any]] = []
    issue_counter: collections.Counter[str] = collections.Counter()
    hard_issue_rows: list[dict[str, Any]] = []
    section_counter: collections.Counter[str] = collections.Counter()
    kind_counter: collections.Counter[str] = collections.Counter()
    district_counter: collections.Counter[str] = collections.Counter()
    wanted_signal_counter: collections.Counter[str] = collections.Counter()
    chunk_cdss: set[str] = set()

    for chunk in chunks:
        chunk_cdss.add(str(chunk.get("cds_code", "")))
        section_counter[chunk.get("section_type", "")] += 1
        kind_counter[chunk.get("chunk_kind", "")] += 1
        district_counter[chunk.get("district", "")] += 1
        for marker in marker_hits(chunk.get("body_text", ""), WANTED_SIGNAL_MARKERS):
            wanted_signal_counter[marker] += 1
        issues = classify_issues(chunk)
        if issues:
            row = issue_row(chunk, issues)
            issue_rows.append(row)
            if any(issue in issues for issue in ("template_marker", "budget_marker", "table_marker", "missing_citation_metadata")):
                hard_issue_rows.append(row)
            issue_counter.update(issues)

    table_like_count = issue_counter.get("table_marker", 0)
    bad_rate = len(issue_rows) / max(len(chunks), 1)
    hard_bad_rate = len(hard_issue_rows) / max(len(chunks), 1)
    table_like_rate = table_like_count / max(len(chunks), 1)
    extraction_rows = read_csv(args.extraction_summary)
    no_chunk_rows = [row for row in extraction_rows if row.get("cds_code", "") not in chunk_cdss]
    summary = {
        "chunks": len(chunks),
        "documents": len(extraction_rows) or None,
        "documents_without_chunks": len(no_chunk_rows),
        "districts": len(district_counter),
        "sections": dict(section_counter.most_common()),
        "chunk_kinds": dict(kind_counter.most_common()),
        "issue_counts": dict(issue_counter.most_common()),
        "issue_chunk_count": len(issue_rows),
        "hard_issue_chunk_count": len(hard_issue_rows),
        "bad_rate": round(bad_rate, 4),
        "hard_bad_rate": round(hard_bad_rate, 4),
        "table_like_rate": round(table_like_rate, 4),
        "wanted_signal_counts": dict(wanted_signal_counter.most_common()),
        "pass": hard_bad_rate <= args.max_bad_rate and table_like_rate <= args.max_table_like_rate,
        "thresholds": {
            "max_bad_rate": args.max_bad_rate,
            "max_table_like_rate": args.max_table_like_rate,
        },
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "validation_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_csv(args.output_dir / "issue_chunks.csv", issue_rows)
    write_csv(args.output_dir / "hard_issue_chunks.csv", hard_issue_rows)
    write_csv(args.output_dir / "no_chunk_documents.csv", no_chunk_rows)
    write_csv(args.output_dir / "review_samples.csv", sample_rows(chunks, args.sample_per_bucket))
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    if args.strict and not summary["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
