#!/usr/bin/env python3
"""Extract goals, actions, and metrics from California LCAP PDFs."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pdfplumber
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parent.parent
LCAPS_DIR = ROOT / "lcaps"
OUTPUT_DIR = ROOT / "outputs" / "lcaps_json"
PER_LCAP_DIR = OUTPUT_DIR / "per_lcap"

GOAL_TABLE_HEADERS = ("Goal #", "Description", "Type of Goal")
ACTION_TABLE_HEADERS = ("Action #", "Title", "Description", "Total Funds", "Contributing")
METRIC_TABLE_HEADERS = ("Metric #", "Metric", "Baseline", "Year 1 Outcome")

GOAL_ID_RE = re.compile(r"^\d+(?:\.\d+)?[A-Za-z]?$")
RECORD_ID_RE = re.compile(r"^[A-Za-z]?\d+[A-Za-z]?(?:\([A-Za-z]?\d+[A-Za-z]?\)|\.[A-Za-z]?\d*[A-Za-z]?)*$")
YEAR_RE = re.compile(r"\b(20\d{2}-\d{2}|20\d{2}-20\d{2})\b")
MONEY_RE = re.compile(r"-?\$?\s*(\d[\d,]*(?:\.\d+)?)")
VALUE_TOKEN_RE = re.compile(r"(?<!\w)(?:\(?[-+]?\$?\d[\d,]*\.?\d*%?\)?)(?!\w)")
GOAL_NUMBER_TOKEN_RE = re.compile(
    r"(?i)(?:equity multiplier\s+)?(?:broad|focus)?\s*goal\s*(\d+(?:\.\d+)?[A-Za-z]?)"
)


@dataclass
class ParsedTable:
    kind: str
    rows: list[list[str]]


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    replacements = {
        "\xa0": " ",
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\u2022": "*",
        "\u00ad": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def flatten_row(row: list[str]) -> str:
    return " ".join(part for part in (normalize_text(cell) for cell in row) if part)


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", normalize_text(value)).strip()


def normalize_table(table: list[list[Any]]) -> list[list[str]]:
    width = max((len(row) for row in table), default=0)
    normalized: list[list[str]] = []
    for row in table:
        current = [normalize_text(cell) for cell in row]
        if len(current) < width:
            current.extend([""] * (width - len(current)))
        normalized.append(current)
    return normalized


def classify_table(table: list[list[str]]) -> str | None:
    joined = " | ".join(compact_text(flatten_row(row)) for row in table[:4])
    lowered = joined.lower()
    if "goal #" in lowered and "description" in lowered:
        return "goal"
    if "action #" in lowered and (
        ("action title" in lowered and ("total funds" in lowered or "planned expenditures" in lowered))
        or ("title" in lowered and "description" in lowered and "total funds" in lowered)
    ):
        return "action"
    if "metric" in lowered and "baseline" in lowered and ("year 1 outcome" in lowered or "year 1" in lowered):
        return "metric"
    return None


def row_is_header(row: list[str], kind: str) -> bool:
    cells = [compact_text(cell).lower() for cell in row if compact_text(cell)]
    if not cells:
        return True
    if kind == "goal":
        header_hits = sum(cell in {"goal #", "description", "type of goal"} for cell in cells)
        return header_hits >= 2
    if kind == "action":
        header_hits = 0
        for cell in cells:
            if cell in {"action #", "action title", "title", "description", "total funds", "contributing"}:
                header_hits += 1
            elif "planned expenditures" in cell or "contributing to increased or improved services" in cell:
                header_hits += 1
        return header_hits >= 3
    if kind == "metric":
        header_hits = 0
        for cell in cells:
            if cell in {"metric #", "metric", "baseline", "year 1 outcome", "year 2 outcome"}:
                header_hits += 1
            elif cell.startswith("target for year 3") or cell.startswith("desired outcome"):
                header_hits += 1
            elif cell.startswith("current difference"):
                header_hits += 1
        return header_hits >= 3
    return False


def append_multiline(target: dict[str, Any], field_names: list[str], row: list[str]) -> None:
    for index, field_name in enumerate(field_names):
        if index >= len(row):
            break
        value = row[index]
        if not value:
            continue
        existing = target.get(field_name, "")
        target[field_name] = f"{existing}\n{value}".strip() if existing else value


def metric_column_map(table: list[list[str]]) -> dict[str, int]:
    metric_name_candidates: list[int] = []
    baseline_idx = 2
    year_1_idx = 3
    year_2_idx = 4
    year_3_idx = 5
    current_diff_idx = 6

    for row in table[:4]:
        for index, cell in enumerate(row):
            lowered = compact_text(cell).lower()
            if not lowered:
                continue
            if lowered == "metric":
                metric_name_candidates.append(index)
            elif "current difference" in lowered:
                current_diff_idx = index
            elif lowered == "baseline" or lowered.startswith("baseline "):
                baseline_idx = index
            elif "year 1 outcome" in lowered:
                year_1_idx = index
            elif "year 2 outcome" in lowered:
                year_2_idx = index
            elif "target for year 3" in lowered:
                year_3_idx = index
            elif "desired outcome" in lowered:
                year_3_idx = index

    return {
        "metric_name": max(metric_name_candidates) if metric_name_candidates else 1,
        "baseline": baseline_idx,
        "year_1": year_1_idx,
        "year_2": year_2_idx,
        "year_3": year_3_idx,
        "current_diff": current_diff_idx,
    }


def project_metric_row(row: list[str], column_map: dict[str, int]) -> list[str]:
    metric_number = ""
    for cell in row:
        if RECORD_ID_RE.match(cell):
            metric_number = cell
            break

    def value_at(key: str) -> str:
        index = column_map[key]
        return row[index] if index < len(row) else ""

    return [
        metric_number,
        value_at("metric_name"),
        value_at("baseline"),
        value_at("year_1"),
        value_at("year_2"),
        value_at("year_3"),
        value_at("current_diff"),
    ]


def project_action_row(row: list[str]) -> list[str]:
    action_number = ""
    normalized_cells = [normalize_text(cell) for cell in row if normalize_text(cell)]
    for cell in normalized_cells:
        if RECORD_ID_RE.match(cell):
            action_number = cell
            break

    if not action_number:
        return ["", "", "\n".join(normalized_cells), "", ""]

    remaining = []
    removed_action_number = False
    for cell in normalized_cells:
        if not removed_action_number and cell == action_number:
            removed_action_number = True
            continue
        remaining.append(cell)

    contributing = ""
    if remaining and (parse_contributing(remaining[-1]) is not None or remaining[-1] in {"Y", "N"}):
        contributing = remaining.pop()

    total_funds = ""
    if remaining and parse_currency(remaining[-1]) is not None:
        total_funds = remaining.pop()

    title = remaining.pop(0) if remaining else ""
    description = "\n".join(remaining)

    return [action_number, title, description, total_funds, contributing]


def extract_goal_number(value: str) -> str | None:
    text = compact_text(value)
    if not text:
        return None
    if GOAL_ID_RE.fullmatch(text):
        return text

    match = GOAL_NUMBER_TOKEN_RE.search(text)
    if match:
        return match.group(1)

    match = re.fullmatch(r"(?i)goal\s+(\d+(?:\.\d+)?[A-Za-z]?)", text)
    if match:
        return match.group(1)

    return None


def goal_column_map(table: list[list[str]]) -> dict[str, int]:
    description_idx = 1
    goal_type_idx = 2

    for row in table[:4]:
        for index, cell in enumerate(row):
            lowered = compact_text(cell).lower()
            if not lowered:
                continue
            if lowered == "description":
                description_idx = index
            elif lowered == "type of goal":
                goal_type_idx = index

    return {
        "description": description_idx,
        "goal_type": goal_type_idx,
    }


def action_column_map(table: list[list[str]]) -> dict[str, int | None]:
    column_map: dict[str, int | None] = {
        "goal_number": None,
        "action_number": 0,
        "title": 1,
        "description": None,
        "contributing": None,
        "total_funds": None,
        "total_personnel": None,
        "total_non_personnel": None,
        "lcff_funds": None,
        "other_state_funds": None,
        "local_funds": None,
        "federal_funds": None,
    }

    for row in table[:4]:
        for index, cell in enumerate(row):
            lowered = compact_text(cell).lower()
            if not lowered:
                continue
            if lowered in {"goal #", "goal"}:
                column_map["goal_number"] = index
            elif "action #" in lowered:
                column_map["action_number"] = index
            elif "action title" in lowered:
                column_map["title"] = index
            elif lowered == "title":
                column_map["title"] = index
            elif lowered == "description":
                column_map["description"] = index
            elif "contributing to increased or improved services" in lowered or lowered == "contributing":
                column_map["contributing"] = index
            elif "total funds" in lowered:
                column_map["total_funds"] = index
            elif "planned expenditures" in lowered:
                column_map["total_funds"] = index
            elif lowered == "total personnel":
                column_map["total_personnel"] = index
            elif lowered == "total non-personnel":
                column_map["total_non_personnel"] = index
            elif lowered == "lcff funds":
                column_map["lcff_funds"] = index
            elif lowered == "other state funds":
                column_map["other_state_funds"] = index
            elif lowered == "local funds":
                column_map["local_funds"] = index
            elif lowered == "federal funds":
                column_map["federal_funds"] = index

    return column_map


def value_at(row: list[str], index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return row[index]


def first_currency_value(*values: str) -> str:
    for value in values:
        if parse_currency(value) is not None:
            return value
    return ""


def project_action_row_from_columns(row: list[str], column_map: dict[str, int | None]) -> tuple[str | None, list[str]]:
    goal_number = extract_goal_number(value_at(row, column_map["goal_number"]))
    action_number = normalize_text(value_at(row, column_map["action_number"]))
    title = normalize_text(value_at(row, column_map["title"]))
    description = normalize_text(value_at(row, column_map["description"]))
    total_funds = first_currency_value(
        value_at(row, column_map["total_funds"]),
        value_at(row, column_map["lcff_funds"]),
        value_at(row, column_map["total_personnel"]),
        value_at(row, column_map["total_non_personnel"]),
        value_at(row, column_map["other_state_funds"]),
        value_at(row, column_map["local_funds"]),
        value_at(row, column_map["federal_funds"]),
    )
    contributing = normalize_text(value_at(row, column_map["contributing"]))
    return goal_number, [action_number, title, description, total_funds, contributing]


def parse_currency(value: str) -> float | None:
    match = MONEY_RE.search(value.replace("(", "-").replace(")", ""))
    if not match:
        return None
    numeric = match.group(1).replace(",", "")
    if not numeric:
        return None
    try:
        return float(numeric)
    except ValueError:
        return None


def parse_contributing(value: str) -> bool | None:
    lowered = value.lower()
    if "yes" in lowered:
        return True
    if "no" in lowered:
        return False
    return None


def parse_numeric_tokens(value: str) -> list[float]:
    parsed: list[float] = []
    for token in VALUE_TOKEN_RE.findall(value):
        normalized = token.replace("$", "").replace(",", "").strip()
        negative = normalized.startswith("(") and normalized.endswith(")")
        normalized = normalized.strip("()")
        normalized = normalized.rstrip("%")
        if not normalized or normalized in {"-", "+", "."}:
            continue
        try:
            number = float(normalized)
        except ValueError:
            continue
        parsed.append(-number if negative else number)
    return parsed


def strip_continued_marker(value: str) -> str:
    text = normalize_text(value)
    text = re.sub(r"\(\s*continued\s*\)", "", text, flags=re.I)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def merge_text(existing: str, incoming: str) -> str:
    existing = normalize_text(existing)
    incoming = normalize_text(incoming)
    if not incoming:
        return existing
    if not existing:
        return incoming
    if incoming == existing or incoming in existing:
        return existing
    if existing in incoming:
        return incoming
    return f"{existing}\n{incoming}".strip()


def split_label_value(line: str) -> tuple[str | None, str]:
    for delimiter in (" : ", ": ", " - ", " – ", " — "):
        if delimiter in line:
            left, right = line.split(delimiter, 1)
            return normalize_text(left) or None, normalize_text(right)
    match = re.match(r"^([^:]+):\s*(.+)$", line)
    if match:
        return normalize_text(match.group(1)) or None, normalize_text(match.group(2))
    match = re.match(r"^(.+?)\s+-\s+(.+)$", line)
    if match:
        return normalize_text(match.group(1)) or None, normalize_text(match.group(2))
    return None, normalize_text(line)


def measurement_payload(raw_value: str) -> dict[str, Any]:
    raw = normalize_text(raw_value)
    if not raw:
        return {"raw": "", "context_lines": [], "entries": []}

    lines = [normalize_text(line) for line in raw.split("\n") if normalize_text(line)]
    context_lines: list[str] = []
    entries: list[dict[str, Any]] = []

    for index, line in enumerate(lines):
        label, value = split_label_value(line)
        looks_like_context = (
            index == 0
            and len(lines) > 1
            and label is None
            and "%" not in value
            and not re.search(r"\b(ALL|EL|SED|SWD|FY|AA|AS|HI|HOM|LTEL|FOS)\b", value)
        )
        if looks_like_context:
            context_lines.append(value)
            continue

        entries.append(
            {
                "label": label,
                "value": value,
                "numeric_values": parse_numeric_tokens(value),
            }
        )

    return {"raw": raw, "context_lines": context_lines, "entries": entries}


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")


def collect_page_text(path: Path) -> dict[int, str]:
    reader = PdfReader(str(path))
    page_text: dict[int, str] = {}
    for page_number, page in enumerate(reader.pages, start=1):
        try:
            page_text[page_number] = normalize_text(page.extract_text() or "")
        except Exception:
            page_text[page_number] = ""
    return page_text


def collect_candidate_pages(page_text: dict[int, str]) -> set[int]:
    candidates: set[int] = set()
    page_numbers = sorted(page_text)
    last_page = page_numbers[-1] if page_numbers else 0

    for page_number, text in page_text.items():
        if not text:
            continue

        compact = compact_text(text).lower()
        has_goal_header = "goal #" in compact and "description" in compact
        has_metric_header = "metric" in compact and "baseline" in compact and (
            "year 1 outcome" in compact or "year 1" in compact
        )
        has_action_header = "action #" in compact and (
            ("action title" in compact and ("total funds" in compact or "planned expenditures" in compact))
            or ("title" in compact and "description" in compact and "total funds" in compact)
        )

        if has_goal_header or has_metric_header or has_action_header:
            for neighbor in (page_number - 1, page_number, page_number + 1):
                if 1 <= neighbor <= last_page:
                    candidates.add(neighbor)

    return candidates or set(page_numbers)


def extract_metadata(path: Path, page_text: dict[int, str] | None = None) -> dict[str, Any]:
    page_text = page_text or collect_page_text(path)
    text = normalize_text("\n".join(page_text.get(index, "") for index in range(1, min(4, len(page_text)) + 1)))

    lea_name = path.stem
    school_year = None

    match = re.search(r"Local Educational Agency \(LEA\) Name:\s*(.+?)\s*CDS Code:", text, re.S)
    if match:
        lea_name = normalize_text(match.group(1))
    else:
        match = re.search(r"Local Control and Accountability Plan for\s+(.+?)\s+Page \d+ of \d+", text, re.S)
        if match:
            lea_name = normalize_text(match.group(1))

    match = re.search(r"School Year:\s*([0-9-]+)", text)
    if match:
        school_year = normalize_text(match.group(1))
    else:
        match = YEAR_RE.search(text)
        if match:
            school_year = match.group(1)

    return {
        "district_name": lea_name,
        "school_year": school_year,
    }


def merge_records(records: list[dict[str, Any]], id_key: str, text_key: str) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []
    for record in records:
        key = (record.get(id_key, ""), record.get(text_key, ""))
        if key not in merged:
            merged[key] = record
            order.append(key)
            continue

        existing = merged[key]
        for field, value in record.items():
            if field == "source_pages":
                existing[field] = sorted(set(existing.get(field, []) + value))
            elif isinstance(value, str) and value and not existing.get(field):
                existing[field] = value
            elif isinstance(value, (int, float)) and existing.get(field) is None:
                existing[field] = value
    return [merged[key] for key in order]


def merge_actions(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []

    for record in records:
        key = record.get("action_number", "")
        record["title"] = strip_continued_marker(record.get("title", ""))
        normalized_title = record["title"] or normalize_text(record.get("description", "")).split("\n")[0]
        merge_key = (key, normalized_title)
        if merge_key not in merged:
            merged[merge_key] = record
            order.append(merge_key)
            continue

        existing = merged[merge_key]
        existing["title"] = merge_text(existing.get("title", ""), record.get("title", ""))
        existing["description"] = merge_text(existing.get("description", ""), record.get("description", ""))
        existing["total_funds_raw"] = existing.get("total_funds_raw") or record.get("total_funds_raw", "")
        existing["contributing_raw"] = existing.get("contributing_raw") or record.get("contributing_raw", "")
        existing["total_funds"] = existing.get("total_funds")
        if existing["total_funds"] is None:
            existing["total_funds"] = record.get("total_funds")
        existing["contributing"] = existing.get("contributing")
        if existing["contributing"] is None:
            existing["contributing"] = record.get("contributing")
        existing["source_pages"] = sorted(set(existing.get("source_pages", []) + record.get("source_pages", [])))

    return [merged[key] for key in order]


def parse_goal_table(table: list[list[str]], page_number: int) -> list[dict[str, Any]]:
    goals: list[dict[str, Any]] = []
    column_map = goal_column_map(table)
    for row in table:
        if row_is_header(row, "goal"):
            continue
        if not row:
            continue
        goal_number = extract_goal_number(row[0])
        if not goal_number:
            continue
        description = value_at(row, column_map["description"])
        goal_type = value_at(row, column_map["goal_type"])
        goals.append(
            {
                "goal_number": goal_number,
                "description": description,
                "goal_type": goal_type,
                "source_pages": [page_number],
                "metrics": [],
                "actions": [],
            }
        )
    return goals


def parse_metric_table(
    table: list[list[str]],
    page_number: int,
    current_goal: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    field_names = [
        "metric_number",
        "metric_name",
        "baseline_raw",
        "year_1_outcome_raw",
        "year_2_outcome_raw",
        "year_3_target_raw",
        "current_difference_from_baseline_raw",
    ]
    metrics: list[dict[str, Any]] = []
    column_map = metric_column_map(table)
    synthetic_index = len(current_goal["metrics"]) + 1 if current_goal else 1

    for row in table:
        if row_is_header(row, "metric"):
            continue
        row = project_metric_row(row, column_map)
        if not any(row):
            continue

        has_explicit_id = bool(row[0] and RECORD_ID_RE.match(row[0]))
        starts_new_metric = has_explicit_id or (
            not row[0]
            and bool(row[1])
            and any(row[2:])
        )

        if starts_new_metric:
            metric = {field: "" for field in field_names}
            if not row[0]:
                goal_number = current_goal["goal_number"] if current_goal else "goal"
                row[0] = f"{goal_number}.metric_{synthetic_index}"
                synthetic_index += 1
            append_multiline(metric, field_names, row)
            metric["source_pages"] = [page_number]
            metrics.append(metric)
            continue

        if metrics:
            append_multiline(metrics[-1], field_names, row)
            metrics[-1]["source_pages"] = sorted(set(metrics[-1]["source_pages"] + [page_number]))
        elif current_goal and current_goal["metrics"]:
            append_multiline(current_goal["metrics"][-1], field_names, row)
            current_goal["metrics"][-1]["source_pages"] = sorted(
                set(current_goal["metrics"][-1]["source_pages"] + [page_number])
            )

    for metric in metrics:
        metric["baseline"] = measurement_payload(metric["baseline_raw"])
        metric["year_1_outcome"] = measurement_payload(metric["year_1_outcome_raw"])
        metric["year_3_target"] = measurement_payload(metric["year_3_target_raw"])

    return metrics


def parse_action_table(
    table: list[list[str]],
    page_number: int,
    current_goal: dict[str, Any] | None,
    goal_lookup: dict[str, dict[str, Any]],
) -> list[tuple[str | None, dict[str, Any]]]:
    field_names = ["action_number", "title", "description", "total_funds_raw", "contributing_raw"]
    actions: list[tuple[str | None, dict[str, Any]]] = []
    column_map = action_column_map(table)
    header_preview = " | ".join(compact_text(flatten_row(row)) for row in table[:4]).lower()
    structured_table = column_map["goal_number"] is not None or "action title" in header_preview

    for row in table:
        if row_is_header(row, "action"):
            continue
        row_goal_number: str | None = None
        if structured_table:
            row_goal_number, row = project_action_row_from_columns(row, column_map)
        else:
            row = project_action_row(row)
        if not any(row):
            continue

        if RECORD_ID_RE.match(row[0]):
            action = {field: "" for field in field_names}
            append_multiline(action, field_names, row)
            action["source_pages"] = [page_number]
            target_goal_number = row_goal_number or (current_goal["goal_number"] if current_goal else None)
            actions.append((target_goal_number, action))
            continue

        if actions:
            append_multiline(actions[-1][1], field_names, row)
            actions[-1][1]["source_pages"] = sorted(set(actions[-1][1]["source_pages"] + [page_number]))
        elif current_goal and current_goal["actions"]:
            append_multiline(current_goal["actions"][-1], field_names, row)
            current_goal["actions"][-1]["source_pages"] = sorted(
                set(current_goal["actions"][-1]["source_pages"] + [page_number])
            )
        elif row_goal_number and row_goal_number in goal_lookup and goal_lookup[row_goal_number]["actions"]:
            append_multiline(goal_lookup[row_goal_number]["actions"][-1], field_names, row)
            goal_lookup[row_goal_number]["actions"][-1]["source_pages"] = sorted(
                set(goal_lookup[row_goal_number]["actions"][-1]["source_pages"] + [page_number])
            )

    for _, action in actions:
        action["title"] = strip_continued_marker(action["title"])
        action["total_funds"] = parse_currency(action["total_funds_raw"])
        action["contributing"] = parse_contributing(action["contributing_raw"])

    return actions


def extract_tables(path: Path) -> list[ParsedTable]:
    parsed_tables: list[ParsedTable] = []
    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for table in tables:
                normalized = normalize_table(table)
                kind = classify_table(normalized)
                if kind:
                    parsed_tables.append(ParsedTable(kind=kind, rows=[[str(page_number)]] + normalized))
    return parsed_tables


def parse_pdf(path: Path) -> dict[str, Any]:
    page_text = collect_page_text(path)
    metadata = extract_metadata(path, page_text=page_text)
    candidate_pages = collect_candidate_pages(page_text)
    result: dict[str, Any] = {
        "source_file": path.name,
        "source_path": str(path),
        "district_name": metadata["district_name"],
        "school_year": metadata["school_year"],
        "goals": [],
        "extraction_warnings": [],
    }

    current_goal: dict[str, Any] | None = None
    last_goal_key: tuple[str, str] | None = None
    goal_lookup: dict[str, dict[str, Any]] = {}

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            if page_number not in candidate_pages:
                continue
            tables = page.extract_tables() or []
            for raw_table in tables:
                table = normalize_table(raw_table)
                kind = classify_table(table)
                if not kind:
                    continue

                if kind == "goal":
                    goals = parse_goal_table(table, page_number)
                    if not goals:
                        has_goal_continuation = any(
                            len(row) > 1 and not normalize_text(row[0]) and normalize_text(row[1])
                            for row in table
                            if not row_is_header(row, "goal")
                        )
                        if not has_goal_continuation:
                            result["extraction_warnings"].append(
                                f"Goal table detected on page {page_number} but no goal rows were parsed."
                            )
                        continue

                    for goal in goals:
                        goal_key = (goal["goal_number"], goal["description"])
                        if goal_key == last_goal_key and result["goals"]:
                            current_goal = result["goals"][-1]
                            current_goal["source_pages"] = sorted(
                                set(current_goal["source_pages"] + goal["source_pages"])
                            )
                            goal_lookup[current_goal["goal_number"]] = current_goal
                            continue

                        result["goals"].append(goal)
                        current_goal = goal
                        last_goal_key = goal_key
                        goal_lookup[goal["goal_number"]] = goal
                    continue

                if current_goal is None and not goal_lookup:
                    result["extraction_warnings"].append(
                        f"{kind.title()} table detected on page {page_number} before any goal table."
                    )
                    continue

                if kind == "metric":
                    metrics = parse_metric_table(table, page_number, current_goal)
                    current_goal["metrics"].extend(metrics)
                elif kind == "action":
                    actions = parse_action_table(table, page_number, current_goal, goal_lookup)
                    for target_goal_number, action in actions:
                        target_goal = goal_lookup.get(target_goal_number or "")
                        if target_goal is None:
                            target_goal = current_goal
                        if target_goal is None:
                            result["extraction_warnings"].append(
                                f"Action {action.get('action_number', '')} on page {page_number} could not be attached to a goal."
                            )
                            continue
                        target_goal["actions"].append(action)

    deduped_goals: list[dict[str, Any]] = []
    seen_goals: dict[tuple[str, str], dict[str, Any]] = {}
    for goal in result["goals"]:
        goal_key = (goal["goal_number"], goal["description"])
        if goal_key not in seen_goals:
            goal["metrics"] = merge_records(goal["metrics"], "metric_number", "metric_name")
            goal["actions"] = merge_actions(goal["actions"])
            seen_goals[goal_key] = goal
            deduped_goals.append(goal)
            continue

        existing = seen_goals[goal_key]
        existing["source_pages"] = sorted(set(existing["source_pages"] + goal["source_pages"]))
        existing["metrics"] = merge_records(existing["metrics"] + goal["metrics"], "metric_number", "metric_name")
        existing["actions"] = merge_actions(existing["actions"] + goal["actions"])

    for goal in deduped_goals:
        goal["source_pages"] = sorted(set(goal["source_pages"]))
        goal["metrics"] = merge_records(goal["metrics"], "metric_number", "metric_name")
        goal["actions"] = merge_actions(goal["actions"])

    result["goals"] = deduped_goals
    result["goal_count"] = len(result["goals"])
    result["metric_count"] = sum(len(goal["metrics"]) for goal in result["goals"])
    result["action_count"] = sum(len(goal["actions"]) for goal in result["goals"])
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=LCAPS_DIR,
        help="Directory containing LCAP PDF files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory where extracted JSON outputs should be written.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively find PDFs under input-dir. Useful for statewide county subfolders.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()
    per_lcap_dir = output_dir / "per_lcap"

    output_dir.mkdir(parents=True, exist_ok=True)
    per_lcap_dir.mkdir(parents=True, exist_ok=True)

    all_lcaps: list[dict[str, Any]] = []
    pdf_paths = sorted(input_dir.rglob("*.pdf") if args.recursive else input_dir.glob("*.pdf"))
    for pdf_path in pdf_paths:
        parsed = parse_pdf(pdf_path)
        all_lcaps.append(parsed)

        output_stem = pdf_path.stem
        if args.recursive:
            relative_stem = pdf_path.relative_to(input_dir).with_suffix("")
            output_stem = "__".join(relative_stem.parts)
        output_path = per_lcap_dir / f"{sanitize_filename(output_stem)}.json"
        output_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False) + "\n")

    summary = {
        "generated_from": str(input_dir),
        "lcap_count": len(all_lcaps),
        "districts": all_lcaps,
    }
    (output_dir / "all_lcaps.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
