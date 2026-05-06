#!/usr/bin/env python3
"""Build flat analytics tables from LCAP and Dashboard JSON outputs."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LCAP_PATH = ROOT / "outputs" / "statewide_2025" / "lcaps_json" / "all_lcaps.json"
DEFAULT_MANIFEST_PATH = ROOT / "outputs" / "lcap_downloads" / "lcaps_2025_manifest.json"
DEFAULT_DASHBOARD_DIR = ROOT / "data" / "dashboard_public" / "2025" / "by_cds_code"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "analytics" / "2025"

CDS_CODE_RE = re.compile(r"\b(\d{14})\b")

INDICATOR_NAMES = {
    "1": "chronic_absenteeism",
    "2": "suspension_rate",
    "3": "english_learner_progress",
    "4": "graduation_rate",
    "5": "college_career",
    "6": "ela",
    "7": "math",
    "8": "science",
    "cci": "college_career",
    "elpi": "english_learner_progress",
    "elpi_2024": "english_learner_progress",
}

TABLE_FIELDS: dict[str, list[str]] = {
    "districts": [
        "cds_code",
        "cd_code",
        "county",
        "district",
        "doc",
        "doc_type",
        "status_type",
        "street",
        "city",
        "zip",
        "state",
        "phone",
        "admin_first_name",
        "admin_last_name",
        "latitude",
        "longitude",
        "has_lcap",
        "has_dashboard",
    ],
    "lcap_documents": [
        "cds_code",
        "cd_code",
        "county",
        "district",
        "school_year",
        "source_file",
        "source_path",
        "pdf_url",
        "goal_count",
        "metric_count",
        "action_count",
        "extraction_warning_count",
        "extraction_error_count",
        "extraction_warnings",
        "extraction_errors",
    ],
    "lcap_goals": [
        "goal_id",
        "cds_code",
        "county",
        "district",
        "school_year",
        "goal_number",
        "goal_type",
        "description",
        "source_pages",
    ],
    "lcap_actions": [
        "action_id",
        "goal_id",
        "cds_code",
        "county",
        "district",
        "school_year",
        "goal_number",
        "action_number",
        "title",
        "description",
        "total_funds",
        "total_funds_raw",
        "contributing",
        "contributing_raw",
        "source_pages",
    ],
    "lcap_metrics": [
        "metric_id",
        "goal_id",
        "cds_code",
        "county",
        "district",
        "school_year",
        "goal_number",
        "metric_number",
        "metric_name",
        "baseline_raw",
        "year_1_outcome_raw",
        "year_2_outcome_raw",
        "year_3_target_raw",
        "current_difference_from_baseline_raw",
        "source_pages",
    ],
    "dashboard_indicators": [
        "cds_code",
        "county",
        "district",
        "school_year_id",
        "indicator_id",
        "indicator_name",
        "student_group",
        "status",
        "change",
        "status_id",
        "change_id",
        "performance",
        "count",
        "chronic_count",
        "red",
        "orange",
        "yellow",
        "green",
        "blue",
        "is_private_data",
    ],
    "dashboard_student_groups": [
        "cds_code",
        "county",
        "district",
        "school_year_id",
        "indicator_id",
        "indicator_name",
        "student_group",
        "status",
        "change",
        "status_id",
        "change_id",
        "performance",
        "count",
        "chronic_count",
        "red",
        "orange",
        "yellow",
        "green",
        "blue",
        "is_private_data",
    ],
    "dashboard_trends": [
        "cds_code",
        "county",
        "district",
        "school_year_id",
        "indicator_id",
        "indicator_name",
        "grade",
        "current_year",
        "one_year_ago",
        "two_years_ago",
        "three_years_ago",
        "four_years_ago",
    ],
}

SQLITE_TYPES = {
    "goal_count": "INTEGER",
    "metric_count": "INTEGER",
    "action_count": "INTEGER",
    "extraction_warning_count": "INTEGER",
    "extraction_error_count": "INTEGER",
    "status": "REAL",
    "change": "REAL",
    "status_id": "INTEGER",
    "change_id": "INTEGER",
    "performance": "INTEGER",
    "count": "INTEGER",
    "chronic_count": "INTEGER",
    "red": "INTEGER",
    "orange": "INTEGER",
    "yellow": "INTEGER",
    "green": "INTEGER",
    "blue": "INTEGER",
    "school_year_id": "INTEGER",
    "total_funds": "REAL",
    "contributing": "INTEGER",
    "has_lcap": "INTEGER",
    "has_dashboard": "INTEGER",
    "is_private_data": "INTEGER",
    "current_year": "REAL",
    "one_year_ago": "REAL",
    "two_years_ago": "REAL",
    "three_years_ago": "REAL",
    "four_years_ago": "REAL",
}


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_bool_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return 1 if bool(value) else 0


def extract_cds_code(*values: Any) -> str:
    for value in values:
        match = CDS_CODE_RE.search(clean(value))
        if match:
            return match.group(1)
    return ""


def indicator_name(indicator_id: Any) -> str:
    key = clean(indicator_id)
    return INDICATOR_NAMES.get(key, f"indicator_{key}" if key else "")


def json_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def csv_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json_text(value)
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return ""
    return value


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_manifest(path: Path) -> list[dict[str, Any]]:
    data = load_json(path)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    raise ValueError(f"Unsupported manifest shape: {path}")


def manifest_indexes(records: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_cds: dict[str, dict[str, Any]] = {}
    by_path: dict[str, dict[str, Any]] = {}
    for record in records:
        cds_code = clean(record.get("cds_code"))
        if cds_code:
            by_cds[cds_code] = record
        for key in ("output_path", "cds_index_path"):
            path = clean(record.get(key))
            if path:
                by_path[str(Path(path))] = record
    return by_cds, by_path


def district_row(record: dict[str, Any], has_dashboard: bool) -> dict[str, Any]:
    source = record.get("cde_public_district") if isinstance(record.get("cde_public_district"), dict) else record
    return {
        "cds_code": clean(record.get("cds_code") or source.get("cds_code")),
        "cd_code": clean(record.get("cd_code") or source.get("cd_code")),
        "county": clean(record.get("county") or source.get("county")),
        "district": clean(record.get("district") or source.get("district")),
        "doc": clean(record.get("doc") or source.get("doc")),
        "doc_type": clean(record.get("doc_type") or source.get("doc_type")),
        "status_type": clean(record.get("status_type") or source.get("status_type")),
        "street": clean(source.get("street")),
        "city": clean(source.get("city")),
        "zip": clean(source.get("zip")),
        "state": clean(source.get("state")),
        "phone": clean(source.get("phone")),
        "admin_first_name": clean(source.get("admin_first_name")),
        "admin_last_name": clean(source.get("admin_last_name")),
        "latitude": clean(source.get("latitude")),
        "longitude": clean(source.get("longitude")),
        "has_lcap": as_bool_int(record.get("has_lcap")),
        "has_dashboard": 1 if has_dashboard else 0,
    }


def resolve_lcap_manifest(parsed: dict[str, Any], by_cds: dict[str, dict[str, Any]], by_path: dict[str, dict[str, Any]]) -> dict[str, Any]:
    source_path = clean(parsed.get("source_path"))
    record = by_path.get(source_path)
    if record:
        return record
    cds_code = extract_cds_code(source_path, parsed.get("source_file"), parsed.get("district_name"))
    return by_cds.get(cds_code, {"cds_code": cds_code})


def flatten_lcaps(lcap_path: Path, manifest_by_cds: dict[str, dict[str, Any]], manifest_by_path: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    data = load_json(lcap_path)
    districts = data.get("districts", []) if isinstance(data, dict) else data
    if not isinstance(districts, list):
        raise ValueError(f"Unsupported LCAP JSON shape: {lcap_path}")

    rows: dict[str, list[dict[str, Any]]] = {
        "lcap_documents": [],
        "lcap_goals": [],
        "lcap_actions": [],
        "lcap_metrics": [],
    }

    for parsed in districts:
        manifest = resolve_lcap_manifest(parsed, manifest_by_cds, manifest_by_path)
        cds_code = clean(manifest.get("cds_code")) or extract_cds_code(parsed.get("source_path"), parsed.get("source_file"))
        county = clean(manifest.get("county"))
        district = clean(manifest.get("district")) or clean(parsed.get("district_name"))
        school_year = clean(parsed.get("school_year"))

        extraction_warnings = parsed.get("extraction_warnings") or []
        extraction_errors = parsed.get("extraction_errors") or []
        rows["lcap_documents"].append(
            {
                "cds_code": cds_code,
                "cd_code": clean(manifest.get("cd_code")),
                "county": county,
                "district": district,
                "school_year": school_year,
                "source_file": clean(parsed.get("source_file")),
                "source_path": clean(parsed.get("source_path")),
                "pdf_url": clean(manifest.get("pdf_url")),
                "goal_count": parsed.get("goal_count", 0),
                "metric_count": parsed.get("metric_count", 0),
                "action_count": parsed.get("action_count", 0),
                "extraction_warning_count": len(extraction_warnings),
                "extraction_error_count": len(extraction_errors),
                "extraction_warnings": extraction_warnings,
                "extraction_errors": extraction_errors,
            }
        )

        for goal_index, goal in enumerate(parsed.get("goals") or [], start=1):
            goal_number = clean(goal.get("goal_number")) or str(goal_index)
            goal_id = f"{cds_code}:goal:{goal_number}:{goal_index}"
            rows["lcap_goals"].append(
                {
                    "goal_id": goal_id,
                    "cds_code": cds_code,
                    "county": county,
                    "district": district,
                    "school_year": school_year,
                    "goal_number": goal_number,
                    "goal_type": clean(goal.get("goal_type")),
                    "description": clean(goal.get("description")),
                    "source_pages": goal.get("source_pages") or [],
                }
            )

            for action_index, action in enumerate(goal.get("actions") or [], start=1):
                action_number = clean(action.get("action_number")) or str(action_index)
                rows["lcap_actions"].append(
                    {
                        "action_id": f"{goal_id}:action:{action_number}:{action_index}",
                        "goal_id": goal_id,
                        "cds_code": cds_code,
                        "county": county,
                        "district": district,
                        "school_year": school_year,
                        "goal_number": goal_number,
                        "action_number": action_number,
                        "title": clean(action.get("title")),
                        "description": clean(action.get("description")),
                        "total_funds": action.get("total_funds"),
                        "total_funds_raw": clean(action.get("total_funds_raw")),
                        "contributing": as_bool_int(action.get("contributing")),
                        "contributing_raw": clean(action.get("contributing_raw")),
                        "source_pages": action.get("source_pages") or [],
                    }
                )

            for metric_index, metric in enumerate(goal.get("metrics") or [], start=1):
                metric_number = clean(metric.get("metric_number")) or str(metric_index)
                rows["lcap_metrics"].append(
                    {
                        "metric_id": f"{goal_id}:metric:{metric_number}:{metric_index}",
                        "goal_id": goal_id,
                        "cds_code": cds_code,
                        "county": county,
                        "district": district,
                        "school_year": school_year,
                        "goal_number": goal_number,
                        "metric_number": metric_number,
                        "metric_name": clean(metric.get("metric_name")),
                        "baseline_raw": clean(metric.get("baseline_raw")),
                        "year_1_outcome_raw": clean(metric.get("year_1_outcome_raw")),
                        "year_2_outcome_raw": clean(metric.get("year_2_outcome_raw")),
                        "year_3_target_raw": clean(metric.get("year_3_target_raw")),
                        "current_difference_from_baseline_raw": clean(
                            metric.get("current_difference_from_baseline_raw")
                        ),
                        "source_pages": metric.get("source_pages") or [],
                    }
                )

    return rows


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def dashboard_metric_row(package: dict[str, Any], indicator_id: Any, payload: dict[str, Any]) -> dict[str, Any]:
    metadata = package.get("metadata") or {}
    district = metadata.get("district") or {}
    return {
        "cds_code": clean(district.get("cds_code") or payload.get("cdsCode")),
        "county": clean(district.get("county")),
        "district": clean(district.get("district")),
        "school_year_id": payload.get("schoolYearId") or metadata.get("school_year_id"),
        "indicator_id": clean(indicator_id or payload.get("indicatorId")),
        "indicator_name": indicator_name(indicator_id or payload.get("indicatorId")),
        "student_group": clean(payload.get("studentGroup")),
        "status": payload.get("status"),
        "change": payload.get("change"),
        "status_id": payload.get("statusId"),
        "change_id": payload.get("changeId"),
        "performance": payload.get("performance"),
        "count": payload.get("count"),
        "chronic_count": payload.get("chronicCount"),
        "red": payload.get("red"),
        "orange": payload.get("orange"),
        "yellow": payload.get("yellow"),
        "green": payload.get("green"),
        "blue": payload.get("blue"),
        "is_private_data": as_bool_int(payload.get("isPrivateData")),
    }


def flatten_dashboard(dashboard_dir: Path) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    rows: dict[str, list[dict[str, Any]]] = {
        "dashboard_indicators": [],
        "dashboard_student_groups": [],
        "dashboard_trends": [],
    }
    dashboard_cds_codes: set[str] = set()

    for path in sorted(dashboard_dir.glob("*.json")):
        package = load_json(path)
        metadata = package.get("metadata") or {}
        district = metadata.get("district") or {}
        cds_code = clean(district.get("cds_code")) or path.stem
        if cds_code:
            dashboard_cds_codes.add(cds_code)

        for card in package.get("summary", {}).get("summary_cards") or []:
            if not isinstance(card, dict):
                continue
            primary = card.get("primary")
            if isinstance(primary, dict):
                rows["dashboard_indicators"].append(dashboard_metric_row(package, card.get("indicatorId"), primary))

        for indicator_id, group_payload in (package.get("student_groups") or {}).items():
            for item in as_list(group_payload):
                if not isinstance(item, dict):
                    continue
                primary = item.get("primary")
                candidates: list[dict[str, Any]] = []
                if isinstance(primary, dict) and isinstance(primary.get("list"), list):
                    candidates.extend(row for row in primary["list"] if isinstance(row, dict))
                elif isinstance(primary, dict):
                    candidates.append(primary)
                elif isinstance(item.get("list"), list):
                    candidates.extend(row for row in item["list"] if isinstance(row, dict))
                for candidate in candidates:
                    rows["dashboard_student_groups"].append(dashboard_metric_row(package, indicator_id, candidate))

        for indicator_id, trend_payload in (package.get("charts") or {}).items():
            for trend in as_list(trend_payload):
                if not isinstance(trend, dict):
                    continue
                rows["dashboard_trends"].append(
                    {
                        "cds_code": cds_code,
                        "county": clean(district.get("county")),
                        "district": clean(district.get("district")),
                        "school_year_id": trend.get("schoolYearId") or metadata.get("school_year_id"),
                        "indicator_id": clean(indicator_id or trend.get("indicatorId")),
                        "indicator_name": indicator_name(indicator_id or trend.get("indicatorId")),
                        "grade": clean(trend.get("grade")),
                        "current_year": trend.get("currentYear"),
                        "one_year_ago": trend.get("oneYearAgo"),
                        "two_years_ago": trend.get("twoYearsAgo"),
                        "three_years_ago": trend.get("threeYearsAgo"),
                        "four_years_ago": trend.get("fourYearsAgo"),
                    }
                )

    return rows, dashboard_cds_codes


def write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in fields})


def sqlite_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json_text(value)
    if isinstance(value, bool):
        return 1 if value else 0
    return value


def write_sqlite(path: Path, tables: dict[str, list[dict[str, Any]]]) -> None:
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    try:
        for table, fields in TABLE_FIELDS.items():
            columns = ", ".join(f'"{field}" {SQLITE_TYPES.get(field, "TEXT")}' for field in fields)
            connection.execute(f'CREATE TABLE "{table}" ({columns})')
            placeholders = ", ".join("?" for _ in fields)
            quoted_fields = ", ".join(f'"{field}"' for field in fields)
            values = [
                tuple(sqlite_value(row.get(field)) for field in fields)
                for row in tables.get(table, [])
            ]
            if values:
                connection.executemany(f'INSERT INTO "{table}" ({quoted_fields}) VALUES ({placeholders})', values)

        index_statements = [
            "CREATE INDEX idx_districts_cds ON districts(cds_code)",
            "CREATE INDEX idx_lcap_documents_cds ON lcap_documents(cds_code)",
            "CREATE INDEX idx_lcap_goals_cds ON lcap_goals(cds_code)",
            "CREATE INDEX idx_lcap_actions_cds ON lcap_actions(cds_code)",
            "CREATE INDEX idx_lcap_actions_funds ON lcap_actions(total_funds)",
            "CREATE INDEX idx_lcap_metrics_cds ON lcap_metrics(cds_code)",
            "CREATE INDEX idx_dashboard_indicators_lookup ON dashboard_indicators(indicator_name, student_group, change)",
            "CREATE INDEX idx_dashboard_student_groups_lookup ON dashboard_student_groups(indicator_name, student_group, performance)",
            "CREATE INDEX idx_dashboard_trends_lookup ON dashboard_trends(indicator_name, cds_code)",
        ]
        for statement in index_statements:
            connection.execute(statement)
        connection.commit()
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2025, help="Output year label.")
    parser.add_argument("--lcap-path", type=Path, default=DEFAULT_LCAP_PATH)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--dashboard-dir", type=Path, default=DEFAULT_DASHBOARD_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(args.manifest_path)
    manifest_by_cds, manifest_by_path = manifest_indexes(manifest)
    dashboard_rows, dashboard_cds_codes = flatten_dashboard(args.dashboard_dir)
    lcap_rows = flatten_lcaps(args.lcap_path, manifest_by_cds, manifest_by_path)

    district_rows = [district_row(record, clean(record.get("cds_code")) in dashboard_cds_codes) for record in manifest]
    for cds_code in sorted(dashboard_cds_codes - set(manifest_by_cds)):
        district_rows.append(district_row({"cds_code": cds_code}, True))

    tables: dict[str, list[dict[str, Any]]] = {
        "districts": district_rows,
        **lcap_rows,
        **dashboard_rows,
    }

    for table, fields in TABLE_FIELDS.items():
        write_csv(output_dir / f"{table}.csv", fields, tables.get(table, []))
    write_sqlite(output_dir / "analytics.sqlite", tables)

    summary = {table: len(tables.get(table, [])) for table in TABLE_FIELDS}
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    for table, count in summary.items():
        print(f"{table}: {count}")
    print(f"wrote {output_dir}")


if __name__ == "__main__":
    main()
