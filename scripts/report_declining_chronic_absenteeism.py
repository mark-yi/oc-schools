#!/usr/bin/env python3
"""Report districts with improving chronic absenteeism and attendance-related LCAP spend."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT / "outputs" / "analytics" / "2025" / "analytics.sqlite"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "analytics" / "2025" / "reports"

BROAD_ATTENDANCE_FILTER = """
(lower(a.title || ' ' || a.description) like '%attendance%'
 or lower(a.title || ' ' || a.description) like '%absen%'
 or lower(a.title || ' ' || a.description) like '%truanc%'
 or lower(a.title || ' ' || a.description) like '%re-engagement%'
 or lower(a.title || ' ' || a.description) like '%home visit%'
 or lower(a.title || ' ' || a.description) like '%sarb%')
"""

STRICT_ATTENDANCE_FILTER = """
(lower(a.title) like '%attendance%'
 or lower(a.title) like '%absen%'
 or lower(a.title) like '%truanc%'
 or lower(a.title) like '%re-engagement%'
 or lower(a.title) like '%home visit%'
 or lower(a.title) like '%sarb%')
"""

VALID_LCAP_JOIN = """
join lcap_documents ld
  on ld.cds_code = a.cds_code
 and coalesce(ld.district_name_match, 1) != 0
"""


def money(value: Any) -> str:
    return "${:,.0f}".format(float(value or 0))


def pct(value: Any) -> str:
    return "" if value in (None, "") else f"{float(value):.1f}%"


def pts(value: Any) -> str:
    return "" if value in (None, "") else f"{float(value):.1f} pts"


def markdown_table(rows: list[dict[str, Any]], headers: list[str], accessors: list[Any]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for index, row in enumerate(rows, start=1):
        values: list[str] = []
        for accessor in accessors:
            if accessor == "__rank__":
                value = index
            elif callable(accessor):
                value = accessor(row)
            else:
                value = row.get(accessor, "")
            values.append(str(value).replace("\n", " ").replace("|", "/"))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def fetch_summary(connection: sqlite3.Connection) -> sqlite3.Row:
    return connection.execute(
        f"""
        with declining as (
          select *
          from dashboard_indicators
          where indicator_name = 'chronic_absenteeism'
            and student_group = 'ALL'
            and change < 0
        ),
        broad as (
          select di.cds_code, sum(coalesce(a.total_funds, 0)) funds
          from declining di
          join lcap_actions a on a.cds_code = di.cds_code
          {VALID_LCAP_JOIN}
          where {BROAD_ATTENDANCE_FILTER}
          group by di.cds_code
        ),
        strict as (
          select di.cds_code, sum(coalesce(a.total_funds, 0)) funds
          from declining di
          join lcap_actions a on a.cds_code = di.cds_code
          {VALID_LCAP_JOIN}
          where {STRICT_ATTENDANCE_FILTER}
          group by di.cds_code
        )
        select
          (select count(*) from declining) declining_count,
          (select count(*) from broad) broad_count,
          (select count(*) from strict) strict_count,
          (select round(sum(funds), 0) from broad) broad_funds,
          (select round(sum(funds), 0) from strict) strict_funds,
          (select count(*) from lcap_documents where district_name_match = 0) excluded_lcap_mismatch_count
        """
    ).fetchone()


def fetch_candidates(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        with declining as (
          select *
          from dashboard_indicators
          where indicator_name = 'chronic_absenteeism'
            and student_group = 'ALL'
            and change < 0
        ),
        broad as (
          select di.cds_code, sum(coalesce(a.total_funds, 0)) broad_funds, count(a.action_id) broad_actions
          from declining di
          join lcap_actions a on a.cds_code = di.cds_code
          {VALID_LCAP_JOIN}
          where {BROAD_ATTENDANCE_FILTER}
          group by di.cds_code
        ),
        strict as (
          select di.cds_code, sum(coalesce(a.total_funds, 0)) strict_funds, count(a.action_id) strict_actions
          from declining di
          join lcap_actions a on a.cds_code = di.cds_code
          {VALID_LCAP_JOIN}
          where {STRICT_ATTENDANCE_FILTER}
          group by di.cds_code
        )
        select
          d.cds_code,
          d.county,
          d.district,
          di.status as chronic_absenteeism_rate,
          di.change as chronic_absenteeism_change,
          di.count as enrolled_count,
          di.chronic_count,
          coalesce(b.broad_funds, 0) as broad_attendance_funds,
          coalesce(b.broad_actions, 0) as broad_attendance_actions,
          coalesce(s.strict_funds, 0) as strict_attendance_funds,
          coalesce(s.strict_actions, 0) as strict_attendance_actions,
          case
            when coalesce(b.broad_funds, 0) > 0
            then 100.0 * coalesce(s.strict_funds, 0) / b.broad_funds
            else 0
          end as actionable_share_pct,
          case
            when di.status >= 25 then 'very high residual need'
            when di.status >= 20 then 'high residual need'
            when di.status >= 15 then 'moderate residual need'
            else 'lower residual need'
          end as residual_need_band
        from declining di
        join districts d on d.cds_code = di.cds_code
        left join broad b on b.cds_code = di.cds_code
        left join strict s on s.cds_code = di.cds_code
        order by broad_attendance_funds desc, chronic_count desc
        """
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_action_evidence(connection: sqlite3.Connection, cds_code: str, limit: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        select
          a.action_number,
          a.title,
          a.total_funds,
          a.source_pages,
          substr(replace(a.description, char(10), ' '), 1, 260) as description
        from lcap_actions a
        {VALID_LCAP_JOIN}
        where a.cds_code = ?
          and {BROAD_ATTENDANCE_FILTER}
        order by coalesce(a.total_funds, 0) desc
        limit ?
        """,
        (cds_code, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def build_report(
    summary: sqlite3.Row,
    candidates: list[dict[str, Any]],
    connection: sqlite3.Connection,
    top_limit: int,
    evidence_limit: int,
    top_csv: Path,
    all_csv: Path,
) -> str:
    top_rows = candidates[:top_limit]
    strict_top = [row for row in candidates if row["strict_attendance_funds"] and row["strict_attendance_funds"] > 0]
    strict_top.sort(key=lambda row: (row["strict_attendance_funds"], row["chronic_count"] or 0), reverse=True)

    lines: list[str] = [
        "# Declining Chronic Absenteeism: LCAP Spend Sales Report",
        "",
        f"Generated from `analytics.sqlite` on {datetime.now().strftime('%Y-%m-%d %H:%M')}.",
        "",
        "## Read This First",
        "",
        "This report finds districts where chronic absenteeism is improving, then ranks them by LCAP action dollars that appear attendance-related. These are not necessarily the districts in the most acute crisis; they are districts showing active motion, budget, and residual need around attendance.",
        "",
        "A declining chronic absenteeism rate means California Dashboard `indicator_name = chronic_absenteeism`, `student_group = ALL`, and `change < 0`. Lower is better for this indicator.",
        "",
        "LCAP spend is heuristic:",
        "- **Broad attendance-adjacent spend** includes actions where the title or description mentions attendance, absenteeism, truancy, re-engagement, home visits, or SARB.",
        "- **Strict attendance-titled spend** includes only actions whose title mentions those terms.",
        "- Large bundled/base actions can inflate broad spend. Treat page citations and action descriptions as the evidence to inspect before outreach.",
        "- LCAP documents with obvious parsed-district/manifest-district mismatches are excluded from LCAP spend calculations.",
        "",
        "## Statewide Summary",
        "",
        f"- Districts with declining chronic absenteeism: **{summary['declining_count']:,}**",
        f"- Declining districts with broad attendance-adjacent LCAP actions: **{summary['broad_count']:,}**",
        f"- Declining districts with strict attendance-titled LCAP actions: **{summary['strict_count']:,}**",
        f"- Broad attendance-adjacent LCAP dollars among declining districts: **{money(summary['broad_funds'])}**",
        f"- Strict attendance-titled LCAP dollars among declining districts: **{money(summary['strict_funds'])}**",
        f"- LCAP documents excluded for obvious district mismatch: **{summary['excluded_lcap_mismatch_count']:,}**",
        "",
        "## Top Sales Candidates By Broad Attendance-Adjacent LCAP Spend",
        "",
    ]

    lines.append(
        markdown_table(
            top_rows[:25],
            [
                "Rank",
                "County",
                "District",
                "Current chronic rate",
                "Change",
                "Chronically absent students",
                "Broad LCAP $",
                "Broad actions",
                "Strict title $",
                "Actionable share",
                "Residual need",
            ],
            [
                "__rank__",
                "county",
                "district",
                lambda row: pct(row["chronic_absenteeism_rate"]),
                lambda row: pts(row["chronic_absenteeism_change"]),
                lambda row: f"{int(row['chronic_count'] or 0):,}",
                lambda row: money(row["broad_attendance_funds"]),
                "broad_attendance_actions",
                lambda row: money(row["strict_attendance_funds"]),
                lambda row: f"{float(row['actionable_share_pct'] or 0):.1f}%",
                "residual_need_band",
            ],
        )
    )

    lines.extend(["", "## Cleaner Signal: Top Districts By Strict Attendance-Titled Spend", ""])
    lines.append(
        markdown_table(
            strict_top[:20],
            [
                "Rank",
                "County",
                "District",
                "Current chronic rate",
                "Change",
                "Chronically absent students",
                "Strict title $",
                "Strict actions",
                "Actionable share",
                "Broad $",
            ],
            [
                "__rank__",
                "county",
                "district",
                lambda row: pct(row["chronic_absenteeism_rate"]),
                lambda row: pts(row["chronic_absenteeism_change"]),
                lambda row: f"{int(row['chronic_count'] or 0):,}",
                lambda row: money(row["strict_attendance_funds"]),
                "strict_attendance_actions",
                lambda row: f"{float(row['actionable_share_pct'] or 0):.1f}%",
                lambda row: money(row["broad_attendance_funds"]),
            ],
        )
    )

    lines.extend(["", "## Account Notes For Top Broad-Spend Candidates", ""])
    for index, row in enumerate(top_rows[:12], start=1):
        lines.extend(
            [
                f"### {index}. {row['district']} ({row['county']})",
                "",
                f"- Chronic absenteeism: **{pct(row['chronic_absenteeism_rate'])}**, change **{pts(row['chronic_absenteeism_change'])}**, chronically absent students **{int(row['chronic_count'] or 0):,}**",
                f"- Estimated broad attendance-adjacent LCAP spend: **{money(row['broad_attendance_funds'])}** across **{row['broad_attendance_actions']}** actions",
                f"- Strict attendance-titled LCAP spend: **{money(row['strict_attendance_funds'])}** across **{row['strict_attendance_actions']}** actions",
                f"- Actionable share: **{float(row['actionable_share_pct'] or 0):.1f}%** of broad attendance-adjacent spend is strict attendance-titled spend",
                f"- Sales read: {row['residual_need_band']}; improving trend plus budget suggests an active attendance agenda. Validate whether the spend is actually vendor-addressable before outreach.",
                "",
                "Top LCAP evidence:",
            ]
        )
        for action in fetch_action_evidence(connection, row["cds_code"], evidence_limit):
            title = str(action["title"] or "").replace("\n", " ")
            description = str(action["description"] or "").replace("\n", " ")
            lines.append(
                f"- Action {action['action_number']}: **{title}**, {money(action['total_funds'])}; pages `{action['source_pages']}`. {description}"
            )
        lines.append("")

    lines.extend(
        [
            "## Files",
            "",
            f"- Top candidates CSV: `{top_csv}`",
            f"- All declining districts CSV: `{all_csv}`",
            "- Source database: `outputs/analytics/2025/analytics.sqlite`",
            "",
            "## Next Refinement",
            "",
            "The next useful pass is to classify each attendance-related action as base operations, staffing, services, software/data system, transportation, family outreach, or intervention. That would separate true sellable spend from bundled district operating costs.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--top-limit", type=int, default=50)
    parser.add_argument("--evidence-limit", type=int, default=4)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.output_dir / "declining_chronic_absenteeism_sales_report.md"
    top_csv = args.output_dir / "declining_chronic_absenteeism_top_candidates.csv"
    all_csv = args.output_dir / "declining_chronic_absenteeism_all_districts.csv"

    connection = sqlite3.connect(args.db_path)
    connection.row_factory = sqlite3.Row
    try:
        summary = fetch_summary(connection)
        candidates = fetch_candidates(connection)
        write_csv(top_csv, candidates[: args.top_limit])
        write_csv(all_csv, candidates)
        report_path.write_text(
            build_report(summary, candidates, connection, args.top_limit, args.evidence_limit, top_csv, all_csv),
            encoding="utf-8",
        )
    finally:
        connection.close()

    print(report_path)
    print(top_csv)
    print(all_csv)


if __name__ == "__main__":
    main()
