#!/usr/bin/env python3
"""Reusable opportunity queries over the LCAP analytics database."""

from __future__ import annotations

import math
from pathlib import Path
import re
import sqlite3
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ANALYTICS_DB = ROOT / "outputs" / "analytics" / "2025" / "analytics.sqlite"

TOPICS: dict[str, dict[str, Any]] = {
    "chronic_absenteeism": {
        "indicator_name": "chronic_absenteeism",
        "student_group": "ALL",
        "lower_is_better": True,
        "terms": (
            "chronic absentee",
            "attendance",
            "absen",
            "truanc",
            "re-engagement",
            "reengagement",
            "home visit",
            "sarb",
            "sart",
        ),
        "strict_title_terms": (
            "chronic absentee",
            "attendance",
            "absen",
            "truanc",
            "re-engagement",
            "reengagement",
            "home visit",
            "sarb",
            "sart",
        ),
        "default_narrative_query": (
            "chronic absenteeism attendance barriers family outreach "
            "student re-engagement truancy home visits"
        ),
    }
}

SELLABLE_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("software_or_data_system", ("software", "platform", "system", "dashboard", "data", "analytics", "monitor")),
    (
        "outreach_workflow",
        ("outreach", "message", "messaging", "communication", "notify", "phone", "text", "family engagement"),
    ),
    ("case_management_or_services", ("case management", "home visit", "liaison", "re-engagement", "reengagement")),
    ("attendance_intervention", ("sarb", "sart", "truancy", "attendance team", "attendance intervention")),
)

BUNDLED_PATTERNS = (
    "base:",
    "base ",
    "ongoing operating",
    "on-going operating",
    "instruction",
    "teacher",
    "staffing",
    "personnel",
    "salary",
    "salaries",
    "benefits",
    "maintenance",
)


class OpportunityError(ValueError):
    """Raised when an opportunity query is not supported."""


def connect_analytics(db_path: Path = DEFAULT_ANALYTICS_DB) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def normalize_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\u2010", "-").replace("\u2011", "-").replace("\u2012", "-")
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def compact_money(value: Any) -> str:
    return "${:,.0f}".format(float(value or 0))


def topic_config(topic: str) -> dict[str, Any]:
    normalized = topic.strip().casefold().replace("-", "_").replace(" ", "_")
    if normalized not in TOPICS:
        supported = ", ".join(sorted(TOPICS))
        raise OpportunityError(f"Unsupported topic '{topic}'. Supported topics: {supported}.")
    return TOPICS[normalized]


def text_match_sql(expression: str, terms: tuple[str, ...]) -> tuple[str, list[str]]:
    clauses = [f"lower({expression}) like ?" for _ in terms]
    return "(" + " or ".join(clauses) + ")", [f"%{term.casefold()}%" for term in terms]


def outcome_trend_sql(config: dict[str, Any], trend: str) -> str:
    normalized = trend.strip().casefold().replace("-", "_").replace(" ", "_")
    lower_is_better = bool(config["lower_is_better"])
    if normalized in {"any", "all", ""}:
        return ""
    if normalized == "improving":
        return "and di.change < 0" if lower_is_better else "and di.change > 0"
    if normalized == "worsening":
        return "and di.change > 0" if lower_is_better else "and di.change < 0"
    if normalized in {"decreasing_rate", "declining_rate", "rate_declining"}:
        return "and di.change < 0"
    if normalized in {"increasing_rate", "rising_rate", "rate_increasing"}:
        return "and di.change > 0"
    raise OpportunityError(
        "Unsupported outcome_trend. Use improving, worsening, decreasing_rate, increasing_rate, or any."
    )


def classify_actionability(title: str, description: str, funds: float) -> dict[str, Any]:
    text = f"{normalize_text(title)} {normalize_text(description)}".casefold()
    for label, patterns in SELLABLE_PATTERNS:
        if any(pattern in text for pattern in patterns):
            return {
                "actionability": label,
                "actionability_confidence": "high",
                "sales_read": "Likely vendor-addressable or operationally addressable attendance work.",
            }
    if any(pattern in text for pattern in BUNDLED_PATTERNS):
        return {
            "actionability": "bundled_or_staffing",
            "actionability_confidence": "low",
            "sales_read": "Budget may be large but likely bundled into staffing, base operations, or broad programs.",
        }
    if funds >= 5_000_000:
        return {
            "actionability": "large_unclear_bundle",
            "actionability_confidence": "low",
            "sales_read": "Large budget with unclear vendor-addressable wedge; inspect source pages before outreach.",
        }
    return {
        "actionability": "unclear",
        "actionability_confidence": "medium",
        "sales_read": "Potential opportunity, but action details need review.",
    }


def opportunity_score(row: dict[str, Any]) -> float:
    strict = float(row.get("strict_action_funds") or 0)
    broad = float(row.get("broad_action_funds") or 0)
    chronic_count = float(row.get("affected_student_count") or 0)
    rate = float(row.get("current_status") or 0)
    change = abs(float(row.get("outcome_change") or 0))
    spend = strict if strict > 0 else broad * 0.15
    return (
        math.log10(spend + 1) * 2.0
        + math.log10(chronic_count + 1) * 1.5
        + rate / 10.0
        + change
    )


def find_opportunities(
    *,
    topic: str = "chronic_absenteeism",
    outcome_trend: str = "worsening",
    rank_by: str = "strict_action_funds",
    county: str | None = None,
    district: str | None = None,
    limit: int = 25,
    include_actions: bool = True,
    action_limit: int = 3,
    db_path: Path = DEFAULT_ANALYTICS_DB,
) -> list[dict[str, Any]]:
    """Find district opportunities by joining Dashboard outcomes to LCAP spend."""

    config = topic_config(topic)
    sort_key = rank_by.strip().casefold().replace("-", "_")
    if sort_key not in {
        "broad_action_funds",
        "strict_action_funds",
        "affected_student_count",
        "current_status",
        "outcome_change",
        "opportunity_score",
    }:
        raise OpportunityError(
            "Unsupported rank_by. Use strict_action_funds, broad_action_funds, affected_student_count, "
            "current_status, outcome_change, or opportunity_score."
        )
    action_scope = "strict" if sort_key == "strict_action_funds" else "broad"
    action_match, action_params = text_match_sql(
        "coalesce(a.title, '') || ' ' || coalesce(a.description, '')",
        tuple(config["terms"]),
    )
    title_match, title_params = text_match_sql("coalesce(a.title, '')", tuple(config["strict_title_terms"]))
    goal_match, goal_params = text_match_sql("coalesce(g.description, '')", tuple(config["terms"]))
    metric_match, metric_params = text_match_sql(
        """
        coalesce(m.metric_name, '') || ' ' ||
        coalesce(m.baseline_raw, '') || ' ' ||
        coalesce(m.year_1_outcome_raw, '') || ' ' ||
        coalesce(m.year_2_outcome_raw, '') || ' ' ||
        coalesce(m.year_3_target_raw, '') || ' ' ||
        coalesce(m.current_difference_from_baseline_raw, '')
        """,
        tuple(config["terms"]),
    )
    trend_clause = outcome_trend_sql(config, outcome_trend)

    filters: list[str] = []
    filter_params: list[Any] = []
    if county:
        filters.append("d.county = ?")
        filter_params.append(county)
    if district:
        filters.append("d.district like ?")
        filter_params.append(district.replace("*", "%"))
    filter_clause = "and " + " and ".join(filters) if filters else ""

    sql = f"""
        with outcomes as (
          select di.*
          from dashboard_indicators di
          where di.indicator_name = ?
            and di.student_group = ?
            {trend_clause}
        ),
        broad_actions as (
          select
            a.cds_code,
            count(distinct a.action_id) action_count,
            round(sum(coalesce(a.total_funds, 0)), 0) action_funds
          from lcap_actions a
          where {action_match}
            and exists (
              select 1
              from lcap_documents ld
              where ld.cds_code = a.cds_code
                and coalesce(ld.district_name_match, 1) != 0
            )
          group by a.cds_code
        ),
        strict_actions as (
          select
            a.cds_code,
            count(distinct a.action_id) action_count,
            round(sum(coalesce(a.total_funds, 0)), 0) action_funds
          from lcap_actions a
          where {title_match}
            and exists (
              select 1
              from lcap_documents ld
              where ld.cds_code = a.cds_code
                and coalesce(ld.district_name_match, 1) != 0
            )
          group by a.cds_code
        ),
        goal_matches as (
          select g.cds_code, count(distinct g.goal_id) goal_count
          from lcap_goals g
          where {goal_match}
          group by g.cds_code
        ),
        metric_matches as (
          select m.cds_code, count(distinct m.metric_id) metric_count
          from lcap_metrics m
          where {metric_match}
          group by m.cds_code
        )
        select
          d.cds_code,
          d.county,
          d.district,
          o.indicator_name,
          o.student_group,
          o.status current_status,
          o.change outcome_change,
          o.count enrollment_count,
          o.chronic_count affected_student_count,
          coalesce(ba.action_count, 0) broad_action_count,
          coalesce(ba.action_funds, 0) broad_action_funds,
          coalesce(sa.action_count, 0) strict_action_count,
          coalesce(sa.action_funds, 0) strict_action_funds,
          coalesce(gm.goal_count, 0) topic_goal_count,
          coalesce(mm.metric_count, 0) topic_metric_count,
          case
            when coalesce(ba.action_funds, 0) > 0
            then 100.0 * coalesce(sa.action_funds, 0) / ba.action_funds
            else 0
          end strict_share_pct
        from outcomes o
        join districts d on d.cds_code = o.cds_code
        left join broad_actions ba on ba.cds_code = o.cds_code
        left join strict_actions sa on sa.cds_code = o.cds_code
        left join goal_matches gm on gm.cds_code = o.cds_code
        left join metric_matches mm on mm.cds_code = o.cds_code
        where (
          coalesce(ba.action_count, 0) > 0
          or coalesce(sa.action_count, 0) > 0
          or coalesce(gm.goal_count, 0) > 0
          or coalesce(mm.metric_count, 0) > 0
        )
        {filter_clause}
    """
    params: list[Any] = [
        config["indicator_name"],
        config["student_group"],
        *action_params,
        *title_params,
        *goal_params,
        *metric_params,
        *filter_params,
    ]

    connection = connect_analytics(db_path)
    try:
        rows = [dict(row) for row in connection.execute(sql, params).fetchall()]
        for row in rows:
            row["topic"] = topic
            row["outcome_trend"] = outcome_trend
            row["opportunity_score"] = round(opportunity_score(row), 3)
            row["outcome_read"] = outcome_read(topic, outcome_trend, row)
            if include_actions:
                row["top_action_scope"] = action_scope
                row["top_actions"] = fetch_topic_actions(
                    connection,
                    row["cds_code"],
                    topic=topic,
                    scope=action_scope,
                    limit=action_limit,
                )
    finally:
        connection.close()

    if sort_key == "opportunity_score":
        rows.sort(key=lambda row: (-float(row["opportunity_score"]), row["district"]))
    elif sort_key == "outcome_change":
        rows.sort(key=lambda row: (abs(float(row.get("outcome_change") or 0)), row["district"]), reverse=True)
    elif sort_key in {"broad_action_funds", "strict_action_funds", "affected_student_count", "current_status"}:
        rows.sort(key=lambda row: (float(row.get(sort_key) or 0), row["district"]), reverse=True)
    return rows[:limit]


def fetch_topic_actions(
    connection: sqlite3.Connection,
    cds_code: str,
    *,
    topic: str = "chronic_absenteeism",
    scope: str = "broad",
    limit: int = 5,
) -> list[dict[str, Any]]:
    config = topic_config(topic)
    if scope == "strict":
        match, params = text_match_sql("coalesce(a.title, '')", tuple(config["strict_title_terms"]))
    else:
        match, params = text_match_sql(
            "coalesce(a.title, '') || ' ' || coalesce(a.description, '')",
            tuple(config["terms"]),
        )
    rows = connection.execute(
        f"""
        select
          a.action_id,
          a.goal_id,
          a.goal_number,
          a.action_number,
          a.title,
          a.description,
          a.total_funds,
          a.total_funds_raw,
          a.contributing,
          a.source_pages
        from lcap_actions a
        where a.cds_code = ?
          and {match}
          and exists (
            select 1
            from lcap_documents ld
            where ld.cds_code = a.cds_code
              and coalesce(ld.district_name_match, 1) != 0
          )
        order by coalesce(a.total_funds, 0) desc
        limit ?
        """,
        [cds_code, *params, limit],
    ).fetchall()
    actions: list[dict[str, Any]] = []
    for row in rows:
        action = dict(row)
        action["description_snippet"] = normalize_text(action.get("description"))[:360]
        action.pop("description", None)
        action.update(classify_actionability(action.get("title", ""), action["description_snippet"], action["total_funds"] or 0))
        actions.append(action)
    return actions


def get_account_brief(
    cds_code: str,
    *,
    topic: str = "chronic_absenteeism",
    db_path: Path = DEFAULT_ANALYTICS_DB,
    action_limit: int = 6,
) -> dict[str, Any]:
    config = topic_config(topic)
    connection = connect_analytics(db_path)
    try:
        district = connection.execute("select * from districts where cds_code = ?", (cds_code,)).fetchone()
        outcome = connection.execute(
            """
            select *
            from dashboard_indicators
            where cds_code = ?
              and indicator_name = ?
              and student_group = ?
            limit 1
            """,
            (cds_code, config["indicator_name"], config["student_group"]),
        ).fetchone()
        actions = fetch_topic_actions(connection, cds_code, topic=topic, scope="broad", limit=action_limit)
        strict_actions = fetch_topic_actions(connection, cds_code, topic=topic, scope="strict", limit=action_limit)
        goals = fetch_topic_goals(connection, cds_code, topic=topic, limit=5)
        metrics = fetch_topic_metrics(connection, cds_code, topic=topic, limit=5)
    finally:
        connection.close()
    return {
        "cds_code": cds_code,
        "topic": topic,
        "district": dict(district) if district else None,
        "dashboard_outcome": dict(outcome) if outcome else None,
        "topic_goals": goals,
        "topic_metrics": metrics,
        "broad_topic_actions": actions,
        "strict_topic_actions": strict_actions,
    }


def fetch_topic_goals(
    connection: sqlite3.Connection,
    cds_code: str,
    *,
    topic: str = "chronic_absenteeism",
    limit: int = 5,
) -> list[dict[str, Any]]:
    config = topic_config(topic)
    match, params = text_match_sql("coalesce(g.description, '')", tuple(config["terms"]))
    rows = connection.execute(
        f"""
        select goal_id, goal_number, goal_type, description, source_pages
        from lcap_goals g
        where cds_code = ?
          and {match}
        order by goal_number
        limit ?
        """,
        [cds_code, *params, limit],
    ).fetchall()
    goals: list[dict[str, Any]] = []
    for row in rows:
        goal = dict(row)
        goal["description_snippet"] = normalize_text(goal.get("description"))[:420]
        goal.pop("description", None)
        goals.append(goal)
    return goals


def fetch_topic_metrics(
    connection: sqlite3.Connection,
    cds_code: str,
    *,
    topic: str = "chronic_absenteeism",
    limit: int = 5,
) -> list[dict[str, Any]]:
    config = topic_config(topic)
    match, params = text_match_sql(
        """
        coalesce(m.metric_name, '') || ' ' ||
        coalesce(m.baseline_raw, '') || ' ' ||
        coalesce(m.year_1_outcome_raw, '') || ' ' ||
        coalesce(m.year_2_outcome_raw, '') || ' ' ||
        coalesce(m.year_3_target_raw, '') || ' ' ||
        coalesce(m.current_difference_from_baseline_raw, '')
        """,
        tuple(config["terms"]),
    )
    rows = connection.execute(
        f"""
        select
          metric_id,
          goal_number,
          metric_number,
          metric_name,
          baseline_raw,
          year_1_outcome_raw,
          year_2_outcome_raw,
          year_3_target_raw,
          current_difference_from_baseline_raw,
          source_pages
        from lcap_metrics m
        where cds_code = ?
          and {match}
        order by goal_number, metric_number
        limit ?
        """,
        [cds_code, *params, limit],
    ).fetchall()
    return [dict(row) for row in rows]


def outcome_read(topic: str, trend: str, row: dict[str, Any]) -> str:
    config = topic_config(topic)
    change = float(row.get("outcome_change") or 0)
    lower_is_better = bool(config["lower_is_better"])
    if change == 0:
        return "flat"
    improving = change < 0 if lower_is_better else change > 0
    return "improving" if improving else "worsening"


def rows_to_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Rank | County | District | Current rate | Change | Affected students | Broad $ | Strict $ | Goals | Metrics | Score |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for index, row in enumerate(rows, start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(index),
                    str(row.get("county") or ""),
                    str(row.get("district") or ""),
                    f"{float(row.get('current_status') or 0):.1f}%",
                    f"{float(row.get('outcome_change') or 0):.1f} pts",
                    f"{int(row.get('affected_student_count') or 0):,}",
                    compact_money(row.get("broad_action_funds")),
                    compact_money(row.get("strict_action_funds")),
                    str(row.get("topic_goal_count") or 0),
                    str(row.get("topic_metric_count") or 0),
                    f"{float(row.get('opportunity_score') or 0):.1f}",
                ]
            )
            + " |"
        )
    return "\n".join(lines)
