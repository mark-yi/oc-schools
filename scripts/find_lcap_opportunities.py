#!/usr/bin/env python3
"""Find GTM opportunities from Dashboard outcomes plus LCAP budget evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lcap_opportunities import DEFAULT_ANALYTICS_DB, find_opportunities, normalize_text, rows_to_markdown


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_ANALYTICS_DB)
    parser.add_argument("--topic", default="chronic_absenteeism")
    parser.add_argument(
        "--outcome-trend",
        default="worsening",
        choices=["improving", "worsening", "decreasing_rate", "increasing_rate", "any"],
        help=(
            "Business outcome direction. For chronic absenteeism, worsening means the rate increased; "
            "decreasing_rate means the rate declined."
        ),
    )
    parser.add_argument(
        "--rank-by",
        default="strict_action_funds",
        choices=[
            "strict_action_funds",
            "broad_action_funds",
            "affected_student_count",
            "current_status",
            "outcome_change",
            "opportunity_score",
        ],
    )
    parser.add_argument("--county")
    parser.add_argument("--district", help="Exact district name, or use * as a wildcard.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--action-limit", type=int, default=3)
    parser.add_argument("--no-actions", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = find_opportunities(
        topic=args.topic,
        outcome_trend=args.outcome_trend,
        rank_by=args.rank_by,
        county=args.county,
        district=args.district,
        limit=args.limit,
        include_actions=not args.no_actions,
        action_limit=args.action_limit,
        db_path=args.db_path,
    )
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return
    print(rows_to_markdown(rows))
    if rows and not args.no_actions:
        print("\nTop evidence actions:")
        for index, row in enumerate(rows[: min(len(rows), 8)], start=1):
            print(f"\n{index}. {row['district']} ({row['county']})")
            for action in row.get("top_actions", []):
                title = normalize_text(action.get("title") or "")
                funds = "${:,.0f}".format(float(action.get("total_funds") or 0))
                pages = action.get("source_pages") or ""
                read = action.get("sales_read") or ""
                print(f"- Action {action.get('action_number')}: {title} - {funds}; pages {pages}. {read}")


if __name__ == "__main__":
    main()
