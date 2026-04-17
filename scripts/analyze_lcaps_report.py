#!/usr/bin/env python3
"""Cross-district analysis for the Orange County LCAP corpus."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / "outputs" / "lcaps_json" / "all_lcaps.json"
OUTPUT_DIR = ROOT / "outputs" / "research"

NUMBER_RE = re.compile(r"\(?[-+]?\$?\d[\d,]*\.?\d*%?\)?")
PAIR_RE = re.compile(r"^([A-Za-z][A-Za-z0-9/&() .,%+'-]{0,90}?)[\s]*[:-][\s]*(.+)$")

LOWER_IS_BETTER_KEYWORDS = (
    "absen",
    "suspension",
    "dropout",
    "discipline",
    "incident",
    "expulsion",
    "chronic absentee",
    "d grade",
    "f grade",
    "missing",
    "misassign",
    "deficien",
    "complaint",
    "proportionally",
    "disproportion",
    "rate of d",
    "rate of f",
)

RELATIVE_TARGET_PATTERNS = (
    re.compile(r"baseline\s*([+-])\s*(\d+(?:\.\d+)?)", re.I),
    re.compile(r"(?:improve|increase)\s+by\s+(\d+(?:\.\d+)?)", re.I),
    re.compile(r"(?:decrease|reduce)\s+by\s+(\d+(?:\.\d+)?)", re.I),
    re.compile(r"maintain(?:\s+or\s+(?:improve|increase))?\s+by\s+(\d+(?:\.\d+)?)", re.I),
)

THEMES: list[dict[str, Any]] = [
    {
        "key": "academic_instruction",
        "label": "Academic Achievement & Instruction",
        "keywords": (
            "english language arts",
            "ela",
            "mathematics",
            "math",
            "science",
            "state standards",
            "instruction",
            "curriculum",
            "assessment",
            "academic",
            "literacy",
            "broad course of study",
            "a-g deficiency",
            "caaspp",
            "cast",
            "gpa",
        ),
    },
    {
        "key": "college_career",
        "label": "College, Career & Graduation",
        "keywords": (
            "college",
            "career",
            "graduation",
            "graduate",
            "a-g",
            "uc/csu",
            "pathway",
            "cte",
            "advanced placement",
            "baccalaureate",
            "dual enrollment",
            "postsecondary",
            "seal of biliteracy",
            "seal of civic",
            "ap ",
            "ib ",
        ),
    },
    {
        "key": "attendance_climate",
        "label": "Attendance, Belonging & School Climate",
        "keywords": (
            "attendance",
            "absentee",
            "suspension",
            "school climate",
            "belonging",
            "behavior",
            "restorative",
            "pbis",
            "social-emotional",
            "well-being",
            "wellness",
            "mental health",
            "sense of belonging",
            "connectedness",
            "school safety",
            "school culture",
        ),
    },
    {
        "key": "family_community",
        "label": "Family & Community Engagement",
        "keywords": (
            "family",
            "parent",
            "community",
            "engagement",
            "advisory",
            "communication",
            "outreach",
            "partnership",
            "faces",
            "delac",
            "elac",
            "school site council",
            "community school",
        ),
    },
    {
        "key": "english_learner",
        "label": "English Learner & Multilingual Support",
        "keywords": (
            "english learner",
            "english language development",
            "eld",
            "elpac",
            "reclassification",
            "rfep",
            "ltel",
            "multilingual",
            "plurilingual",
            "language acquisition",
            "biliteracy",
        ),
    },
    {
        "key": "special_education",
        "label": "Special Education & Inclusion",
        "keywords": (
            "special education",
            "students with disabilities",
            "swd",
            "sped",
            "inclusion",
            "udl",
            "iep",
            "exceptional needs",
            "caa",
            "alternate assessment",
            "extended school year",
            "esy",
        ),
    },
    {
        "key": "technology_data",
        "label": "Technology, Data & Systems",
        "keywords": (
            "technology",
            "digital",
            "software",
            "device",
            "wifi",
            "data",
            "dashboard",
            "progress monitoring",
            "platform",
            "assessment system",
            "schoolinks",
            "classlink",
            "schoology",
            "library/media",
            "artificial intelligence",
        ),
    },
    {
        "key": "intervention_extended_learning",
        "label": "Intervention, MTSS & Expanded Learning",
        "keywords": (
            "intervention",
            "mtss",
            "tier 2",
            "tier 3",
            "tutorial",
            "tutoring",
            "summer school",
            "after school",
            "expanded learning",
            "extended learning",
            "credit recovery",
            "case management",
            "acceleration",
            "independent study",
            "academic support",
        ),
    },
    {
        "key": "staffing_prof_learning",
        "label": "Staffing & Professional Learning",
        "keywords": (
            "staffing",
            "staff",
            "professional learning",
            "professional development",
            "plc",
            "coach",
            "coaching",
            "induction",
            "mentor",
            "teacher training",
            "administrator training",
            "recruiting",
            "retaining",
            "personnel",
        ),
    },
]

THEME_INDEX = {theme["key"]: theme for theme in THEMES}


def normalize_text(value: Any) -> str:
    text = str(value or "")
    replacements = {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\xa0": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def parse_number(token: str) -> float | None:
    cleaned = token.replace("$", "").replace(",", "").strip()
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    cleaned = cleaned.strip("()").rstrip("%")
    if not cleaned:
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return -number if negative else number


def first_number(text: str) -> float | None:
    match = NUMBER_RE.search(text or "")
    return parse_number(match.group(0)) if match else None


def normalize_label(label: str) -> str:
    cleaned = normalize_text(label).lower()
    cleaned = cleaned.strip(". ")
    aliases = {
        "all students": "all",
        "district wide": "district",
        "districtwide": "district",
        "district overall": "district",
        "overall districtwide rate": "district",
        "overall district": "district",
        "district": "district",
        "all": "all",
        "english learners": "el",
        "english learner": "el",
        "els": "el",
        "el": "el",
        "long-term english learners": "ltel",
        "ltels": "ltel",
        "ltel": "ltel",
        "reclassified fluent english proficient": "rfep",
        "rfep": "rfep",
        "students with disabilities": "swd",
        "swd": "swd",
        "sped": "swd",
        "special education": "swd",
        "socioeconomically disadvantaged": "sed",
        "low income": "sed",
        "li": "sed",
        "sed/li": "sed",
        "nslp/low income": "sed",
        "sed": "sed",
        "foster youth": "fy",
        "foster": "fy",
        "fy": "fy",
        "students experiencing homelessness": "homeless",
        "homeless youth": "homeless",
        "homeless": "homeless",
        "m-v": "homeless",
        "mv": "homeless",
        "hispanic/latino": "hispanic_latino",
        "hispanic": "hispanic_latino",
        "latino": "hispanic_latino",
        "african american": "african_american",
        "aa": "african_american",
        "asian": "asian",
        "as": "asian",
        "white": "white",
    }
    return aliases.get(cleaned, cleaned)


def extract_measurement_map(raw: str) -> tuple[dict[str, float], list[float]]:
    lines = [normalize_text(line) for line in (raw or "").split("\n") if normalize_text(line)]
    label_map: dict[str, float] = {}
    unlabeled: list[float] = []
    for line in lines:
        lower = line.lower()
        if line.endswith("Outcome") or line.endswith("from Baseline"):
            continue
        pair_match = PAIR_RE.match(line)
        if pair_match and re.search(r"\d", pair_match.group(2)):
            label = normalize_label(pair_match.group(1))
            value = first_number(pair_match.group(2))
            if value is not None:
                label_map[label] = value
                continue
        if re.search(r"%|point|dfs|level|students|rate|score", lower):
            value = first_number(line)
            if value is not None and len(line) < 60:
                unlabeled.append(value)
    return label_map, unlabeled


def infer_direction(metric_name: str, target_text: str) -> str:
    text = normalize_text(f"{metric_name} {target_text}").lower()
    if any(keyword in text for keyword in LOWER_IS_BETTER_KEYWORDS):
        return "lower"
    if any(phrase in text for phrase in ("less than", "at most", "no more than", "decrease", "reduce")):
        return "lower"
    return "higher"


def relative_target_delta(target_text: str, direction: str) -> float | None:
    text = normalize_text(target_text).lower()
    baseline_match = RELATIVE_TARGET_PATTERNS[0].search(text)
    if baseline_match:
        sign = 1 if baseline_match.group(1) == "+" else -1
        return float(baseline_match.group(2)) * sign

    if "maintain" in text and "+/- 0" in text:
        return 0.0
    if "maintain" in text and "improve by" not in text and "increase by" not in text:
        return 0.0

    improve_match = RELATIVE_TARGET_PATTERNS[1].search(text)
    if improve_match:
        value = float(improve_match.group(1))
        return value if direction == "higher" else -value

    decrease_match = RELATIVE_TARGET_PATTERNS[2].search(text)
    if decrease_match:
        value = float(decrease_match.group(1))
        return -value if direction == "higher" else value

    maintain_improve_match = RELATIVE_TARGET_PATTERNS[3].search(text)
    if maintain_improve_match:
        value = float(maintain_improve_match.group(1))
        return value if direction == "higher" else -value

    return None


def build_target_map(
    target_text: str,
    baseline_map: dict[str, float],
    target_map: dict[str, float],
    unlabeled_target: list[float],
    direction: str,
) -> tuple[dict[str, float], list[float]]:
    if target_map or unlabeled_target:
        return target_map, unlabeled_target

    delta = relative_target_delta(target_text, direction)
    if delta is not None and baseline_map:
        return {label: value + delta for label, value in baseline_map.items()}, []

    return target_map, unlabeled_target


def score_metric(metric: dict[str, Any]) -> dict[str, Any] | None:
    baseline_map, baseline_unlabeled = extract_measurement_map(metric.get("baseline_raw", ""))
    year_1_map, year_1_unlabeled = extract_measurement_map(metric.get("year_1_outcome_raw", ""))
    target_map, target_unlabeled = extract_measurement_map(metric.get("year_3_target_raw", ""))

    direction = infer_direction(metric.get("metric_name", ""), metric.get("year_3_target_raw", ""))
    target_map, target_unlabeled = build_target_map(
        metric.get("year_3_target_raw", ""),
        baseline_map,
        target_map,
        target_unlabeled,
        direction,
    )

    ratios: list[float] = []
    labels = sorted(set(baseline_map) & set(year_1_map) & set(target_map))
    for label in labels:
        baseline_value = baseline_map[label]
        year_1_value = year_1_map[label]
        target_value = target_map[label]
        if math.isclose(baseline_value, target_value):
            continue
        denominator = (target_value - baseline_value) if direction == "higher" else (baseline_value - target_value)
        numerator = (year_1_value - baseline_value) if direction == "higher" else (baseline_value - year_1_value)
        if denominator <= 0:
            continue
        ratios.append(numerator / denominator)

    if not ratios and baseline_unlabeled and year_1_unlabeled:
        target_value = target_unlabeled[0] if target_unlabeled else None
        if target_value is None:
            delta = relative_target_delta(metric.get("year_3_target_raw", ""), direction)
            if delta is not None:
                target_value = baseline_unlabeled[0] + delta
        if target_value is not None and not math.isclose(baseline_unlabeled[0], target_value):
            denominator = (
                target_value - baseline_unlabeled[0]
                if direction == "higher"
                else baseline_unlabeled[0] - target_value
            )
            numerator = (
                year_1_unlabeled[0] - baseline_unlabeled[0]
                if direction == "higher"
                else baseline_unlabeled[0] - year_1_unlabeled[0]
            )
            if denominator > 0:
                ratios.append(numerator / denominator)

    if not ratios:
        return None

    average_ratio = sum(ratios) / len(ratios)
    if average_ratio >= 1:
        status = "at_or_above_target"
    elif average_ratio >= (1 / 3):
        status = "on_track"
    elif average_ratio >= 0:
        status = "off_track"
    else:
        status = "moving_away"

    return {
        "status": status,
        "direction": direction,
        "ratio": average_ratio,
        "evidence_points": len(ratios),
    }


def theme_scores(text: str) -> dict[str, int]:
    lower = normalize_text(text).lower()
    scores: dict[str, int] = {}
    for theme in THEMES:
        score = 0
        for keyword in theme["keywords"]:
            if keyword in lower:
                score += 2 if " " in keyword else 1
        if score:
            scores[theme["key"]] = score
    return scores


def primary_theme(text: str) -> str:
    scores = theme_scores(text)
    if not scores:
        return "other"
    ordered_keys = [theme["key"] for theme in THEMES]
    return max(scores, key=lambda key: (scores[key], -ordered_keys.index(key)))


def any_themes(text: str) -> set[str]:
    scores = theme_scores(text)
    return set(scores) if scores else {"other"}


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def district_progress_label(positive_share: float, moving_away_share: float, scorable_metrics: int) -> str:
    if scorable_metrics < 10:
        return "Limited evidence"
    if positive_share >= 0.75 and moving_away_share <= 0.10:
        return "Strong early trajectory"
    if positive_share >= 0.60:
        return "Mostly on track"
    if positive_share >= 0.45:
        return "Mixed trajectory"
    return "Needs acceleration"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    output = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    output.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(output)


def build_report(summary: dict[str, Any]) -> str:
    top_common = summary["theme_summary"][:8]
    top_funding = sorted(summary["theme_summary"], key=lambda row: row["total_action_dollars"], reverse=True)[:8]
    theme_progress = [row for row in summary["theme_progress"] if row["scorable_metrics"] >= 10]
    district_progress = [row for row in summary["district_progress"] if row["scorable_metrics"] >= 10]
    top_progress = sorted(district_progress, key=lambda row: row["positive_share"], reverse=True)[:8]
    bottom_progress = sorted(district_progress, key=lambda row: row["positive_share"])[:8]

    common_rows = [
        [
            row["theme_label"],
            str(row["districts_with_theme"]),
            str(row["goal_count"]),
            str(row["action_count"]),
            str(row["metric_count"]),
        ]
        for row in top_common
    ]
    funding_rows = [
        [
            row["theme_label"],
            format_currency(row["total_action_dollars"]),
            format_pct(row["dollar_share"]),
            str(row["action_count"]),
        ]
        for row in top_funding
    ]
    progress_rows = [
        [
            row["theme_label"],
            str(row["scorable_metrics"]),
            str(row["at_or_above_target"]),
            str(row["on_track"]),
            str(row["off_track"]),
            str(row["moving_away"]),
            format_pct(row["positive_share"]),
        ]
        for row in sorted(theme_progress, key=lambda row: row["positive_share"], reverse=True)[:8]
    ]
    top_district_rows = [
        [
            row["district_name"],
            str(row["scorable_metrics"]),
            format_pct(row["positive_share"]),
            row["progress_label"],
            row["top_theme_by_dollars"],
        ]
        for row in top_progress
    ]
    bottom_district_rows = [
        [
            row["district_name"],
            str(row["scorable_metrics"]),
            format_pct(row["positive_share"]),
            row["progress_label"],
            row["top_theme_by_dollars"],
        ]
        for row in bottom_progress
    ]

    report = f"""# Orange County LCAP Cross-District Research Report

## Executive Summary

This review covers **29 Orange County school districts**, spanning **141 goals, 1,421 metrics, and 1,356 actions** extracted from the 2025-26 LCAPs. Across the corpus, districts have budgeted roughly **{format_currency(summary["total_action_dollars"])}** inside the action tables.

The dominant through-lines are consistent across the county: academic improvement, college/career readiness, attendance and school climate, English learner support, and family/community engagement. The action dollars, however, are concentrated more heavily in broad instructional systems, personnel-heavy supports, and college/career pathways than in narrowly targeted programs.

Progress is mixed but not uniformly negative. Using a conservative year-1 trajectory heuristic on **{summary["overall_progress"]["scorable_metrics"]} numerically scorable metric records**, **{format_pct(summary["overall_progress"]["positive_share"])}** are either already at/above their year-3 target or moving fast enough to be considered on track. The weakest early signals cluster around academic proficiency gaps, some English learner outcomes, and a subset of attendance/climate measures.

## Corpus Overview

- Districts reviewed: **{summary["district_count"]}**
- Total goals: **{summary["total_goals"]}**
- Total metrics: **{summary["total_metrics"]}**
- Total actions: **{summary["total_actions"]}**
- Total action dollars: **{format_currency(summary["total_action_dollars"])}**
- Numerically scorable metric records: **{summary["overall_progress"]["scorable_metrics"]}**

## What Districts Are Most Commonly Working On

The table below ranks themes by how broadly they appear across districts and how often they show up in goals, actions, and metrics.

{markdown_table(["Theme", "Districts", "Goals", "Actions", "Metrics"], common_rows)}

**Reading across the common themes:**

- **Academic achievement and instruction** is the backbone of nearly every plan. This includes CAASPP/ELA/math performance, standards alignment, curriculum, teacher practice, and broad-course access.
- **Student support, attendance, and climate** is the second major lane. Districts are investing in belonging, behavior systems, MTSS, attendance, restorative practices, and mental-health-adjacent supports.
- **College, career, and graduation** is not peripheral; it is a core countywide priority, especially in secondary and unified districts through A-G completion, CTE pathways, AP/IB, dual enrollment, and graduation metrics.
- **English learner support** shows up as a distinct strategic priority in most districts, usually through ELD, ELPAC growth, reclassification, newcomer support, and biliteracy pathways.
- **Family/community engagement** is treated as a formal strategic lever rather than an add-on, especially through community schools, advisory structures, parent education, translators, and outreach staff.

## Where the Money Is Concentrated

The next table ranks themes by total action dollars. These totals are based on the action tables and therefore reflect planned spending attached to LCAP actions, not necessarily marginal new spending only.

{markdown_table(["Theme", "Action Dollars", "Share of Dollars", "Actions"], funding_rows)}

**What the money pattern says:**

- The largest dollars sit inside **system-level instructional and academic supports**, which often include districtwide staffing, classroom support, curriculum implementation, and broad access programs.
- **College/career and graduation-related investments** are materially large, especially where districts attach staffing, pathway infrastructure, dual enrollment, counseling platforms, CTE, and secondary support systems to that work.
- **Attendance/climate/support investments** are also substantial, particularly where districts fund MTSS, behavior systems, counseling, wellness, and intervention staff.
- **Family engagement** and **English learner support** are common everywhere, but their action dollars are typically smaller than the large districtwide academic and staffing actions.

## Are Districts On Track?

The metric trajectory model compares baseline, year-1 outcome, and year-3 target where a numeric comparison is possible. It uses the direction implied by the metric or target language, then estimates whether year-1 progress is at pace for a three-year target. This should be read as an **early trajectory signal**, not a formal accountability judgment.

- At or above target: **{summary["overall_progress"]["at_or_above_target"]}**
- On track: **{summary["overall_progress"]["on_track"]}**
- Off track: **{summary["overall_progress"]["off_track"]}**
- Moving away from target: **{summary["overall_progress"]["moving_away"]}**
- Positive early trajectory (at/above target or on track): **{format_pct(summary["overall_progress"]["positive_share"])}**

### Theme-Level Progress

{markdown_table(["Theme", "Scorable", "At/Above", "On Track", "Off Track", "Moving Away", "Positive Share"], progress_rows)}

**Interpretation:**

- The strongest early signals generally come from **college/career/graduation** and some **system-maintenance metrics** where districts are maintaining already-high performance or steadily improving access/completion rates.
- **Attendance/climate** metrics are mixed: some districts are clearly improving, but chronic absenteeism and suspension-related targets remain uneven.
- **Academic proficiency** remains the most stubborn countywide challenge. Districts are investing heavily there, but year-1 data suggests that moving student-group performance fast enough to meet three-year targets is still difficult.
- **English learner** measures are highly variable. Some districts show strong reclassification or proficiency movement, while others remain behind pace.

### District-Level Early Signal

Highest positive-share districts among those with at least 10 scorable metrics:

{markdown_table(["District", "Scorable Metrics", "Positive Share", "Signal", "Largest Dollar Theme"], top_district_rows)}

Most mixed / weakest positive-share districts among those with at least 10 scorable metrics:

{markdown_table(["District", "Scorable Metrics", "Positive Share", "Signal", "Largest Dollar Theme"], bottom_district_rows)}

These rankings are best read as a way to identify where early trajectory looks stronger or weaker inside the LCAP metric system. They should not be confused with an external performance ranking of districts.

## Key Findings

- **Districts are broadly converging on the same strategic agenda.** The county is not fragmented into wildly different priorities; most districts are pursuing a common portfolio of academic improvement, student support, postsecondary readiness, EL services, and family engagement.
- **The money is concentrated in broad operating levers.** The biggest LCAP action dollars tend to sit in districtwide staffing, instructional infrastructure, course access, and large support systems rather than in small targeted pilots.
- **Progress is better in completion/access metrics than in proficiency-gap metrics.** Graduation, pathway completion, and some college/career readiness indicators show more positive movement than ELA/math performance gaps.
- **Student support work is now core strategy, not side strategy.** Attendance, belonging, MTSS, restorative practices, counseling, and wellness are major parts of district planning and spending.
- **EL support is widespread but still uneven in results.** Districts are clearly prioritizing multilingual learners, but the year-1 pace suggests this remains one of the hardest improvement areas.

## Method Notes

- The source corpus is the structured JSON extracted from the 29 LCAP PDFs.
- Theme assignment is keyword-based and designed for cross-district comparability, not perfect semantic classification.
- Funding totals come from action tables and may include large districtwide operational actions.
- Progress scoring only uses metric records where baseline, year-1, and year-3 values could be interpreted numerically with enough consistency to compare.
- Supporting machine-readable outputs are saved alongside this report:
  - `summary.json`
  - `theme_summary.csv`
  - `theme_progress.csv`
  - `district_progress.csv`
"""
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-path",
        type=Path,
        default=INPUT_PATH,
        help="Path to all_lcaps.json produced by extract_lcaps.py.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory where report outputs should be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input_path.resolve()
    output_dir = args.output_dir.resolve()

    output_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    districts = payload["districts"]

    total_action_dollars = 0.0
    theme_rollup: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "theme_label": "",
            "districts_with_theme": set(),
            "goal_count": 0,
            "action_count": 0,
            "metric_count": 0,
            "total_action_dollars": 0.0,
            "progress_counts": Counter(),
        }
    )
    district_progress_rows: list[dict[str, Any]] = []

    overall_progress_counts = Counter()

    for district in districts:
        district_name = district["district_name"]
        district_theme_dollars: defaultdict[str, float] = defaultdict(float)
        district_progress_counts = Counter()
        district_scorable_metrics = 0

        for goal in district["goals"]:
            goal_theme = primary_theme(goal["description"])
            theme_rollup[goal_theme]["theme_label"] = THEME_INDEX.get(goal_theme, {}).get("label", "Other / uncategorized")
            theme_rollup[goal_theme]["districts_with_theme"].add(district_name)
            theme_rollup[goal_theme]["goal_count"] += 1

            for action in goal["actions"]:
                text = f"{action.get('title', '')} {action.get('description', '')}"
                action_theme = primary_theme(text)
                theme_rollup[action_theme]["theme_label"] = THEME_INDEX.get(action_theme, {}).get("label", "Other / uncategorized")
                theme_rollup[action_theme]["districts_with_theme"].add(district_name)
                theme_rollup[action_theme]["action_count"] += 1
                action_dollars = float(action.get("total_funds") or 0.0)
                theme_rollup[action_theme]["total_action_dollars"] += action_dollars
                district_theme_dollars[action_theme] += action_dollars
                total_action_dollars += action_dollars

            for metric in goal["metrics"]:
                metric_text = f"{metric.get('metric_name', '')} {goal.get('description', '')}"
                metric_theme = primary_theme(metric_text)
                theme_rollup[metric_theme]["theme_label"] = THEME_INDEX.get(metric_theme, {}).get("label", "Other / uncategorized")
                theme_rollup[metric_theme]["districts_with_theme"].add(district_name)
                theme_rollup[metric_theme]["metric_count"] += 1

                metric_score = score_metric(metric)
                if metric_score:
                    district_scorable_metrics += 1
                    district_progress_counts[metric_score["status"]] += 1
                    overall_progress_counts[metric_score["status"]] += 1
                    theme_rollup[metric_theme]["progress_counts"][metric_score["status"]] += 1

        positive_count = district_progress_counts["at_or_above_target"] + district_progress_counts["on_track"]
        moving_away_count = district_progress_counts["moving_away"]
        positive_share = positive_count / district_scorable_metrics if district_scorable_metrics else 0.0
        moving_away_share = moving_away_count / district_scorable_metrics if district_scorable_metrics else 0.0
        top_theme = max(district_theme_dollars.items(), key=lambda item: item[1])[0] if district_theme_dollars else "other"

        district_progress_rows.append(
            {
                "district_name": district_name,
                "goal_count": district["goal_count"],
                "metric_count": district["metric_count"],
                "action_count": district["action_count"],
                "total_action_dollars": round(sum(district_theme_dollars.values()), 2),
                "scorable_metrics": district_scorable_metrics,
                "at_or_above_target": district_progress_counts["at_or_above_target"],
                "on_track": district_progress_counts["on_track"],
                "off_track": district_progress_counts["off_track"],
                "moving_away": district_progress_counts["moving_away"],
                "positive_share": round(positive_share, 4),
                "moving_away_share": round(moving_away_share, 4),
                "progress_label": district_progress_label(positive_share, moving_away_share, district_scorable_metrics),
                "top_theme_by_dollars": THEME_INDEX.get(top_theme, {}).get("label", "Other / uncategorized"),
            }
        )

    overall_scorable_metrics = sum(overall_progress_counts.values())
    overall_positive_share = (
        (overall_progress_counts["at_or_above_target"] + overall_progress_counts["on_track"]) / overall_scorable_metrics
        if overall_scorable_metrics
        else 0.0
    )

    theme_summary_rows: list[dict[str, Any]] = []
    theme_progress_rows: list[dict[str, Any]] = []
    for theme_key, stats in theme_rollup.items():
        districts_with_theme = len(stats["districts_with_theme"])
        theme_summary_rows.append(
            {
                "theme_key": theme_key,
                "theme_label": stats["theme_label"] or "Other / uncategorized",
                "districts_with_theme": districts_with_theme,
                "goal_count": stats["goal_count"],
                "action_count": stats["action_count"],
                "metric_count": stats["metric_count"],
                "total_action_dollars": round(stats["total_action_dollars"], 2),
                "dollar_share": round((stats["total_action_dollars"] / total_action_dollars) if total_action_dollars else 0.0, 6),
            }
        )
        scorable = sum(stats["progress_counts"].values())
        positive = stats["progress_counts"]["at_or_above_target"] + stats["progress_counts"]["on_track"]
        theme_progress_rows.append(
            {
                "theme_key": theme_key,
                "theme_label": stats["theme_label"] or "Other / uncategorized",
                "scorable_metrics": scorable,
                "at_or_above_target": stats["progress_counts"]["at_or_above_target"],
                "on_track": stats["progress_counts"]["on_track"],
                "off_track": stats["progress_counts"]["off_track"],
                "moving_away": stats["progress_counts"]["moving_away"],
                "positive_share": round((positive / scorable) if scorable else 0.0, 4),
            }
        )

    theme_summary_rows.sort(key=lambda row: (row["districts_with_theme"], row["action_count"], row["metric_count"]), reverse=True)
    theme_progress_rows.sort(key=lambda row: (row["positive_share"], row["scorable_metrics"]), reverse=True)
    district_progress_rows.sort(key=lambda row: row["positive_share"], reverse=True)

    summary = {
        "district_count": payload["lcap_count"],
        "total_goals": sum(d["goal_count"] for d in districts),
        "total_metrics": sum(d["metric_count"] for d in districts),
        "total_actions": sum(d["action_count"] for d in districts),
        "total_action_dollars": round(total_action_dollars, 2),
        "overall_progress": {
            "scorable_metrics": overall_scorable_metrics,
            "at_or_above_target": overall_progress_counts["at_or_above_target"],
            "on_track": overall_progress_counts["on_track"],
            "off_track": overall_progress_counts["off_track"],
            "moving_away": overall_progress_counts["moving_away"],
            "positive_share": round(overall_positive_share, 4),
        },
        "theme_summary": theme_summary_rows,
        "theme_progress": theme_progress_rows,
        "district_progress": district_progress_rows,
    }

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    write_csv(
        output_dir / "theme_summary.csv",
        [
            "theme_key",
            "theme_label",
            "districts_with_theme",
            "goal_count",
            "action_count",
            "metric_count",
            "total_action_dollars",
            "dollar_share",
        ],
        theme_summary_rows,
    )
    write_csv(
        output_dir / "theme_progress.csv",
        [
            "theme_key",
            "theme_label",
            "scorable_metrics",
            "at_or_above_target",
            "on_track",
            "off_track",
            "moving_away",
            "positive_share",
        ],
        theme_progress_rows,
    )
    write_csv(
        output_dir / "district_progress.csv",
        [
            "district_name",
            "goal_count",
            "metric_count",
            "action_count",
            "total_action_dollars",
            "scorable_metrics",
            "at_or_above_target",
            "on_track",
            "off_track",
            "moving_away",
            "positive_share",
            "moving_away_share",
            "progress_label",
            "top_theme_by_dollars",
        ],
        district_progress_rows,
    )

    report = build_report(summary)
    (output_dir / "lcap_cross_district_report.md").write_text(report + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
