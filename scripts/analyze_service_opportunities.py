#!/usr/bin/env python3
"""Find LCAP problem areas that look commercially relevant for district services."""

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
OUTPUT_DIR = ROOT / "outputs" / "opportunity_research"

NUMBER_RE = re.compile(r"\(?[-+]?\$?\d[\d,]*\.?\d*%?\)?")
PAIR_RE = re.compile(r"^([A-Za-z][A-Za-z0-9/&() .,%+'-]{0,100}?)[\s]*[:-][\s]*(.+)$")

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
    "rate of d",
    "rate of f",
    "tier 3",
    "recommended for tier 3",
    "needing tier 3",
    "needs tier 3",
    "students recommended",
    "students needing",
)


OPPORTUNITY_AREAS: list[dict[str, Any]] = [
    {
        "key": "chronic_absenteeism",
        "label": "Chronic Absenteeism & Attendance Recovery",
        "metric_keywords": (
            "chronic absenteeism",
            "attendance rate",
            "absentee",
            "truancy",
        ),
        "action_keywords": (
            "attendance",
            "absentee",
            "truancy",
            "re-engagement",
            "attendance intervention",
            "home visit",
            "attendance support",
        ),
        "service_hypothesis": "Attendance early-warning, multilingual family outreach, case management workflows, and student re-engagement services.",
    },
    {
        "key": "school_climate_suspension",
        "label": "School Climate, Behavior & Suspension",
        "metric_keywords": (
            "suspension",
            "school climate",
            "discipline",
            "belonging",
            "connectedness",
            "school safety",
            "adult they trust",
            "happy to be at this school",
        ),
        "action_keywords": (
            "pbis",
            "restorative",
            "discipline",
            "behavior",
            "school climate",
            "belonging",
            "connectedness",
            "student behavior",
            "restorative practices",
        ),
        "service_hypothesis": "Behavior support systems, restorative-practice implementation, school-climate coaching, and student support programming.",
    },
    {
        "key": "ela_proficiency",
        "label": "ELA Proficiency & Literacy",
        "metric_keywords": (
            "english language arts",
            "ela",
            "literacy",
            "reading",
            "writing",
            "caaspp - ela",
            "caaspp ela",
            "sbac) english language arts",
            "research/inquiry",
        ),
        "action_keywords": (
            "english language arts",
            "ela",
            "literacy",
            "reading",
            "writing",
            "reading intervention",
            "literacy intervention",
        ),
        "service_hypothesis": "High-dosage literacy intervention, instructional coaching, assessment/data cycles, and targeted tutoring support.",
    },
    {
        "key": "math_proficiency",
        "label": "Math Proficiency",
        "metric_keywords": (
            "mathematics",
            "math",
            "caaspp - math",
            "caaspp math",
            "sbac) mathematics",
            "algebra",
            "numeracy",
        ),
        "action_keywords": (
            "math",
            "mathematics",
            "algebra",
            "numeracy",
            "math intervention",
        ),
        "service_hypothesis": "Math intervention, acceleration supports, classroom coaching, and benchmark-driven instructional services.",
    },
    {
        "key": "english_learner_progress",
        "label": "English Learner Progress & Reclassification",
        "metric_keywords": (
            "english learner",
            "elpac",
            "reclassification",
            "rfep",
            "ltel",
            "elpi",
        ),
        "action_keywords": (
            "english learner",
            "english language development",
            "eld",
            "reclassification",
            "rfep",
            "ltel",
            "newcomer",
            "multilingual",
            "plurilingual",
        ),
        "service_hypothesis": "ELD program support, reclassification tracking, newcomer services, multilingual family engagement, and teacher coaching.",
    },
    {
        "key": "graduation_credit_recovery",
        "label": "Graduation, Credit Recovery & Alternative Pathways",
        "metric_keywords": (
            "graduation",
            "dropout",
            "credit recovery",
        ),
        "action_keywords": (
            "graduation",
            "credit recovery",
            "continuation",
            "alternative school",
            "independent study",
            "case management",
            "on-time graduation",
        ),
        "service_hypothesis": "Credit-recovery programs, mentoring/case management, alternative pathway support, and early-warning graduation services.",
    },
    {
        "key": "college_career_readiness",
        "label": "College, Career & Postsecondary Readiness",
        "metric_keywords": (
            "college and career",
            "college/career",
            "college/career indicator",
            "a-g",
            "uc/csu",
            "cte",
            "pathway",
            "career readiness",
            "postsecondary",
            "college going",
            "seal of biliteracy",
        ),
        "action_keywords": (
            "college and career",
            "college/career",
            "a-g",
            "uc/csu",
            "cte",
            "pathway",
            "dual enrollment",
            "advanced placement",
            "baccalaureate",
            "career readiness",
            "postsecondary",
            "college going",
            "seal of biliteracy",
        ),
        "service_hypothesis": "College/career pathway design, advising systems, pathway activation, dual-enrollment operations, and postsecondary planning platforms.",
    },
    {
        "key": "family_community_engagement",
        "label": "Family & Community Engagement",
        "metric_keywords": (
            "family",
            "parent",
            "family survey",
            "parent survey",
            "parent engagement",
            "family engagement",
            "community engagement",
            "delac",
            "elac",
            "school site council",
        ),
        "action_keywords": (
            "family",
            "parent",
            "community engagement",
            "family engagement",
            "parent engagement",
            "outreach",
            "translator",
            "community school",
            "advisory",
            "delac",
            "elac",
            "family advocacy",
        ),
        "service_hypothesis": "Multilingual family engagement platforms, community outreach services, workshop programming, and parent activation campaigns.",
    },
    {
        "key": "mental_health_wellness",
        "label": "Mental Health, SEL & Wellness",
        "metric_keywords": (
            "mental health",
            "social-emotional",
            "well-being",
            "wellness",
            "socioemotional",
        ),
        "action_keywords": (
            "mental health",
            "wellness",
            "social-emotional",
            "sel",
            "counseling",
            "psychologist",
            "social worker",
            "well-being",
            "trauma-informed",
        ),
        "service_hypothesis": "Mental health program delivery, wellness curricula, SEL implementation, counseling supports, and referral/case-management tools.",
    },
    {
        "key": "mtss_intervention",
        "label": "MTSS, Tutoring & Targeted Intervention",
        "metric_keywords": (
            "tier 2",
            "tier 3",
            "expanded learning",
            "extended learning",
            "intervention",
            "progress monitoring",
        ),
        "action_keywords": (
            "mtss",
            "intervention",
            "tutoring",
            "tutorial",
            "tier 2",
            "tier 3",
            "summer school",
            "after school",
            "extended learning",
            "expanded learning",
            "progress monitoring",
        ),
        "service_hypothesis": "MTSS implementation support, tutoring operations, expanded-learning programming, and intervention workflow/data tools.",
    },
    {
        "key": "special_education_inclusion",
        "label": "Special Education & Inclusion",
        "metric_keywords": (
            "special education",
            "students with disabilities",
            "swd",
            "caa",
            "exceptional needs",
            "esy",
        ),
        "action_keywords": (
            "special education",
            "students with disabilities",
            "swd",
            "sped",
            "inclusion",
            "udl",
            "caa",
            "exceptional needs",
            "esy",
        ),
        "service_hypothesis": "Inclusive instruction support, SPED program services, progress monitoring, extended school year supports, and compliance/implementation tools.",
    },
    {
        "key": "data_technology",
        "label": "Data Systems & Instructional Technology",
        "metric_keywords": (
            "progress monitoring",
            "technology to access curriculum",
            "online assessment",
        ),
        "action_keywords": (
            "technology",
            "software",
            "platform",
            "device",
            "wifi",
            "data system",
            "data warehouse",
            "schoolinks",
            "classlink",
            "schoology",
            "digital",
        ),
        "service_hypothesis": "Instructional technology, student support platforms, progress dashboards, workflow systems, and district data integration.",
    },
]

AREA_INDEX = {area["key"]: area for area in OPPORTUNITY_AREAS}


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
    return text.strip()


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


def infer_direction(metric_name: str, text: str) -> str:
    lowered = normalize_text(f"{metric_name} {text}").lower()
    if any(keyword in lowered for keyword in LOWER_IS_BETTER_KEYWORDS):
        return "lower"
    return "higher"


def area_scores(text: str) -> dict[str, int]:
    lowered = normalize_text(text).lower()
    scores: dict[str, int] = {}
    for area in OPPORTUNITY_AREAS:
        score = 0
        for keyword in area["metric_keywords"]:
            if keyword in lowered:
                score += 2 if " " in keyword else 1
        if score:
            scores[area["key"]] = score
    return scores


def area_scores_for_kind(text: str, kind: str) -> dict[str, int]:
    lowered = normalize_text(text).lower()
    scores: dict[str, int] = {}
    field = {
        "metric": "metric_keywords",
        "action": "action_keywords",
        "goal": "metric_keywords",
    }[kind]
    for area in OPPORTUNITY_AREAS:
        score = 0
        for keyword in area[field]:
            if keyword in lowered:
                score += 2 if " " in keyword else 1
        if score:
            scores[area["key"]] = score
    return scores


def matched_areas(text: str) -> set[str]:
    scores = area_scores(text)
    return set(scores) if scores else set()


def primary_area(text: str) -> str | None:
    scores = area_scores(text)
    if not scores:
        return None
    order = [area["key"] for area in OPPORTUNITY_AREAS]
    return max(scores, key=lambda key: (scores[key], -order.index(key)))


def matched_areas_for_kind(text: str, kind: str) -> set[str]:
    scores = area_scores_for_kind(text, kind)
    return set(scores) if scores else set()


def procurement_profile(action: dict[str, Any]) -> tuple[str, float]:
    text = normalize_text(f"{action.get('title', '')} {action.get('description', '')}").lower()
    if any(keyword in text for keyword in ("software", "platform", "license", "online", "app", "digital tool", "schoolinks", "classlink", "schoology", "warehouse")):
        return "technology_platform", 0.90
    if any(keyword in text for keyword in ("consultant", "coaching", "coach", "professional development", "training", "workshop", "technical assistance")):
        return "coaching_pd_services", 0.70
    if any(keyword in text for keyword in ("tutoring", "tutorial", "after school", "summer school", "intervention", "case management", "mentoring")):
        return "program_services", 0.65
    if any(keyword in text for keyword in ("community partner", "partnership", "translator", "outreach", "engagement specialist", "mental health", "counseling", "wellness")):
        return "community_support_services", 0.60
    if any(keyword in text for keyword in ("curriculum", "materials", "books", "supplies", "resources", "assessment", "devices", "hot spot", "hotspot")):
        return "materials_resources", 0.55
    if any(keyword in text for keyword in ("staffing", "staff", "teacher", "administrator", "classified", "personnel", "salary", "specialist", "tosa", "fte", "psychologist", "social worker", "nurse")):
        return "internal_staffing", 0.20
    return "mixed_or_unclear", 0.40


def parse_current_difference(metric: dict[str, Any]) -> list[dict[str, Any]]:
    lines = [
        normalize_text(line)
        for line in (metric.get("current_difference_from_baseline_raw") or "").split("\n")
        if normalize_text(line)
    ]
    direction = infer_direction(metric.get("metric_name", ""), metric.get("current_difference_from_baseline_raw", ""))
    parsed: list[dict[str, Any]] = []

    for line in lines:
        lowered = line.lower()
        label = None
        value = None
        pair_match = PAIR_RE.match(line)
        if pair_match and re.search(r"\d", pair_match.group(2)):
            label = normalize_text(pair_match.group(1))
            value = first_number(pair_match.group(2))
        else:
            value = first_number(line)

        if value is None and "no difference" in lowered:
            parsed.append({"label": label, "value": 0.0, "movement": "flat", "raw": line})
            continue

        if value is None:
            continue

        if "improvement" in lowered or "improved" in lowered:
            movement = "improving"
        elif "maintain" in lowered or "no difference" in lowered:
            movement = "flat"
        elif direction == "higher":
            movement = "improving" if value > 0 else "worsening" if value < 0 else "flat"
        else:
            movement = "improving" if value < 0 else "worsening" if value > 0 else "flat"

        parsed.append({"label": label, "value": value, "movement": movement, "raw": line})

    return parsed


def pace_status(metric: dict[str, Any]) -> dict[str, Any] | None:
    # Reuse a simple single-value pace estimate when possible.
    def parse_map(raw: str) -> tuple[dict[str, float], list[float]]:
        lines = [normalize_text(line) for line in (raw or "").split("\n") if normalize_text(line)]
        label_map: dict[str, float] = {}
        unlabeled: list[float] = []
        for line in lines:
            pair_match = PAIR_RE.match(line)
            if pair_match and re.search(r"\d", pair_match.group(2)):
                label = normalize_text(pair_match.group(1)).lower()
                value = first_number(pair_match.group(2))
                if value is not None:
                    label_map[label] = value
                    continue
            if re.search(r"%|dfs|level|students|rate|score", line.lower()):
                value = first_number(line)
                if value is not None and len(line) < 50:
                    unlabeled.append(value)
        return label_map, unlabeled

    baseline_map, baseline_unlabeled = parse_map(metric.get("baseline_raw", ""))
    year_1_map, year_1_unlabeled = parse_map(metric.get("year_1_outcome_raw", ""))
    target_map, target_unlabeled = parse_map(metric.get("year_3_target_raw", ""))
    direction = infer_direction(metric.get("metric_name", ""), metric.get("year_3_target_raw", ""))

    ratios: list[float] = []
    for label in sorted(set(baseline_map) & set(year_1_map) & set(target_map)):
        baseline_value = baseline_map[label]
        year_1_value = year_1_map[label]
        target_value = target_map[label]
        if math.isclose(baseline_value, target_value):
            if math.isclose(year_1_value, target_value):
                ratios.append(1.0)
            continue
        denominator = (target_value - baseline_value) if direction == "higher" else (baseline_value - target_value)
        numerator = (year_1_value - baseline_value) if direction == "higher" else (baseline_value - year_1_value)
        if denominator > 0:
            ratios.append(numerator / denominator)

    if not ratios and baseline_unlabeled and year_1_unlabeled and target_unlabeled:
        baseline_value = baseline_unlabeled[0]
        year_1_value = year_1_unlabeled[0]
        target_value = target_unlabeled[0]
        if math.isclose(baseline_value, target_value):
            if math.isclose(year_1_value, target_value):
                ratios.append(1.0)
        else:
            denominator = (target_value - baseline_value) if direction == "higher" else (baseline_value - target_value)
            numerator = (year_1_value - baseline_value) if direction == "higher" else (baseline_value - year_1_value)
            if denominator > 0:
                ratios.append(numerator / denominator)

    if not ratios:
        return None

    ratio = sum(ratios) / len(ratios)
    if ratio >= 1:
        status = "at_or_above_target"
    elif ratio >= 1 / 3:
        status = "on_track"
    elif ratio >= 0:
        status = "off_track"
    else:
        status = "moving_away"

    return {"status": status, "ratio": ratio}


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    output = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    output.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-path",
        type=Path,
        default=INPUT_PATH,
        help="Path to all_lcaps.json generated by extract_lcaps.py.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory where opportunity research files should be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input_path.resolve()
    output_dir = args.output_dir.resolve()

    output_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    districts = payload["districts"]
    district_count = len(districts)

    area_rollup: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "area_label": "",
            "districts": set(),
            "goal_count": 0,
            "action_count": 0,
            "metric_count": 0,
            "action_dollars": 0.0,
            "externalizable_dollars": 0.0,
            "procurement_mix": Counter(),
            "movement_counts": Counter(),
            "pace_counts": Counter(),
            "district_negative_pressure": set(),
            "district_examples": [],
        }
    )

    district_area_rows: list[dict[str, Any]] = []

    for district in districts:
        district_name = district["district_name"]
        district_area_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "goal_count": 0,
                "action_count": 0,
                "metric_count": 0,
                "action_dollars": 0.0,
                "externalizable_dollars": 0.0,
                "movement_counts": Counter(),
                "pace_counts": Counter(),
                "action_examples": [],
                "metric_examples": [],
            }
        )

        for goal in district["goals"]:
            goal_text = goal.get("description", "")
            for area_key in matched_areas_for_kind(goal_text, "goal"):
                area_rollup[area_key]["area_label"] = AREA_INDEX[area_key]["label"]
                area_rollup[area_key]["districts"].add(district_name)
                area_rollup[area_key]["goal_count"] += 1
                district_area_stats[area_key]["goal_count"] += 1

            for action in goal["actions"]:
                action_title = action.get("title", "")
                action_text = f"{action_title} {action.get('description', '')}"
                action_area_scores = area_scores_for_kind(action_text, "action")
                action_title_scores = area_scores_for_kind(action_title, "action")
                action_areas = set(action_area_scores)
                if not action_areas:
                    continue
                profile, vendor_fit = procurement_profile(action)
                dollars = float(action.get("total_funds") or 0.0)
                for area_key in action_areas:
                    area_rollup[area_key]["area_label"] = AREA_INDEX[area_key]["label"]
                    area_rollup[area_key]["districts"].add(district_name)
                    area_rollup[area_key]["action_count"] += 1
                    area_rollup[area_key]["action_dollars"] += dollars
                    area_rollup[area_key]["externalizable_dollars"] += dollars * vendor_fit
                    area_rollup[area_key]["procurement_mix"][profile] += 1

                    district_area_stats[area_key]["action_count"] += 1
                    district_area_stats[area_key]["action_dollars"] += dollars
                    district_area_stats[area_key]["externalizable_dollars"] += dollars * vendor_fit

                    district_area_stats[area_key]["action_examples"].append(
                        {
                            "action_number": action.get("action_number", ""),
                            "title": action.get("title", ""),
                            "dollars": dollars,
                            "vendor_fit": vendor_fit,
                            "externalizable_dollars": dollars * vendor_fit,
                            "match_score": action_area_scores.get(area_key, 0),
                            "title_match_score": action_title_scores.get(area_key, 0),
                        }
                    )

            for metric in goal["metrics"]:
                metric_text = metric.get("metric_name", "")
                metric_area_scores = area_scores_for_kind(metric_text, "metric")
                metric_areas = set(metric_area_scores)
                if not metric_areas:
                    continue
                movement = parse_current_difference(metric)
                pace = pace_status(metric)
                for area_key in metric_areas:
                    area_rollup[area_key]["area_label"] = AREA_INDEX[area_key]["label"]
                    area_rollup[area_key]["districts"].add(district_name)
                    area_rollup[area_key]["metric_count"] += 1
                    district_area_stats[area_key]["metric_count"] += 1

                    if movement:
                        counts = Counter(item["movement"] for item in movement)
                        area_rollup[area_key]["movement_counts"].update(counts)
                        district_area_stats[area_key]["movement_counts"].update(counts)

                    if pace:
                        area_rollup[area_key]["pace_counts"][pace["status"]] += 1
                        district_area_stats[area_key]["pace_counts"][pace["status"]] += 1

                    metric_example = {
                        "metric_number": metric.get("metric_number", ""),
                        "metric_name": metric.get("metric_name", ""),
                        "pace_status": pace["status"] if pace else "",
                        "pace_ratio": pace["ratio"] if pace else None,
                        "movement_raw": movement[0]["raw"] if movement else "",
                        "movement_worsening_count": sum(1 for item in movement if item["movement"] == "worsening"),
                        "match_score": metric_area_scores.get(area_key, 0),
                    }
                    district_area_stats[area_key]["metric_examples"].append(metric_example)

        for area_key, stats in district_area_stats.items():
            movement_total = sum(stats["movement_counts"].values())
            worsening = stats["movement_counts"]["worsening"]
            flat = stats["movement_counts"]["flat"]
            improving = stats["movement_counts"]["improving"]
            pace_total = sum(stats["pace_counts"].values())
            negative_pace = stats["pace_counts"]["off_track"] + stats["pace_counts"]["moving_away"]
            positive_pace = stats["pace_counts"]["at_or_above_target"] + stats["pace_counts"]["on_track"]

            movement_negative_share = worsening / movement_total if movement_total else 0.0
            pace_negative_share = negative_pace / pace_total if pace_total else 0.0

            if movement_negative_share >= 0.25 or pace_negative_share >= 0.25:
                area_rollup[area_key]["district_negative_pressure"].add(district_name)

            top_actions = sorted(
                stats["action_examples"],
                key=lambda item: (item["title_match_score"], item["match_score"], item["externalizable_dollars"]),
                reverse=True,
            )[:3]
            negative_metric_pool = [
                item
                for item in stats["metric_examples"]
                if item["pace_status"] in {"moving_away", "off_track"} or item["movement_worsening_count"] > 0
            ]
            metric_source = negative_metric_pool if negative_metric_pool else stats["metric_examples"]
            negative_metrics = sorted(
                metric_source,
                key=lambda item: (
                    item["pace_status"] in {"moving_away", "off_track"},
                    item["movement_worsening_count"],
                    item["match_score"],
                    -(item["pace_ratio"] if item["pace_ratio"] is not None else -999),
                ),
                reverse=True,
            )[:3]

            district_area_rows.append(
                {
                    "district_name": district_name,
                    "area_key": area_key,
                    "area_label": AREA_INDEX[area_key]["label"],
                    "goal_count": stats["goal_count"],
                    "action_count": stats["action_count"],
                    "metric_count": stats["metric_count"],
                    "action_dollars": round(stats["action_dollars"], 2),
                    "externalizable_dollars": round(stats["externalizable_dollars"], 2),
                    "movement_points": movement_total,
                    "movement_improving": improving,
                    "movement_flat": flat,
                    "movement_worsening": worsening,
                    "movement_negative_share": round(movement_negative_share, 4),
                    "pace_metrics": pace_total,
                    "pace_positive": positive_pace,
                    "pace_negative": negative_pace,
                    "pace_negative_share": round(pace_negative_share, 4),
                    "top_action_1": top_actions[0]["title"] if len(top_actions) > 0 else "",
                    "top_action_1_dollars": round(top_actions[0]["dollars"], 2) if len(top_actions) > 0 else 0.0,
                    "top_action_2": top_actions[1]["title"] if len(top_actions) > 1 else "",
                    "top_action_2_dollars": round(top_actions[1]["dollars"], 2) if len(top_actions) > 1 else 0.0,
                    "metric_example_1": negative_metrics[0]["metric_name"] if len(negative_metrics) > 0 else "",
                    "metric_example_1_status": negative_metrics[0]["pace_status"] if len(negative_metrics) > 0 else "",
                    "metric_example_1_diff": negative_metrics[0]["movement_raw"] if len(negative_metrics) > 0 else "",
                }
            )

    max_externalizable = max((stats["externalizable_dollars"] for stats in area_rollup.values()), default=0.0)

    area_rows: list[dict[str, Any]] = []
    for area_key, stats in area_rollup.items():
        districts_with_area = len(stats["districts"])
        movement_total = sum(stats["movement_counts"].values())
        worsening = stats["movement_counts"]["worsening"]
        pace_total = sum(stats["pace_counts"].values())
        negative_pace = stats["pace_counts"]["off_track"] + stats["pace_counts"]["moving_away"]
        positive_pace = stats["pace_counts"]["at_or_above_target"] + stats["pace_counts"]["on_track"]
        movement_negative_share = worsening / movement_total if movement_total else 0.0
        pace_negative_share = negative_pace / pace_total if pace_total else 0.0
        districts_with_negative = len(stats["district_negative_pressure"])
        prevalence_share = districts_with_area / district_count if district_count else 0.0
        pressure_share = max(movement_negative_share, pace_negative_share)
        multi_district_pressure_share = districts_with_negative / districts_with_area if districts_with_area else 0.0
        spend_component = (
            math.log10(stats["externalizable_dollars"] + 1) / math.log10(max_externalizable + 1)
            if max_externalizable > 0
            else 0.0
        )
        opportunity_score = (
            0.30 * prevalence_share
            + 0.30 * spend_component
            + 0.25 * pressure_share
            + 0.15 * multi_district_pressure_share
        )

        top_district_examples = sorted(
            [row for row in district_area_rows if row["area_key"] == area_key],
            key=lambda row: (
                row["externalizable_dollars"] * (1 + row["movement_negative_share"] + row["pace_negative_share"]),
                row["action_dollars"],
            ),
            reverse=True,
        )[:5]

        area_rows.append(
            {
                "area_key": area_key,
                "area_label": stats["area_label"] or AREA_INDEX[area_key]["label"],
                "districts_with_area": districts_with_area,
                "district_prevalence_share": round(prevalence_share, 4),
                "goal_count": stats["goal_count"],
                "action_count": stats["action_count"],
                "metric_count": stats["metric_count"],
                "action_dollars": round(stats["action_dollars"], 2),
                "externalizable_dollars": round(stats["externalizable_dollars"], 2),
                "movement_points": movement_total,
                "movement_worsening": worsening,
                "movement_negative_share": round(movement_negative_share, 4),
                "pace_metrics": pace_total,
                "pace_positive": positive_pace,
                "pace_negative": negative_pace,
                "pace_negative_share": round(pace_negative_share, 4),
                "districts_with_negative_pressure": districts_with_negative,
                "district_negative_pressure_share": round(multi_district_pressure_share, 4),
                "externalizable_share_of_action_dollars": round(
                    (stats["externalizable_dollars"] / stats["action_dollars"]) if stats["action_dollars"] else 0.0,
                    4,
                ),
                "top_procurement_profile": stats["procurement_mix"].most_common(1)[0][0] if stats["procurement_mix"] else "",
                "service_hypothesis": AREA_INDEX[area_key]["service_hypothesis"],
                "opportunity_score": round(opportunity_score, 4),
                "example_district_1": top_district_examples[0]["district_name"] if len(top_district_examples) > 0 else "",
                "example_district_1_action": top_district_examples[0]["top_action_1"] if len(top_district_examples) > 0 else "",
                "example_district_1_action_dollars": top_district_examples[0]["top_action_1_dollars"] if len(top_district_examples) > 0 else 0.0,
                "example_district_1_metric": top_district_examples[0]["metric_example_1"] if len(top_district_examples) > 0 else "",
                "example_district_1_metric_status": top_district_examples[0]["metric_example_1_status"] if len(top_district_examples) > 0 else "",
                "example_district_2": top_district_examples[1]["district_name"] if len(top_district_examples) > 1 else "",
                "example_district_2_action": top_district_examples[1]["top_action_1"] if len(top_district_examples) > 1 else "",
                "example_district_2_action_dollars": top_district_examples[1]["top_action_1_dollars"] if len(top_district_examples) > 1 else 0.0,
                "example_district_2_metric": top_district_examples[1]["metric_example_1"] if len(top_district_examples) > 1 else "",
                "example_district_2_metric_status": top_district_examples[1]["metric_example_1_status"] if len(top_district_examples) > 1 else "",
            }
        )

    area_rows.sort(key=lambda row: row["opportunity_score"], reverse=True)
    district_area_rows.sort(
        key=lambda row: (
            row["externalizable_dollars"] * (1 + row["movement_negative_share"] + row["pace_negative_share"]),
            row["action_dollars"],
        ),
        reverse=True,
    )

    write_csv(
        output_dir / "opportunity_areas.csv",
        [
            "area_key",
            "area_label",
            "districts_with_area",
            "district_prevalence_share",
            "goal_count",
            "action_count",
            "metric_count",
            "action_dollars",
            "externalizable_dollars",
            "movement_points",
            "movement_worsening",
            "movement_negative_share",
            "pace_metrics",
            "pace_positive",
            "pace_negative",
            "pace_negative_share",
            "districts_with_negative_pressure",
            "district_negative_pressure_share",
            "externalizable_share_of_action_dollars",
            "top_procurement_profile",
            "service_hypothesis",
            "opportunity_score",
            "example_district_1",
            "example_district_1_action",
            "example_district_1_action_dollars",
            "example_district_1_metric",
            "example_district_1_metric_status",
            "example_district_2",
            "example_district_2_action",
            "example_district_2_action_dollars",
            "example_district_2_metric",
            "example_district_2_metric_status",
        ],
        area_rows,
    )

    write_csv(
        output_dir / "district_area_examples.csv",
        [
            "district_name",
            "area_key",
            "area_label",
            "goal_count",
            "action_count",
            "metric_count",
            "action_dollars",
            "externalizable_dollars",
            "movement_points",
            "movement_improving",
            "movement_flat",
            "movement_worsening",
            "movement_negative_share",
            "pace_metrics",
            "pace_positive",
            "pace_negative",
            "pace_negative_share",
            "top_action_1",
            "top_action_1_dollars",
            "top_action_2",
            "top_action_2_dollars",
            "metric_example_1",
            "metric_example_1_status",
            "metric_example_1_diff",
        ],
        district_area_rows,
    )

    summary = {
        "district_count": district_count,
        "area_rows": area_rows,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    top_rows = area_rows[:8]
    top_table = markdown_table(
        ["Opportunity Area", "Districts", "Action $", "Externalizable $", "Neg. Metric Share", "Score"],
        [
            [
                row["area_label"],
                str(row["districts_with_area"]),
                format_currency(row["action_dollars"]),
                format_currency(row["externalizable_dollars"]),
                format_pct(max(row["movement_negative_share"], row["pace_negative_share"])),
                f"{row['opportunity_score']:.2f}",
            ]
            for row in top_rows
        ],
    )

    deep_dives: list[str] = []
    for row in top_rows[:5]:
        examples = [
            candidate
            for candidate in district_area_rows
            if candidate["area_key"] == row["area_key"]
            and (candidate["movement_negative_share"] > 0 or candidate["pace_negative_share"] > 0)
        ]
        examples.sort(
            key=lambda candidate: (
                candidate["externalizable_dollars"] * (1 + candidate["movement_negative_share"] + candidate["pace_negative_share"]),
                candidate["action_dollars"],
            ),
            reverse=True,
        )
        example_lines: list[str] = []
        for candidate in examples[:3]:
            parts = [
                f"**{candidate['district_name']}**",
                f"action dollars {format_currency(candidate['action_dollars'])}",
                f"externalizable dollars {format_currency(candidate['externalizable_dollars'])}",
            ]
            if candidate["top_action_1"]:
                parts.append(f"top action: `{candidate['top_action_1']}` ({format_currency(candidate['top_action_1_dollars'])})")
            if candidate["metric_example_1"]:
                metric_status = candidate["metric_example_1_status"] or "movement only"
                parts.append(f"metric signal: `{candidate['metric_example_1']}` [{metric_status}]")
            if candidate["metric_example_1_diff"]:
                parts.append(f"current diff snippet: `{candidate['metric_example_1_diff']}`")
            example_lines.append("- " + "; ".join(parts))

        deep_dives.append(
            "\n".join(
                [
                    f"### {row['area_label']}",
                    "",
                    f"- District prevalence: **{row['districts_with_area']} / {district_count}** ({format_pct(row['district_prevalence_share'])})",
                    f"- Direct action dollars: **{format_currency(row['action_dollars'])}**",
                    f"- Estimated externalizable dollars: **{format_currency(row['externalizable_dollars'])}**",
                    f"- Negative metric signal share: **{format_pct(max(row['movement_negative_share'], row['pace_negative_share']))}**",
                    f"- Districts showing meaningful pressure: **{row['districts_with_negative_pressure']}**",
                    f"- Likely buyable service lane: {row['service_hypothesis']}",
                    "",
                    "**Examples:**",
                    *(example_lines or ["- No negative-signal district examples cleared the filter for this area."]),
                ]
            )
        )

    narrative = f"""# LCAP Service Opportunity Report

## Purpose

This report is built for a commercial question, not a policy question: **where are Orange County districts already spending money, across many districts, on problem areas where the metrics still show pressure or weak movement?**

The analysis uses four layers together:

1. **Prevalence**: how many districts are working on the problem.
2. **Direct spend**: how much action-table money is attached to that problem.
3. **Externalizable spend**: how much of that action money looks buyable from a vendor or service provider rather than being pure internal payroll.
4. **Metric pressure**: whether the related metrics are worsening, off track, or otherwise under pressure.

This is the level of analysis you need if the goal is to decide **what districts might realistically pay for**.

## What Was Wrong With the First Report

The first report was useful as a countywide strategy summary, but it was too broad for market discovery:

- It grouped work into themes like “Academic Achievement & Instruction,” which is too wide to tell you what specific pain districts might buy help for.
- It treated all action dollars as equally relevant, even though a huge share of LCAP spend is just internal staffing and not obvious vendor spend.
- It emphasized overall directionality without asking the commercial question: **where is spend already present and performance still under pressure?**
- It did not produce district-level example bundles pairing **real actions + real metric evidence**.

## Top Service Opportunities

{top_table}

## How To Read The Ranking

- **Action $** is the direct action-table money tied to the opportunity area through keyword matching.
- **Externalizable $** is a weighted estimate of the portion that looks buyable from a vendor: software, coaching, program services, materials, contracted support, or mixed external services.
- **Neg. Metric Share** is the stronger of:
  - the share of metric movement points that look like worsening movement from baseline, or
  - the share of pace-scored metrics that are off track / moving away.
- **Score** is a composite opportunity score that weights prevalence, externalizable spend, and metric pressure.

## Deep Dives

{chr(10).join(deep_dives)}

## How I Would Fix The Pipeline For Real GTM Use

If this were becoming a repeatable district-opportunity engine, I would harden it in five ways:

1. **Move from broad themes to a two-taxonomy system**.
   Problem taxonomy: chronic absenteeism, suspension/climate, ELA, math, EL progress, graduation, college/career, family engagement, MTSS, wellness, SPED, data systems.
   Solution taxonomy: staffing, software, coaching/PD, tutoring/program delivery, community services, materials/resources.

2. **Keep two spend numbers on every opportunity area**.
   One should be total action dollars.
   The second should be externalizable/vendor-fit dollars.
   Without that split, internal salary-heavy categories will swamp the results and make the market look bigger than it is.

3. **Use both movement and pace signals**.
   Movement: parse `current_difference_from_baseline` for a broad “improving / flat / worsening” read.
   Pace: when baseline, year-1, and year-3 target are all numerically interpretable, estimate whether year-1 progress is fast enough.
   This gives much wider coverage than relying on pace alone.

4. **Score districts inside each opportunity area**.
   For each district-area pair, track:
   - direct dollars
   - externalizable dollars
   - negative movement share
   - negative pace share
   - top actions by dollars
   - top metric warnings
   That gives you a district-targeting list instead of just a countywide narrative.

5. **Add a procurement lens**.
   The next version should classify whether the district is already buying:
   - software/platforms
   - coaching/PD
   - tutoring/program operations
   - community/family services
   - materials/curriculum
   This matters because willingness to pay is not just “money exists”; it is “money exists in a lane districts already procure externally.”

## Files

- `summary.json`
- `opportunity_areas.csv`
- `district_area_examples.csv`
"""

    (output_dir / "service_opportunity_report.md").write_text(narrative + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
