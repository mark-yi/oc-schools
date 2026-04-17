#!/usr/bin/env python3
"""Fetch California School Dashboard public data for districts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "data" / "dashboard_public"
DEFAULT_MANIFEST_DIR = ROOT / "outputs" / "dashboard_public"
DEFAULT_DISTRICTS_PATH = ROOT / "data" / "cde" / "public_districts.json"

CDE_DISTRICTS_URL = "https://www.cde.ca.gov/schooldirectory/report?rid=dl2&tp=txt"
DASHBOARD_API_BASE_URL = "https://api.caschooldashboard.org"
DASHBOARD_LOCAL_INDICATOR_IDS = "1-2-3-6-7-9-10"
DASHBOARD_DEFAULT_INDICATOR_IDS = (1, 2, 3, 4, 5, 6, 7, 8)
DASHBOARD_GROWTH_INDICATOR_IDS = (6, 7)
DASHBOARD_GROWTH_ASSESSMENT_IDS = "6-7"
USER_AGENT = "Mozilla/5.0 (compatible; oc-schools-dashboard-public-data/1.0)"


@dataclass
class District:
    cd_code: str
    cds_code: str
    county: str
    district: str
    doc: str
    doc_type: str
    status_type: str
    county_dir: str
    district_dir: str
    source_record: dict[str, Any] = field(default_factory=dict)


@dataclass
class DashboardRecord:
    cd_code: str
    cds_code: str
    county: str
    district: str
    doc: str
    doc_type: str
    status_type: str
    year: int
    school_year_id: int | None
    output_path: str | None
    cds_index_path: str | None
    endpoint_count: int
    error_count: int
    warning_count: int
    skipped: bool = False
    error: str | None = None


def request(url: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "User-Agent": USER_AGENT,
        },
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean(value: Any) -> str:
    value = "" if value is None else str(value).strip()
    return "" if value == "No Data" else value


def safe_path_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 ._()-]+", " ", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:140] or "unknown"


def read_text_url(url: str) -> str:
    with urllib.request.urlopen(request(url), timeout=60) as response:
        charset = response.headers.get_content_charset() or "utf-8-sig"
        return response.read().decode(charset)


def read_json_url(url: str, timeout: int) -> Any:
    try:
        with urllib.request.urlopen(request(url), timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            text = response.read().decode(charset).strip()
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace").strip()
        detail = f": {body[:300]}" if body else ""
        raise RuntimeError(f"HTTP {error.code}{detail}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(str(error.reason)) from error

    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def district_from_mapping(row: dict[str, Any]) -> District | None:
    cd_code = clean(row.get("cd_code") or row.get("CD Code"))
    if not re.fullmatch(r"\d{7}", cd_code):
        return None

    cds_code = clean(row.get("cds_code") or f"{cd_code}0000000")
    county = clean(row.get("county") or row.get("County"))
    district = clean(row.get("district") or row.get("District"))
    return District(
        cd_code=cd_code,
        cds_code=cds_code,
        county=county,
        district=district,
        doc=clean(row.get("doc") or row.get("DOC")),
        doc_type=clean(row.get("doc_type") or row.get("DOCType")),
        status_type=clean(row.get("status_type") or row.get("StatusType")),
        county_dir=clean(row.get("county_dir")) or safe_path_part(county),
        district_dir=clean(row.get("district_dir")) or safe_path_part(f"{cds_code} - {district}"),
        source_record={str(key): value for key, value in row.items()},
    )


def iter_public_districts_from_cde() -> Iterable[District]:
    text = read_text_url(CDE_DISTRICTS_URL)
    rows = csv.DictReader(text.splitlines(), delimiter="\t")
    for row in rows:
        district = district_from_mapping(row)
        if district:
            yield district


def iter_public_districts_from_json(path: Path) -> Iterable[District]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    for row in rows:
        district = district_from_mapping(row)
        if district:
            yield district


def iter_public_districts(path: Path, refresh: bool) -> Iterable[District]:
    if path.exists() and not refresh:
        yield from iter_public_districts_from_json(path)
        return
    yield from iter_public_districts_from_cde()


def dashboard_url(path: str) -> str:
    return f"{DASHBOARD_API_BASE_URL}/{path.lstrip('/')}"


def fetch_endpoint(
    name: str,
    path: str,
    timeout: int,
    sources: dict[str, str],
    errors: list[dict[str, str]],
    warnings: list[dict[str, str]] | None = None,
    optional: bool = False,
) -> Any:
    url = dashboard_url(path)
    sources[name] = url
    try:
        return read_json_url(url, timeout=timeout)
    except RuntimeError as error:
        issue = {"endpoint": name, "url": url, "error": str(error)}
        if optional and warnings is not None:
            warnings.append(issue)
        else:
            errors.append(issue)
        return None


def school_year_id_for_year(school_years: Any, year: int) -> int | None:
    if not isinstance(school_years, list):
        return None
    for school_year in school_years:
        if not isinstance(school_year, dict):
            continue
        try:
            if int(school_year.get("year")) == year:
                return int(school_year["schoolYearId"])
        except (TypeError, ValueError, KeyError):
            continue
    return None


def collect_indicator_ids(value: Any) -> list[int]:
    indicator_ids: set[int] = set()

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                if key in {"indicatorId", "indicatorID"}:
                    try:
                        indicator_ids.add(int(child))
                    except (TypeError, ValueError):
                        pass
                else:
                    visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return sorted(indicator_ids) or list(DASHBOARD_DEFAULT_INDICATOR_IDS)


def district_metadata(district: District) -> dict[str, Any]:
    data = asdict(district)
    source_record = data.pop("source_record")
    data["cde_public_district"] = source_record
    return data


def fetch_dashboard_public_data(district: District, year: int, timeout: int, local_indicator_ids: str) -> dict[str, Any]:
    sources: dict[str, str] = {}
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    cds_code = urllib.parse.quote(district.cds_code)

    school_years = fetch_endpoint("school_years", f"LEAs/schoolYears/{cds_code}", timeout, sources, errors)
    if school_years is None:
        school_years = fetch_endpoint("school_years_global", "LEAs/schoolYears", timeout, sources, errors)

    school_year_id = school_year_id_for_year(school_years, year)
    package: dict[str, Any] = {
        "metadata": {
            "source": "california_school_dashboard_api",
            "api_base_url": DASHBOARD_API_BASE_URL,
            "fetched_at": utc_now(),
            "requested_year": year,
            "school_year_id": school_year_id,
            "district": district_metadata(district),
            "source_urls": sources,
        },
        "school_years": school_years,
        "profile": {},
        "summary": {},
        "student_groups": {},
        "charts": {},
        "local_indicators": {},
        "growth": {},
        "errors": errors,
        "warnings": warnings,
    }

    if school_year_id is None:
        errors.append(
            {
                "endpoint": "school_year_id",
                "url": sources.get("school_years", ""),
                "error": f"Dashboard school year {year} was not available for this district.",
            }
        )
        return package

    profile = package["profile"]
    profile["lea"] = fetch_endpoint("lea", f"LEAs/{cds_code}/{school_year_id}/true", timeout, sources, errors)
    profile["lea_totals"] = fetch_endpoint(
        "lea_totals",
        f"LEAs/totals/{cds_code}/{school_year_id}",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    profile["district_leas"] = fetch_endpoint(
        "district_leas",
        f"LEAs/DistrictLEAs/{cds_code}/{school_year_id}",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )

    summary = package["summary"]
    summary["summary_cards"] = fetch_endpoint(
        "summary_cards",
        f"Reports/{cds_code}/{school_year_id}/SummaryCards",
        timeout,
        sources,
        errors,
    )

    indicator_ids = collect_indicator_ids(summary["summary_cards"])
    package["metadata"]["indicator_ids"] = indicator_ids

    for indicator_id in indicator_ids:
        indicator_key = str(indicator_id)
        package["student_groups"][indicator_key] = fetch_endpoint(
            f"student_groups_{indicator_id}",
            f"Reports/{cds_code}/{school_year_id}/StudentGroups/{indicator_id}",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        package["charts"][indicator_key] = fetch_endpoint(
            f"yearly_charts_{indicator_id}",
            f"Reports/{cds_code}/{school_year_id}/GraphData/All/{indicator_id}",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )

    charts = package["charts"]
    charts["cci"] = fetch_endpoint(
        "cci_chart",
        f"Reports/{cds_code}/{school_year_id}/GraphData/CCI",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    charts["elpi"] = fetch_endpoint(
        "elpi_graph_data",
        f"Reports/{cds_code}/{school_year_id}/ElpiGraphData",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    charts["elpi_2024"] = fetch_endpoint(
        "elpi_graph_data_2024",
        f"Reports/{cds_code}/{school_year_id}/ElpiGraphData2024",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    charts["five_year_grad_rates"] = fetch_endpoint(
        "five_year_grad_rates",
        f"Reports/{cds_code}/{school_year_id}/GraphData/FiveYearGradRates",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    charts["one_year_grad_rates"] = fetch_endpoint(
        "one_year_grad_rates",
        f"Reports/{cds_code}/{school_year_id}/GraphData/OneYearGradRates",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    charts["five_year_grad_rates_overall"] = fetch_endpoint(
        "five_year_grad_rates_overall",
        f"Reports/{cds_code}/{school_year_id}/GraphData/FiveYearGradRatesOverall",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )

    local_indicators = package["local_indicators"]
    local_indicators["report"] = fetch_endpoint(
        "local_indicators_report",
        f"LocalIndicators/{cds_code}/{school_year_id}/Reports/{local_indicator_ids}",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    local_indicators["log"] = fetch_endpoint(
        "local_indicators_log",
        f"LocalIndicators/{cds_code}/{school_year_id}/Log",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )
    local_indicators["priority1_teaching_data"] = fetch_endpoint(
        "priority1_teaching_data",
        f"LocalIndicators/{cds_code}/{school_year_id}/priority1TeachingData",
        timeout,
        sources,
        errors,
        warnings,
        optional=True,
    )

    growth = package["growth"]
    if year >= 2025:
        growth_prefix = f"Reports/2025/{cds_code}/{school_year_id}"
        growth["summary_cards"] = fetch_endpoint(
            "growth_summary_cards",
            f"{growth_prefix}/GrowthSummaryCards",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        growth["data"] = fetch_endpoint(
            "growth_data",
            f"{growth_prefix}/growth/data",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        growth["additional_assessment"] = fetch_endpoint(
            "growth_additional_assessment",
            f"{growth_prefix}/growth/AddAssessment/{DASHBOARD_GROWTH_ASSESSMENT_IDS}",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        for indicator_id in DASHBOARD_GROWTH_INDICATOR_IDS:
            growth[str(indicator_id)] = fetch_endpoint(
                f"growth_{indicator_id}",
                f"{growth_prefix}/growth/{indicator_id}",
                timeout,
                sources,
                errors,
                warnings,
                optional=True,
            )
    else:
        growth["summary_cards"] = fetch_endpoint(
            "growth_summary_cards",
            f"Reports/{cds_code}/{school_year_id}/GrowthSummaryCards",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        growth["additional_assessment"] = fetch_endpoint(
            "additional_assessment",
            f"Reports/{cds_code}/{school_year_id}/AddAssessment/{DASHBOARD_GROWTH_ASSESSMENT_IDS}",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        growth["alternate_assessment"] = fetch_endpoint(
            "alternate_assessment",
            f"Reports/{cds_code}/{school_year_id}/AltAssessment/{DASHBOARD_GROWTH_ASSESSMENT_IDS}",
            timeout,
            sources,
            errors,
            warnings,
            optional=True,
        )
        for indicator_id in DASHBOARD_GROWTH_INDICATOR_IDS:
            growth[str(indicator_id)] = fetch_endpoint(
                f"growth_{indicator_id}",
                f"Reports/{cds_code}/{school_year_id}/growth/{indicator_id}",
                timeout,
                sources,
                errors,
                warnings,
                optional=True,
            )

    return package


def district_output_path(output_dir: Path, year: int, district: District) -> Path:
    return (
        output_dir
        / str(year)
        / "by_county"
        / district.county_dir
        / district.district_dir
        / f"Dashboard Public Data {year} - {district.cds_code}.json"
    )


def district_index_path(output_dir: Path, year: int, district: District) -> Path:
    return output_dir / str(year) / "by_cds_code" / f"{district.cds_code}.json"


def link_by_cds_code(source_path: Path, index_path: Path) -> None:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    if index_path.exists() or index_path.is_symlink():
        index_path.unlink()

    try:
        relative_target = os.path.relpath(source_path, start=index_path.parent)
        index_path.symlink_to(relative_target)
    except OSError:
        shutil.copy2(source_path, index_path)


def write_package(package: dict[str, Any], output_path: Path, index_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(package, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    link_by_cds_code(output_path, index_path)


def read_existing_manifest_counts(output_path: Path) -> tuple[int | None, int, int, int]:
    try:
        package = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, 0, 0, 0

    metadata = package.get("metadata") if isinstance(package, dict) else {}
    source_urls = metadata.get("source_urls") if isinstance(metadata, dict) else {}
    return (
        metadata.get("school_year_id") if isinstance(metadata, dict) else None,
        len(source_urls) if isinstance(source_urls, dict) else 0,
        len(package.get("errors") or []) if isinstance(package, dict) else 0,
        len(package.get("warnings") or []) if isinstance(package, dict) else 0,
    )


def manifest_record(
    district: District,
    year: int,
    school_year_id: int | None,
    output_path: Path | None,
    index_path: Path | None,
    endpoint_count: int,
    error_count: int,
    warning_count: int,
    skipped: bool,
    error: str | None = None,
) -> DashboardRecord:
    return DashboardRecord(
        cd_code=district.cd_code,
        cds_code=district.cds_code,
        county=district.county,
        district=district.district,
        doc=district.doc,
        doc_type=district.doc_type,
        status_type=district.status_type,
        year=year,
        school_year_id=school_year_id,
        output_path=str(output_path) if output_path else None,
        cds_index_path=str(index_path) if index_path else None,
        endpoint_count=endpoint_count,
        error_count=error_count,
        warning_count=warning_count,
        skipped=skipped,
        error=error,
    )


def write_manifest(records: list[DashboardRecord], manifest_dir: Path, year: int) -> None:
    manifest_dir.mkdir(parents=True, exist_ok=True)
    json_path = manifest_dir / f"dashboard_public_{year}_manifest.json"
    csv_path = manifest_dir / f"dashboard_public_{year}_manifest.csv"

    json_path.write_text(
        json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    fieldnames = list(asdict(records[0]).keys()) if records else [field.name for field in DashboardRecord.__dataclass_fields__.values()]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2025, help="Dashboard year to fetch.")
    parser.add_argument("--county", help="Limit to one county name, case-insensitive.")
    parser.add_argument("--district", help="Limit to districts containing this text, case-insensitive.")
    parser.add_argument("--cds-code", action="append", help="Fetch one CDS code. May be provided more than once.")
    parser.add_argument("--limit", type=int, help="Stop after this many matching districts.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for per-district Dashboard JSON.")
    parser.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR, help="Directory for manifest JSON/CSV.")
    parser.add_argument(
        "--districts-path",
        type=Path,
        default=DEFAULT_DISTRICTS_PATH,
        help="Cached normalized CDE public districts JSON to use when present.",
    )
    parser.add_argument(
        "--refresh-districts",
        action="store_true",
        help="Ignore the cached districts JSON and read the CDE public districts endpoint directly.",
    )
    parser.add_argument("--include-inactive", action="store_true", help="Include districts not marked Active in CDE data.")
    parser.add_argument("--overwrite", action="store_true", help="Refresh districts even when their JSON file already exists.")
    parser.add_argument("--delay", type=float, default=0.15, help="Delay between district fetches, in seconds.")
    parser.add_argument("--timeout", type=int, default=45, help="Per-endpoint timeout, in seconds.")
    parser.add_argument(
        "--local-indicator-ids",
        default=DASHBOARD_LOCAL_INDICATOR_IDS,
        help="Dash-separated local indicator ids to request.",
    )
    return parser.parse_args()


def district_matches(district: District, args: argparse.Namespace) -> bool:
    if args.county and district.county.casefold() != args.county.casefold():
        return False
    if args.district and args.district.casefold() not in district.district.casefold():
        return False
    if args.cds_code and district.cds_code not in set(args.cds_code):
        return False
    if not args.include_inactive and district.status_type and district.status_type != "Active":
        return False
    return True


def main() -> None:
    args = parse_args()
    records: list[DashboardRecord] = []
    matched = 0

    for district in iter_public_districts(args.districts_path, args.refresh_districts):
        if not district_matches(district, args):
            continue

        matched += 1
        if args.limit and matched > args.limit:
            break

        output_path = district_output_path(args.output_dir, args.year, district)
        index_path = district_index_path(args.output_dir, args.year, district)
        if output_path.exists() and not args.overwrite:
            link_by_cds_code(output_path, index_path)
            school_year_id, endpoint_count, error_count, warning_count = read_existing_manifest_counts(output_path)
            records.append(
                manifest_record(
                    district=district,
                    year=args.year,
                    school_year_id=school_year_id,
                    output_path=output_path,
                    index_path=index_path,
                    endpoint_count=endpoint_count,
                    error_count=error_count,
                    warning_count=warning_count,
                    skipped=True,
                )
            )
            print(f"{len(records):04d} skip    {district.cds_code} {district.county} - {district.district}", flush=True)
            continue

        try:
            package = fetch_dashboard_public_data(
                district=district,
                year=args.year,
                timeout=args.timeout,
                local_indicator_ids=args.local_indicator_ids,
            )
            write_package(package, output_path, index_path)
            error_count = len(package["errors"])
            warning_count = len(package["warnings"])
            records.append(
                manifest_record(
                    district=district,
                    year=args.year,
                    school_year_id=package["metadata"]["school_year_id"],
                    output_path=output_path,
                    index_path=index_path,
                    endpoint_count=len(package["metadata"]["source_urls"]),
                    error_count=error_count,
                    warning_count=warning_count,
                    skipped=False,
                )
            )
            status = "partial" if error_count else "warn" if warning_count else "ok"
        except Exception as error:  # Keep long statewide runs moving.
            records.append(
                manifest_record(
                    district=district,
                    year=args.year,
                    school_year_id=None,
                    output_path=None,
                    index_path=None,
                    endpoint_count=0,
                    error_count=1,
                    warning_count=0,
                    skipped=False,
                    error=str(error),
                )
            )
            status = "error"

        print(f"{len(records):04d} {status:7s} {district.cds_code} {district.county} - {district.district}", flush=True)
        time.sleep(args.delay)

    write_manifest(records, args.manifest_dir, args.year)
    fetched = sum(1 for record in records if not record.skipped and not record.error)
    skipped = sum(1 for record in records if record.skipped)
    errors = sum(1 for record in records if record.error or record.error_count)
    warnings = sum(1 for record in records if record.warning_count)
    print(f"Wrote {len(records)} manifest records to {args.manifest_dir}")
    print(f"Fetched: {fetched}; skipped: {skipped}; records with endpoint errors: {errors}; with warnings: {warnings}")


if __name__ == "__main__":
    main()
