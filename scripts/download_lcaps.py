#!/usr/bin/env python3
"""Discover and optionally download California Dashboard LCAP PDFs."""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "lcaps_statewide"
DEFAULT_MANIFEST_DIR = ROOT / "outputs" / "lcap_downloads"

CDE_DISTRICTS_URL = "https://www.cde.ca.gov/schooldirectory/report?rid=dl2&tp=txt"
DASHBOARD_LCAP_CHECK_URL = "https://api.caschooldashboard.org/LCAP/report/check/{cds_code}"
MYCDE_LCAP_URL = "https://api.mycdeconnect.org/reports/lcap?cdsCode={cds_code}&year={year}"
USER_AGENT = "Mozilla/5.0 (compatible; oc-schools-lcap-downloader/1.0)"


@dataclass
class District:
    cd_code: str
    cds_code: str
    county: str
    district: str
    doc: str
    doc_type: str
    status_type: str


@dataclass
class LcapRecord:
    cd_code: str
    cds_code: str
    county: str
    district: str
    doc: str
    doc_type: str
    status_type: str
    year: int
    lcap_url: str
    has_lcap: bool
    pdf_url: str | None = None
    output_path: str | None = None
    error: str | None = None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def request(url: str, method: str = "GET") -> urllib.request.Request:
    return urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})


def read_text_url(url: str) -> str:
    with urllib.request.urlopen(request(url), timeout=60) as response:
        charset = response.headers.get_content_charset() or "utf-8-sig"
        return response.read().decode(charset)


def iter_public_districts() -> Iterable[District]:
    text = read_text_url(CDE_DISTRICTS_URL)
    rows = csv.DictReader(text.splitlines(), delimiter="\t")
    for row in rows:
        cd_code = (row.get("CD Code") or "").strip()
        if not re.fullmatch(r"\d{7}", cd_code):
            continue
        yield District(
            cd_code=cd_code,
            cds_code=f"{cd_code}0000000",
            county=(row.get("County") or "").strip(),
            district=(row.get("District") or "").strip(),
            doc=(row.get("DOC") or "").strip(),
            doc_type=(row.get("DOCType") or "").strip(),
            status_type=(row.get("StatusType") or "").strip(),
        )


def dashboard_has_lcap(cds_code: str) -> bool:
    url = DASHBOARD_LCAP_CHECK_URL.format(cds_code=urllib.parse.quote(cds_code))
    with urllib.request.urlopen(request(url), timeout=30) as response:
        return response.read().decode("utf-8").strip().lower() == "true"


def resolve_pdf_url(cds_code: str, year: int) -> tuple[bool, str | None, str | None]:
    url = MYCDE_LCAP_URL.format(cds_code=urllib.parse.quote(cds_code), year=year)
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(request(url, method="HEAD"), timeout=30) as response:
            location = response.headers.get("Location")
            return bool(location and "NotFound" not in location), location, None
    except urllib.error.HTTPError as error:
        if error.code in {301, 302, 303, 307, 308}:
            location = error.headers.get("Location")
            if location and "NotFound" not in location:
                return True, urllib.parse.urljoin(url, location), None
            return False, None, None
        return False, None, f"HTTP {error.code}"
    except urllib.error.URLError as error:
        return False, None, str(error.reason)


def safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 ._()-]+", " ", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:140] or "unknown"


def download_pdf(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(request(url), timeout=120) as response:
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not response.url.lower().endswith(".pdf"):
            raise RuntimeError(f"unexpected response for PDF download: {content_type or response.url}")
        output_path.write_bytes(response.read())


def write_manifests(records: list[LcapRecord], manifest_dir: Path, year: int) -> None:
    manifest_dir.mkdir(parents=True, exist_ok=True)
    json_path = manifest_dir / f"lcaps_{year}_manifest.json"
    csv_path = manifest_dir / f"lcaps_{year}_manifest.csv"

    json_path.write_text(
        json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    fieldnames = list(asdict(records[0]).keys()) if records else [field.name for field in LcapRecord.__dataclass_fields__.values()]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2025, help="LCAP year parameter to request.")
    parser.add_argument("--county", help="Limit to one county name, case-insensitive.")
    parser.add_argument("--limit", type=int, help="Stop after this many candidate districts.")
    parser.add_argument("--download", action="store_true", help="Download PDFs in addition to writing the manifest.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for downloaded PDFs.")
    parser.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR, help="Directory for manifest JSON/CSV.")
    parser.add_argument("--delay", type=float, default=0.15, help="Delay between LEA probes, in seconds.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    county_filter = args.county.casefold() if args.county else None
    records: list[LcapRecord] = []

    candidates = 0
    for district in iter_public_districts():
        if county_filter and district.county.casefold() != county_filter:
            continue
        candidates += 1
        if args.limit and candidates > args.limit:
            break

        lcap_url = MYCDE_LCAP_URL.format(cds_code=district.cds_code, year=args.year)
        record = LcapRecord(
            **asdict(district),
            year=args.year,
            lcap_url=lcap_url,
            has_lcap=False,
        )

        try:
            if dashboard_has_lcap(district.cds_code):
                has_lcap, pdf_url, error = resolve_pdf_url(district.cds_code, args.year)
                record.has_lcap = has_lcap
                record.pdf_url = pdf_url
                record.error = error
                if args.download and has_lcap and pdf_url:
                    filename = f"{district.cd_code} - {safe_filename(district.district)}.pdf"
                    output_path = args.output_dir / str(args.year) / safe_filename(district.county) / filename
                    download_pdf(lcap_url, output_path)
                    record.output_path = str(output_path)
            time.sleep(args.delay)
        except Exception as error:  # Keep long statewide runs moving.
            record.error = str(error)

        records.append(record)
        status = "LCAP" if record.has_lcap else "none"
        print(f"{len(records):04d} {status:4s} {district.cds_code} {district.county} - {district.district}", flush=True)

    write_manifests(records, args.manifest_dir, args.year)
    lcap_count = sum(1 for record in records if record.has_lcap)
    print(f"Wrote {len(records)} records, {lcap_count} with LCAPs, to {args.manifest_dir}")


if __name__ == "__main__":
    main()
