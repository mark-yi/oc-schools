#!/usr/bin/env python3
"""Fetch and normalize CDE Public Districts directory data."""

from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "data" / "cde"
CDE_DISTRICTS_URL = "https://www.cde.ca.gov/schooldirectory/report?rid=dl2&tp=txt"
USER_AGENT = "Mozilla/5.0 (compatible; oc-schools-cde-directory/1.0)"


@dataclass
class DistrictRecord:
    cd_code: str
    cds_code: str
    county: str
    district: str
    doc: str
    doc_type: str
    status_type: str
    street: str
    city: str
    zip: str
    state: str
    mail_street: str
    mail_city: str
    mail_zip: str
    mail_state: str
    phone: str
    extension: str
    fax_number: str
    admin_first_name: str
    admin_last_name: str
    latitude: str
    longitude: str
    last_update: str
    county_dir: str
    district_dir: str
    source: str = "cde_public_districts"


def request(url: str) -> urllib.request.Request:
    return urllib.request.Request(url, headers={"User-Agent": USER_AGENT})


def safe_path_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 ._()-]+", " ", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:140] or "unknown"


def clean(value: str | None) -> str:
    value = (value or "").strip()
    return "" if value == "No Data" else value


def download_raw_text(url: str) -> str:
    with urllib.request.urlopen(request(url), timeout=60) as response:
        charset = response.headers.get_content_charset() or "utf-8-sig"
        return response.read().decode(charset)


def normalize_records(raw_text: str) -> list[DistrictRecord]:
    records: list[DistrictRecord] = []
    rows = csv.DictReader(raw_text.splitlines(), delimiter="\t")
    for row in rows:
        cd_code = clean(row.get("CD Code"))
        if not re.fullmatch(r"\d{7}", cd_code):
            continue

        district = clean(row.get("District"))
        county = clean(row.get("County"))
        records.append(
            DistrictRecord(
                cd_code=cd_code,
                cds_code=f"{cd_code}0000000",
                county=county,
                district=district,
                doc=clean(row.get("DOC")),
                doc_type=clean(row.get("DOCType")),
                status_type=clean(row.get("StatusType")),
                street=clean(row.get("Street")),
                city=clean(row.get("City")),
                zip=clean(row.get("Zip")),
                state=clean(row.get("State")),
                mail_street=clean(row.get("MailStreet")),
                mail_city=clean(row.get("MailCity")),
                mail_zip=clean(row.get("MailZip")),
                mail_state=clean(row.get("MailState")),
                phone=clean(row.get("Phone")),
                extension=clean(row.get("Ext  ")),
                fax_number=clean(row.get("FaxNumber")),
                admin_first_name=clean(row.get("AdmFName")),
                admin_last_name=clean(row.get("AdmLName")),
                latitude=clean(row.get("Latitude")),
                longitude=clean(row.get("Longitude")),
                last_update=clean(row.get("LastUpDate")),
                county_dir=safe_path_part(county),
                district_dir=safe_path_part(f"{cd_code}0000000 - {district}"),
            )
        )
    return records


def write_outputs(records: list[DistrictRecord], raw_text: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "public_districts_raw.txt").write_text(raw_text, encoding="utf-8")
    (output_dir / "public_districts.json").write_text(
        json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    fieldnames = list(asdict(records[0]).keys()) if records else [field.name for field in DistrictRecord.__dataclass_fields__.values()]
    with (output_dir / "public_districts.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source-url", default=CDE_DISTRICTS_URL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_text = download_raw_text(args.source_url)
    records = normalize_records(raw_text)
    write_outputs(records, raw_text, args.output_dir)

    active_count = sum(1 for record in records if record.status_type == "Active")
    print(f"Wrote {len(records)} public district records to {args.output_dir}")
    print(f"Active records: {active_count}")


if __name__ == "__main__":
    main()
