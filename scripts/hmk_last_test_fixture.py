#!/usr/bin/env python3
"""Prepare deterministic replay inputs for the last HMK extraction test."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class HMKDoc:
    doc_id: int
    file_name: str
    source: str
    source_id: str
    extension: str
    doc_date: str

    def pipeline_input(self) -> dict[str, object]:
        return {
            "item": {
                "id": self.doc_id,
                "source": self.source,
                "source_id": self.source_id,
                "file_name": self.file_name,
                "extension": self.extension,
                "doc_date": self.doc_date,
                "extra": {},
            },
            "language": "de",
        }


HMK_LAST_TEST_DOCS: tuple[HMKDoc, ...] = (
    HMKDoc(
        doc_id=12421,
        file_name="HMK 19752 Auszüge 1 06.07.25, 09_57 Microsoft Lens.pdf",
        source="onedrive:user@example.com",
        source_id="8DF5EDA212792823!s0a3ce0e79ae349f99723d580cba3ab31",
        extension="pdf",
        doc_date="2024-08-17",
    ),
    HMKDoc(
        doc_id=12513,
        file_name="HMK 202402-B 04.11.24, 10_19 Microsoft Lens.pdf",
        source="onedrive:user@example.com",
        source_id="8DF5EDA212792823!184421",
        extension="pdf",
        doc_date="2024-10-09",
    ),
    HMKDoc(
        doc_id=12550,
        file_name="HMK 202454 Rechnung-R-DE-118475690-2024-31001775.pdf",
        source="onedrive:user@example.com",
        source_id="8DF5EDA212792823!185150",
        extension="pdf",
        doc_date="2024-11-08",
    ),
)


def _print_docs() -> None:
    for doc in HMK_LAST_TEST_DOCS:
        print(f"{doc.doc_id}\t{doc.file_name}\t{doc.source_id}")


def _print_payloads() -> None:
    for doc in HMK_LAST_TEST_DOCS:
        print(json.dumps(doc.pipeline_input(), ensure_ascii=False))


def _print_reset_sql() -> None:
    ids = ", ".join(str(doc.doc_id) for doc in HMK_LAST_TEST_DOCS)
    print(
        "UPDATE documents\n"
        "SET raw_structured = NULL,\n"
        "    content_hash = NULL,\n"
        "    file_path = NULL,\n"
        "    doc_type = NULL,\n"
        "    extraction_specialists = array_remove(\n"
        "        array_remove(\n"
        "            array_remove(COALESCE(extraction_specialists, ARRAY[]::text[]),\n"
        "                'hmk_extracted'\n"
        "            ),\n"
        "            'structured_llm'\n"
        "        ),\n"
        "        'merged_structured'\n"
        "    )\n"
        f"WHERE id IN ({ids});"
    )


def _print_notes() -> None:
    print("Pipeline: buddy-hmk-extract-single")
    print("Daigestr target: http://daigestr:8081/v1/convert")
    print("Request style: auto_extract=true, mode=default, retry_on_low_quality=true, quality_retry_threshold=0.75, quality_retry_mode=full")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("docs", "payloads", "reset-sql", "notes", "all"),
        help="What to print",
    )
    args = parser.parse_args()

    if args.command in {"docs", "all"}:
        _print_docs()
        if args.command == "all":
            print()
    if args.command in {"payloads", "all"}:
        _print_payloads()
        if args.command == "all":
            print()
    if args.command in {"reset-sql", "all"}:
        _print_reset_sql()
        if args.command == "all":
            print()
    if args.command in {"notes", "all"}:
        _print_notes()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
