#!/usr/bin/env python3
"""Parse Sparkasse bank CSV exports into transaction records for buddy-api.

Supports two formats:
  - sparkasse_giro  : Sparkasse Giro CAMT V2 (ISO-8859-15, semicolon, columns:
                      Buchungstag, Valutadatum, Verwendungszweck, Beguenstigter/
                      Zahlungspflichtiger, Betrag, Waehrung, ...)
  - sparkasse_kk    : Sparkasse Kreditkarte (ISO-8859-15, semicolon, columns:
                      Belegdatum, Buchungsbetrag, Transaktionsbeschreibung, ...)

Input params (JSON via argv[1] or stdin):
    {
        "csv_path":   "/host/root/path/to/export.csv",   # container-side path
        "account_id": 1,
        "format":     "sparkasse_giro"  # or "sparkasse_kk"
    }

Output (list):
    [
        {
            "type":        "transaction",
            "account_id":  1,
            "booking_date": "2025-03-01",
            "value_date":   "2025-03-01",
            "amount":       -23.47,
            "counterpart":  "Amazon Payments",
            "reference":    "Verwendungszweck-Text",
            "external_id":  "<sha256 of raw row>",
            "raw_data":     { ...all original columns... }
        },
        ...
    ]
"""
import csv
import hashlib
import json
import re
import sys
from datetime import datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_german_decimal(value: str) -> float:
    """Convert German decimal notation '1.234,56' or '-23,47' to float."""
    if not value:
        raise ValueError("empty amount")
    # Remove thousand separators (dot), replace decimal comma with dot
    cleaned = value.strip().replace(".", "").replace(",", ".")
    return float(cleaned)


def parse_german_date(value: str) -> str:
    """Convert German date formats to ISO 8601 YYYY-MM-DD.

    Accepts:
        DD.MM.YYYY  (e.g. 01.03.2025)
        DD.MM.YY    (e.g. 01.03.25)
    """
    value = value.strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date: {value!r}")


def row_sha256(row: dict) -> str:
    """Stable SHA-256 fingerprint of a CSV row (sorted keys)."""
    canonical = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_key(raw: str) -> str:
    """Lowercase, strip BOM / whitespace / special chars from CSV header."""
    return raw.strip().lstrip("\ufeff").lower().replace("\xa0", " ")


# ---------------------------------------------------------------------------
# Format parsers
# ---------------------------------------------------------------------------

def parse_sparkasse_giro(reader, account_id: int) -> list:
    """Parse Sparkasse Girokonto CAMT V2 CSV.

    Expected columns (case-insensitive):
        buchungstag, valutadatum, verwendungszweck,
        beguenstigter/zahlungspflichtiger (or similar), betrag, waehrung
    """
    results = []
    for raw_row in reader:
        # Normalize keys
        row = {normalize_key(k): v.strip() for k, v in raw_row.items() if k}

        # Skip empty / summary rows (no booking date)
        buchungstag = row.get("buchungstag", "").strip()
        if not buchungstag:
            continue

        # Parse dates
        try:
            booking_date = parse_german_date(buchungstag)
        except ValueError:
            continue  # skip header-like rows or summaries

        valutadatum = row.get("valutadatum", "").strip()
        value_date = booking_date  # fallback
        if valutadatum:
            try:
                value_date = parse_german_date(valutadatum)
            except ValueError:
                pass

        # Amount
        betrag_raw = row.get("betrag", "").strip()
        if not betrag_raw:
            continue
        try:
            amount = parse_german_decimal(betrag_raw)
        except ValueError:
            continue

        # Counterpart: several possible column names across Sparkasse variants
        counterpart = (
            row.get("beguenstigter/zahlungspflichtiger")
            or row.get("begünstigter/zahlungspflichtiger")
            or row.get("auftraggeber/beguenstigter")
            or row.get("auftraggeber/begünstigter")
            or row.get("beguenstigter")
            or row.get("zahlungspflichtiger")
            or ""
        ).strip()

        reference = row.get("verwendungszweck", "").strip()

        results.append({
            "type": "transaction",
            "account_id": account_id,
            "booking_date": booking_date,
            "value_date": value_date,
            "amount": amount,
            "counterpart": counterpart,
            "reference": reference,
            "external_id": row_sha256(row),
            "raw_data": dict(row),
        })

    return results


def parse_sparkasse_kk(reader, account_id: int) -> list:
    """Parse Sparkasse Kreditkarte CSV.

    Expected columns (case-insensitive):
        belegdatum, buchungsbetrag, transaktionsbeschreibung
    Also tries: buchungsdatum, betrag, beschreibung, waehrung, merchant, ...
    """
    results = []
    for raw_row in reader:
        row = {normalize_key(k): v.strip() for k, v in raw_row.items() if k}

        # Date: prefer belegdatum, fallback buchungsdatum
        date_raw = (
            row.get("belegdatum")
            or row.get("buchungsdatum")
            or row.get("datum")
            or ""
        ).strip()
        if not date_raw:
            continue
        try:
            booking_date = parse_german_date(date_raw)
        except ValueError:
            continue

        # Booking date for value_date (KK usually has only one date)
        buchungsdatum_raw = row.get("buchungsdatum", "").strip()
        value_date = booking_date
        if buchungsdatum_raw:
            try:
                value_date = parse_german_date(buchungsdatum_raw)
            except ValueError:
                pass

        # Amount: prefer buchungsbetrag, fallback betrag
        betrag_raw = (
            row.get("buchungsbetrag")
            or row.get("betrag")
            or row.get("umsatz")
            or ""
        ).strip()
        if not betrag_raw:
            continue
        try:
            amount = parse_german_decimal(betrag_raw)
        except ValueError:
            continue

        # Description / counterpart
        description = (
            row.get("transaktionsbeschreibung")
            or row.get("beschreibung")
            or row.get("verwendungszweck")
            or row.get("merchant")
            or ""
        ).strip()

        results.append({
            "type": "transaction",
            "account_id": account_id,
            "booking_date": booking_date,
            "value_date": value_date,
            "amount": amount,
            "counterpart": description,
            "reference": description,
            "external_id": row_sha256(row),
            "raw_data": dict(row),
        })

    return results


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

PARSERS = {
    "sparkasse_giro": parse_sparkasse_giro,
    "sparkasse_kk": parse_sparkasse_kk,
}


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    csv_path = params.get("csv_path", "")
    account_id = int(params.get("account_id", 0))
    fmt = params.get("format", "sparkasse_giro")

    if not csv_path:
        print(json.dumps({"error": "csv_path is required"}))
        sys.exit(1)

    if fmt not in PARSERS:
        print(json.dumps({"error": f"Unknown format {fmt!r}. Valid: {list(PARSERS)}"}))
        sys.exit(1)

    try:
        with open(csv_path, encoding="iso-8859-15") as fh:
            # Sniff delimiter — Sparkasse always uses semicolons, but be defensive
            reader = csv.DictReader(fh, delimiter=";")
            transactions = PARSERS[fmt](reader, account_id)
    except FileNotFoundError:
        print(json.dumps({"error": f"File not found: {csv_path}"}))
        sys.exit(1)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)

    print(json.dumps(transactions, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
