#!/usr/bin/env python3
"""Parse an OCR-converted Kontoauszug (bank statement) Markdown into transaction records.

Takes the Markdown output from MarkItDown OCR and extracts individual booking lines
using regex-first heuristics with an LLM fallback for ambiguous/difficult pages.

Input (JSON via argv[1] or stdin):
    {
        "markdown":    "...",          # OCR output from MarkItDown
        "account_id": 1,              # buddy-db accounts.id
        "source_path": "...",         # original file path (for external_id derivation)
        "owner":       "eltern"       # "hans" or "eltern"
    }

Output (list of transaction payloads for buddy-api /v1/ingest):
    [
        {
            "type":         "transaction",
            "account_id":   1,
            "booking_date": "2024-03-01",
            "value_date":   "2024-03-01",
            "amount":       -47.50,
            "counterpart":  "Rewe GmbH",
            "reference":    "Kartenzahlung 01.03. REWE",
            "external_id":  "<sha256>",
            "raw_data":     {"owner": "eltern", "source": "ocr", ...}
        },
        ...
    ]

ENV:
    BUDDY_LLM_PROVIDER  = anthropic | openai | local  (default: anthropic)
    BUDDY_LLM_MODEL     = model name (default: claude-3-5-haiku-20241022)
    BUDDY_LLM_API_KEY   = API key
    BUDDY_LLM_BASE_URL  = base URL for local/custom endpoint
"""
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Regex helpers for German bank statement parsing
# ---------------------------------------------------------------------------

# German date: DD.MM.YYYY or DD.MM.YY (with optional surrounding whitespace)
DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.(?:\d{4}|\d{2}))\b")

# German decimal number: e.g. 1.234,56 or 47,50 or -23,47 — with optional leading sign/space
AMOUNT_RE = re.compile(
    r"(?<!\d)"
    r"([+\-]?\s*\d{1,3}(?:\.\d{3})*,\d{2})"
    r"(?:\s*[€EUR]*)?"
    r"(?!\d)"
)

# Debit/credit indicators commonly found in German statements
DEBIT_INDICATORS = re.compile(
    r"\b(lastschrift|abbuchung|auszahlung|überweisung ausgang|zahlung|entnahme|belastung|s\b)",
    re.IGNORECASE,
)
CREDIT_INDICATORS = re.compile(
    r"\b(gutschrift|einzahlung|überweisung eingang|gehalt|lohn|rente|zinsen|h\b)",
    re.IGNORECASE,
)


def parse_german_date(value: str) -> str:
    """Convert German date string to ISO 8601 YYYY-MM-DD."""
    value = value.strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date: {value!r}")


def parse_german_decimal(value: str) -> float:
    """Convert German decimal notation '1.234,56' or '-23,47' to float."""
    cleaned = value.strip().replace(" ", "").replace(".", "").replace(",", ".")
    return float(cleaned)


def row_sha256(source_path: str, booking_date: str, amount: float, reference: str) -> str:
    """Stable external_id from key fields."""
    canonical = json.dumps(
        {
            "source": source_path,
            "booking_date": booking_date,
            "amount": amount,
            "reference": reference,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Regex-based line parser
# ---------------------------------------------------------------------------

def _parse_line_regex(line: str) -> Optional[dict]:
    """Try to extract a transaction from a single text line using regex.

    Returns a partial dict {booking_date, value_date, amount, counterpart,
    reference} or None if no transaction found.
    """
    dates = DATE_RE.findall(line)
    amounts = AMOUNT_RE.findall(line)

    if not dates or not amounts:
        return None

    # Parse first date as booking_date, second (if present) as value_date
    try:
        booking_date = parse_german_date(dates[0])
    except ValueError:
        return None

    value_date = booking_date
    if len(dates) >= 2:
        try:
            value_date = parse_german_date(dates[1])
        except ValueError:
            pass

    # Use last amount on the line as the transaction amount
    amount_raw = amounts[-1]
    try:
        amount = parse_german_decimal(amount_raw)
    except ValueError:
        return None

    # Determine sign from debit/credit indicators if amount has no explicit sign
    if amount > 0:
        if DEBIT_INDICATORS.search(line):
            amount = -abs(amount)

    # Reference: everything between last date and amount (cleaned up)
    # Strip dates and amount from line to get description
    cleaned = line
    for d in dates:
        cleaned = cleaned.replace(d, " ", 1)
    cleaned = AMOUNT_RE.sub(" ", cleaned)
    reference = re.sub(r"\s{2,}", " ", cleaned).strip().rstrip("|").strip()

    # Counterpart heuristic: first "word group" in the reference (before comma or slash)
    counterpart_match = re.match(r"([A-Za-zÄÖÜäöüß\s\-\.&]{4,40})", reference)
    counterpart = counterpart_match.group(1).strip() if counterpart_match else ""

    return {
        "booking_date": booking_date,
        "value_date": value_date,
        "amount": amount,
        "counterpart": counterpart,
        "reference": reference,
    }


def parse_with_regex(markdown: str) -> list[dict]:
    """Apply regex parsing line-by-line. Returns list of partial transaction dicts."""
    results = []
    for line in markdown.splitlines():
        line = line.strip()
        if len(line) < 10:
            continue
        tx = _parse_line_regex(line)
        if tx:
            results.append(tx)
    return results


# ---------------------------------------------------------------------------
# LLM-based parser (fallback for difficult OCR / complex layouts)
# ---------------------------------------------------------------------------

OCR_SYSTEM_PROMPT = """Du bist ein Spezialist für die Analyse von deutschen Kontoauszügen (OCR-Text).

Deine Aufgabe: Extrahiere ALLE Buchungszeilen aus dem gegebenen Kontoauszug-Text.

Antworte AUSSCHLIESSLICH mit einem JSON-Array — kein erklärender Text, keine Markdown-Fences.

Jede Buchung als Objekt:
{
  "booking_date": "YYYY-MM-DD",   // Buchungsdatum (Pflicht)
  "value_date":   "YYYY-MM-DD",   // Wertstellungsdatum (oder gleich wie booking_date)
  "amount":       -47.50,          // Betrag: negativ = Abbuchung, positiv = Gutschrift
  "counterpart":  "Rewe GmbH",    // Name des Empfängers/Absenders (null wenn nicht erkennbar)
  "reference":    "Kartenzahlung" // Verwendungszweck / Buchungstext (null wenn leer)
}

Wichtige Regeln:
- Kontostand-Zeilen (Anfangssaldo, Endsaldo, Übertrag) NICHT als Buchungen ausgeben
- Bei unlesbaren Beträgen (OCR-Artefakte): Zeile überspringen
- Wenn Datum nur als DD.MM vorkommt: Jahr aus Kontext des Dokuments ableiten
- Vorzeichen: Lastschriften/Abbuchungen = negativ, Gutschriften/Eingänge = positiv
- Betrag als Dezimalzahl, NICHT als String, KEIN Währungssymbol

Antworte NUR mit dem JSON-Array, z.B.:
[{"booking_date":"2024-03-01","value_date":"2024-03-01","amount":-47.50,"counterpart":"Rewe GmbH","reference":"Kartenzahlung 01.03. REWE"},...]"""


def parse_with_llm(markdown: str) -> list[dict]:
    """Use LLM to extract transactions from OCR Markdown.

    Returns list of partial transaction dicts (same shape as regex output).
    Falls back to empty list on any error.
    """
    provider = os.getenv("BUDDY_LLM_PROVIDER", "anthropic").lower()
    api_key = os.getenv("BUDDY_LLM_API_KEY", "")
    base_url = os.getenv("BUDDY_LLM_BASE_URL", "")

    if not api_key:
        print("[buddy_parse_kontoauszug] WARNING: no BUDDY_LLM_API_KEY — skipping LLM parse", file=sys.stderr)
        return []

    # Truncate to avoid token limits (~8k chars is safe for most models)
    content = markdown[:8000] if len(markdown) > 8000 else markdown

    try:
        if provider == "anthropic":
            model = os.getenv("BUDDY_LLM_MODEL", "claude-3-5-haiku-20241022")
            import anthropic  # type: ignore
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model=model,
                max_tokens=4096,
                system=OCR_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": f"Kontoauszug:\n\n{content}"}],
            )
            raw = msg.content[0].text.strip()

        elif provider == "openai":
            model = os.getenv("BUDDY_LLM_MODEL", "gpt-4o-mini")
            import openai  # type: ignore
            kwargs = {"api_key": api_key}
            if base_url:
                kwargs["base_url"] = base_url
            client = openai.OpenAI(**kwargs)
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": OCR_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Kontoauszug:\n\n{content}"},
                ],
                max_tokens=4096,
                temperature=0.1,
            )
            raw = resp.choices[0].message.content.strip()

        elif provider == "local":
            import httpx  # type: ignore
            model = os.getenv("BUDDY_LLM_MODEL", "local-model")
            resp = httpx.post(
                base_url.rstrip("/") + "/chat/completions",
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": OCR_SYSTEM_PROMPT},
                        {"role": "user", "content": f"Kontoauszug:\n\n{content}"},
                    ],
                    "max_tokens": 4096,
                    "temperature": 0.1,
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=120.0,
            )
            resp.raise_for_status()
            raw = resp.json()["choices"][0]["message"]["content"].strip()

        else:
            print(f"[buddy_parse_kontoauszug] Unknown LLM provider: {provider!r}", file=sys.stderr)
            return []

        # Strip markdown fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            print("[buddy_parse_kontoauszug] LLM returned non-list — ignoring", file=sys.stderr)
            return []

        return parsed

    except Exception as exc:  # noqa: BLE001
        print(f"[buddy_parse_kontoauszug] LLM parse error: {exc}", file=sys.stderr)
        return []


# ---------------------------------------------------------------------------
# Merge & deduplicate
# ---------------------------------------------------------------------------

def _deduplicate(transactions: list[dict]) -> list[dict]:
    """Remove duplicate entries by (booking_date, amount, reference)."""
    seen: set[tuple] = set()
    result = []
    for tx in transactions:
        key = (tx.get("booking_date"), tx.get("amount"), (tx.get("reference") or "")[:60])
        if key in seen:
            continue
        seen.add(key)
        result.append(tx)
    return result


def _validate_tx(tx: dict) -> bool:
    """Basic sanity check on a raw transaction dict."""
    if not tx.get("booking_date"):
        return False
    if tx.get("amount") is None:
        return False
    try:
        float(tx["amount"])
    except (TypeError, ValueError):
        return False
    # Must look like a real date
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(tx["booking_date"])):
        return False
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    markdown = params.get("markdown", "")
    account_id = int(params.get("account_id", 0))
    source_path = params.get("source_path", "unknown")
    owner = params.get("owner", "eltern")

    if not markdown:
        print(json.dumps([]))
        return

    # Step 1: Try regex parsing first (fast, no API cost)
    regex_results = parse_with_regex(markdown)

    # Step 2: If regex found < 2 entries, fall back to LLM (OCR quality too low for regex)
    if len(regex_results) < 2:
        print(
            f"[buddy_parse_kontoauszug] Regex found {len(regex_results)} entries — using LLM fallback",
            file=sys.stderr,
        )
        llm_results = parse_with_llm(markdown)
        raw_transactions = llm_results if len(llm_results) >= len(regex_results) else regex_results
        method = "llm" if len(llm_results) >= len(regex_results) else "regex"
    else:
        raw_transactions = regex_results
        method = "regex"

    # Step 3: Validate + deduplicate
    valid = [tx for tx in raw_transactions if _validate_tx(tx)]
    unique = _deduplicate(valid)

    # Step 4: Build full transaction payloads for buddy-api /v1/ingest
    transactions = []
    for tx in unique:
        booking_date = str(tx.get("booking_date", ""))
        value_date = str(tx.get("value_date") or booking_date)
        amount = float(tx.get("amount", 0.0))
        counterpart = str(tx.get("counterpart") or "")
        reference = str(tx.get("reference") or "")

        ext_id = row_sha256(source_path, booking_date, amount, reference)

        transactions.append({
            "type": "transaction",
            "account_id": account_id,
            "booking_date": booking_date,
            "value_date": value_date,
            "amount": amount,
            "counterpart": counterpart,
            "reference": reference,
            "external_id": ext_id,
            "raw_data": {
                "owner": owner,
                "source": "ocr_upload",
                "source_path": source_path,
                "parse_method": method,
            },
        })

    print(json.dumps(transactions, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
