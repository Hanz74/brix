#!/usr/bin/env python3
"""LLM-based document classification for buddy intake pipeline.

Classifies a markdown document (invoice, bank statement, contract, etc.)
into structured data suitable for the buddy-api /v1/ingest endpoint.

Input:
    {
        "markdown": "...",
        "source": "outlook",
        "source_id": "...",
        "filename": ""        # optional
    }

Output:
    {
        "doc_type": "invoice|bank_statement|contract|reminder|receipt|other",
        "category": "software|insurance|utilities|rent|salary|...",
        "amount": 49.99,
        "currency": "EUR",
        "party_name": "Amazon",
        "invoice_number": "INV-2024-001",
        "due_date": "2024-12-31",
        "direction": "incoming|outgoing",
        "subject": "...",
        "summary": "...",
        "extra": {},
        "source": "outlook",
        "source_id": "...",
        "raw_markdown": "...",
        "classification_method": "llm|regex"
    }

ENV:
    BUDDY_LLM_PROVIDER  = anthropic | openai | local  (default: anthropic)
    BUDDY_LLM_MODEL     = model name (default: claude-3-5-haiku-20241022 / gpt-4o-mini)
    BUDDY_LLM_API_KEY   = API key
    BUDDY_LLM_BASE_URL  = base URL for local/custom endpoint
"""
import json
import os
import re
import sys
from datetime import datetime


# ---------------------------------------------------------------------------
# Prompt — base
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_BASE = """Du bist ein Finanz-Dokumenten-Klassifikator. Analysiere das gegebene Markdown-Dokument und extrahiere strukturierte Daten.

Antworte AUSSCHLIESSLICH mit einem JSON-Objekt — kein erklärender Text, keine Markdown-Fences.

Felder:
- doc_type: Dokumenttyp. Erlaubte Werte: invoice, bank_statement, contract, reminder, receipt, salary, insurance, tax_assessment, tax_certificate, donation_receipt, utility_statement, nk_statement, other
- category: Kategorie des Zwecks. Erlaubte Werte: software, hosting, insurance, utilities, rent, salary, travel, food, medical, tax, subscription, banking, transfer, children, authority, health, other
- amount: Betrag als Zahl (ohne Währungssymbol, ohne Tausendertrennzeichen). null wenn nicht erkennbar.
- currency: Währungs-Code, z.B. "EUR", "USD". null wenn nicht erkennbar.
- party_name: Name des Absenders/Empfängers (Firma oder Person). null wenn nicht erkennbar.
- invoice_number: Rechnungsnummer oder Belegnummer. null wenn nicht vorhanden.
- doc_date: Datum des Dokuments im Format YYYY-MM-DD. null wenn nicht erkennbar.
- due_date: Fälligkeitsdatum im Format YYYY-MM-DD. null wenn nicht vorhanden.
- direction: "outgoing" wenn Geld abgeht (Rechnung, die ich bezahle), "incoming" wenn Geld kommt (Gutschrift, Gehalt). null wenn unklar.
- subject: Betreff oder Titel des Dokuments (max 200 Zeichen).
- summary: Kurze Zusammenfassung in 1-2 Sätzen was dieses Dokument ist und was zu tun ist.
- extra: JSON-Objekt mit zusätzlichen relevanten Feldern (z.B. {"vat_amount": 7.99, "period": "2024-11"}).

Dokumenttyp-Hinweise:
- invoice: Rechnung / Faktura (auch Handwerkerrechnung)
- tax_assessment: Steuerbescheid / Einkommensteuerbescheid / Grundsteuerbescheid
- tax_certificate: Lohnsteuerbescheinigung / Jahressteuerbescheinigung (Bank, Depot)
- donation_receipt: Spendenquittung / Zuwendungsbestätigung
- nk_statement: Nebenkostenabrechnung
- contract: Mietvertrag, Dienstleistungsvertrag, Versicherungsvertrag, Vereinbarung

Antworte NUR mit dem JSON-Objekt, z.B.:
{"doc_type":"invoice","category":"software","amount":49.99,...}"""


# ---------------------------------------------------------------------------
# Category-specific prompt extensions
# ---------------------------------------------------------------------------

# Keys are the detected category or doc_type; values are additional instructions
# appended to the system prompt when that type/category is detected.

CATEGORY_PROMPT_EXTENSIONS: dict[str, str] = {
    "contract": """
Dieses Dokument scheint ein Vertrag zu sein. Extrahiere zusätzlich im extra-Feld:
- contract_start: Vertragsbeginn im Format YYYY-MM-DD (null wenn nicht vorhanden)
- contract_end: Vertragsende / Laufzeit-Ende im Format YYYY-MM-DD (null wenn nicht vorhanden)
- notice_period: Kündigungsfrist als Text, z.B. "3 Monate zum Quartalsende" (null wenn nicht vorhanden)
- contract_partner: Vollständiger Name des Vertragspartners (Firma oder Person)
- contract_type: Art des Vertrags, z.B. "Mietvertrag", "Dienstleistungsvertrag", "Versicherungsvertrag"
- auto_renewal: true wenn automatische Verlängerung, false wenn nicht, null wenn unklar""",

    "authority": """
Dieses Dokument scheint von einer Behörde zu stammen. Extrahiere zusätzlich im extra-Feld:
- authority_name: Name der Behörde (z.B. "Finanzamt Köln-Mitte", "Einwohnermeldeamt")
- case_number: Aktenzeichen oder Geschäftszeichen (null wenn nicht vorhanden)
- deadline: Frist / Abgabetermin im Format YYYY-MM-DD (null wenn nicht vorhanden)
- authority_type: Art der Behörde, z.B. "Finanzamt", "Einwohnermeldeamt", "Gericht", "Sozialamt" """,

    "health": """
Dieses Dokument bezieht sich auf Gesundheit / medizinische Versorgung. Extrahiere zusätzlich im extra-Feld:
- doctor_name: Name des Arztes / der Ärztin oder der Praxis (null wenn nicht vorhanden)
- treatment_date: Behandlungsdatum im Format YYYY-MM-DD (null wenn nicht vorhanden)
- diagnosis: Diagnose oder Behandlungsart (kurz, anonymisiert wenn möglich, null wenn nicht vorhanden)
- insurance_relevant: true wenn für Krankenkasse relevant, false wenn Selbstzahler, null wenn unklar""",

    "children": """
Dieses Dokument bezieht sich auf ein Kind oder Kinder. Extrahiere zusätzlich im extra-Feld:
- child_name: Vorname des Kindes (null wenn nicht erkennbar oder aus Datenschutzgründen nicht angeben)
- institution: Name der Institution (Kita, Schule, Verein, Musikschule usw.)
- institution_type: Art der Institution, z.B. "Kita", "Grundschule", "Gymnasium", "Sportverein"
- period: Abrechnungszeitraum, z.B. "2024-09" oder "September 2024" (null wenn nicht vorhanden)""",

    "tax": """
Dieses Dokument bezieht sich auf Steuern. Extrahiere zusätzlich im extra-Feld:
- tax_year: Steuerjahr als Zahl, z.B. 2023 (null wenn nicht erkennbar)
- assessment_type: Art des Bescheids / Dokuments, z.B. "Einkommensteuerbescheid", "Lohnsteuerbescheinigung", "Vorauszahlung", "Jahressteuerbescheinigung Depot"
- deadline: Zahlungs- oder Einspruchsfrist im Format YYYY-MM-DD (null wenn nicht vorhanden)
- tax_amount_due: Nachzahlung als positive Zahl oder Erstattung als negative Zahl (null wenn nicht erkennbar)
- tax_office: Name des Finanzamts (null wenn nicht vorhanden)""",

    "tax_assessment": """
Dieses Dokument ist ein Steuerbescheid oder steuerliches Dokument. Extrahiere zusätzlich im extra-Feld:
- tax_year: Steuerjahr als Zahl, z.B. 2023 (null wenn nicht erkennbar)
- assessment_type: Art des Bescheids, z.B. "Einkommensteuerbescheid", "Grundsteuerbescheid", "Umsatzsteuervoranmeldung"
- deadline: Zahlungs- oder Einspruchsfrist im Format YYYY-MM-DD (null wenn nicht vorhanden)
- tax_amount_due: Nachzahlung als positive Zahl oder Erstattung als negative Zahl (null wenn nicht erkennbar)
- tax_office: Name des Finanzamts (null wenn nicht vorhanden)""",

    "tax_certificate": """
Dieses Dokument ist eine Steuerbescheinigung (Lohnsteuerbescheinigung oder Jahressteuerbescheinigung). Extrahiere zusätzlich im extra-Feld:
- tax_year: Steuerjahr als Zahl
- certificate_type: "Lohnsteuerbescheinigung" oder "Jahressteuerbescheinigung" oder "Freistellungsauftrag"
- issuer: Aussteller (Arbeitgeber oder Bank/Depotbank)
- gross_income: Bruttoarbeitslohn oder Kapitalerträge (null wenn nicht erkennbar)
- tax_withheld: Einbehaltene Lohnsteuer / Kapitalertragsteuer (null wenn nicht erkennbar)""",

    "donation_receipt": """
Dieses Dokument ist eine Spendenquittung oder Zuwendungsbestätigung. Extrahiere zusätzlich im extra-Feld:
- recipient_org: Name der empfangenden Organisation
- donation_date: Datum der Spende im Format YYYY-MM-DD (null wenn nicht vorhanden)
- donation_type: Art der Zuwendung, z.B. "Geldspende", "Sachspende", "Mitgliedsbeitrag"
- tax_deductible: true wenn steuerlich absetzbar (i.d.R. bei gemeinnützigen Organisationen)""",

    "invoice": """
Falls es sich um eine Handwerkerrechnung handelt, extrahiere zusätzlich im extra-Feld:
- craftsman_invoice: true wenn Handwerkerrechnung (steuerlich absetzbar nach §35a EStG)
- work_description: Kurze Beschreibung der ausgeführten Arbeiten (null wenn nicht vorhanden)
- labor_cost: Lohnanteil der Rechnung (null wenn nicht aufgeteilt)""",

    "nk_statement": """
Dieses Dokument ist eine Nebenkostenabrechnung (NK-Abrechnung). Extrahiere zusätzlich im extra-Feld:
- billing_period: Abrechnungszeitraum, z.B. "2023-01-01/2023-12-31"
- advance_payments: Geleistete Vorauszahlungen (null wenn nicht erkennbar)
- balance: Nachzahlung (positiv) oder Guthaben (negativ) (null wenn nicht erkennbar)
- property_address: Adresse des Mietobjekts (null wenn nicht vorhanden)""",
}


# ---------------------------------------------------------------------------
# Doc-type / category detection for prompt selection (pre-LLM heuristic)
# ---------------------------------------------------------------------------

def _detect_relevant_extensions(text: str) -> list[str]:
    """Detect which category-specific prompt extensions to include based on text heuristics."""
    ltext = text.lower()
    extensions = []

    # Contract signals
    if any(w in ltext for w in ["vertrag", "contract", "vereinbarung", "kündigung", "kündigungsfrist",
                                  "laufzeit", "mietvertrag", "mietbeginn"]):
        extensions.append("contract")

    # Authority signals
    if any(w in ltext for w in ["finanzamt", "bescheid", "aktenzeichen", "behörde", "amt",
                                  "bürgeramt", "einwohnermeldeamt", "sozialamt", "jobcenter",
                                  "geschäftszeichen", "gz:", "az:"]):
        extensions.append("authority")

    # Health signals
    if any(w in ltext for w in ["arzt", "ärztin", "praxis", "diagnose", "behandlung",
                                  "krankenkasse", "rezept", "patient", "klinik", "krankenhaus",
                                  "apotheke", "kassenärztlich"]):
        extensions.append("health")

    # Children signals
    if any(w in ltext for w in ["kita", "kindergarten", "schule", "schulgebühr", "hort",
                                  "nachmittagsbetreuung", "sportverein", "musikschule",
                                  "kinder", "elternbeitrag"]):
        extensions.append("children")

    # Tax signals
    if any(w in ltext for w in ["steuer", "finanzamt", "steuerbescheid", "lohnsteuer",
                                  "einkommensteuer", "umsatzsteuer", "kapitalertrag",
                                  "steuerjahr", "veranlagung"]):
        # More specific sub-types
        if any(w in ltext for w in ["lohnsteuerbescheinigung", "jahressteuerbescheinigung",
                                     "kapitalertragsteuer", "freistellungsauftrag"]):
            extensions.append("tax_certificate")
        elif any(w in ltext for w in ["bescheid", "festsetzung", "grundsteuer", "vorauszahlung"]):
            extensions.append("tax_assessment")
        else:
            extensions.append("tax")

    # Donation receipt signals
    if any(w in ltext for w in ["spendenquittung", "zuwendungsbestätigung", "spende",
                                  "gemeinnützig", "förderverein", "ehrenamtlich"]):
        extensions.append("donation_receipt")

    # NK-statement signals
    if any(w in ltext for w in ["nebenkostenabrechnung", "nebenkosten", "betriebskosten",
                                  "heizkostenabrechnung", "nk-abrechnung", "vorauszahlung heizung"]):
        extensions.append("nk_statement")

    # Invoice sub-type (Handwerkerrechnung)
    if any(w in ltext for w in ["handwerker", "montage", "installation", "§35a", "paragraph 35a",
                                  "haushaltsnah", "haushaltsnahe"]):
        if "invoice" not in extensions:
            extensions.append("invoice")

    return extensions


def build_system_prompt(markdown: str) -> str:
    """Build system prompt with category-specific extensions based on document content."""
    extensions = _detect_relevant_extensions(markdown)
    if not extensions:
        return SYSTEM_PROMPT_BASE

    additional = "\n\n--- Zusätzliche Extraktions-Anweisungen basierend auf dem Dokumenttyp ---"
    for ext_key in extensions:
        if ext_key in CATEGORY_PROMPT_EXTENSIONS:
            additional += CATEGORY_PROMPT_EXTENSIONS[ext_key]

    return SYSTEM_PROMPT_BASE + additional


USER_PROMPT_TEMPLATE = """Dokument-Quelle: {source}
Dateiname/ID: {source_id}

Markdown-Inhalt:
{markdown}"""


# ---------------------------------------------------------------------------
# LLM providers
# ---------------------------------------------------------------------------

def call_anthropic(markdown: str, source: str, source_id: str, model: str, api_key: str) -> dict:
    """Call Anthropic Claude API via httpx."""
    import httpx

    system_prompt = build_system_prompt(markdown)
    user_content = USER_PROMPT_TEMPLATE.format(
        source=source,
        source_id=source_id,
        markdown=markdown[:12000],  # cap to avoid token limits
    )

    payload = {
        "model": model or "claude-3-5-haiku-20241022",
        "max_tokens": 1500,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_content}],
    }

    with httpx.Client(timeout=60) as client:
        resp = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["content"][0]["text"].strip()

    return json.loads(text)


def call_openai(markdown: str, source: str, source_id: str, model: str, api_key: str, base_url: str | None = None) -> dict:
    """Call OpenAI-compatible API via httpx."""
    import httpx

    system_prompt = build_system_prompt(markdown)
    user_content = USER_PROMPT_TEMPLATE.format(
        source=source,
        source_id=source_id,
        markdown=markdown[:12000],
    )

    url = (base_url or "https://api.openai.com").rstrip("/") + "/v1/chat/completions"

    payload = {
        "model": model or "gpt-4o-mini",
        "max_tokens": 1500,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
    }

    headers = {"content-type": "application/json"}
    if api_key:
        headers["authorization"] = f"Bearer {api_key}"

    with httpx.Client(timeout=60) as client:
        resp = client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()

    return json.loads(text)


def call_local(markdown: str, source: str, source_id: str, model: str, base_url: str | None = None) -> dict:
    """Call a local OpenAI-compatible endpoint (e.g. Ollama, LM Studio)."""
    default_url = base_url or "http://localhost:11434"
    return call_openai(
        markdown=markdown,
        source=source,
        source_id=source_id,
        model=model or "llama3.2",
        api_key="",
        base_url=default_url,
    )


# ---------------------------------------------------------------------------
# Regex fallback
# ---------------------------------------------------------------------------

def _extract_amount(text: str) -> tuple[float | None, str | None]:
    """Try to extract a monetary amount from text."""
    # Matches: 1.234,56 € / EUR  or  1,234.56 EUR
    patterns = [
        r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*(?:EUR|€)",   # German: 1.234,56 EUR  or  9,99 EUR
        r"(\d{1,3}(?:,\d{3})*\.\d{2})\s*(?:EUR|€|USD|\$)",  # English: 1,234.56 USD
        r"(?:EUR|€|USD|\$)\s*(\d{1,3}(?:,\d{3})*\.\d{2})",
        r"(?:Betrag|Rechnungsbetrag|Total|Summe|Amount)[:\s]+(\d[\d.,]+)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1).strip()
            # Normalise German formats:
            # 1.234,56 → 1234.56  (thousands dot + decimal comma)
            if re.match(r"\d{1,3}(\.\d{3})+,\d{2}$", raw):
                raw = raw.replace(".", "").replace(",", ".")
            # 9,99 → 9.99  (simple decimal comma, no thousands separator)
            elif re.match(r"^\d+,\d{2}$", raw):
                raw = raw.replace(",", ".")
            # 1,234.56 → 1234.56  (thousands comma + decimal dot)
            elif re.match(r"\d{1,3}(,\d{3})+\.\d{2}$", raw):
                raw = raw.replace(",", "")
            try:
                amount = float(raw)
                currency = "EUR" if "€" in text or "EUR" in text else "USD"
                return amount, currency
            except ValueError:
                continue
    return None, None


def _extract_date(text: str, label_hints: list[str] | None = None) -> str | None:
    """Try to extract a date, optionally near a label."""
    patterns = [
        r"\b(\d{4}-\d{2}-\d{2})\b",                          # ISO
        r"\b(\d{2}\.\d{2}\.\d{4})\b",                         # German DD.MM.YYYY
        r"\b(\d{1,2}\.\s*\w+\s*\d{4})\b",                    # 5. Januar 2024
    ]
    de_months = {
        "januar": "01", "februar": "02", "märz": "03", "april": "04",
        "mai": "05", "juni": "06", "juli": "07", "august": "08",
        "september": "09", "oktober": "10", "november": "11", "dezember": "12",
    }

    candidates = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            raw = m.group(1).strip()
            # Normalise German date
            if re.match(r"\d{2}\.\d{2}\.\d{4}", raw):
                d, mo, y = raw.split(".")
                raw = f"{y}-{mo}-{d}"
            elif re.match(r"\d{1,2}\.\s*\w+\s*\d{4}", raw):
                parts = re.split(r"[\s.]+", raw)
                if len(parts) == 3:
                    month_str = parts[1].lower()
                    if month_str in de_months:
                        raw = f"{parts[2]}-{de_months[month_str]}-{int(parts[0]):02d}"
                    else:
                        continue
            candidates.append((m.start(), raw))

    if not candidates:
        return None

    # If label hints given, prefer dates near those labels
    if label_hints:
        for label in label_hints:
            lm = re.search(label, text, re.IGNORECASE)
            if lm:
                label_pos = lm.end()
                nearby = [(abs(pos - label_pos), d) for pos, d in candidates]
                nearby.sort()
                if nearby and nearby[0][0] < 200:
                    return nearby[0][1]

    # Default: return first date found
    return candidates[0][1]


def _extract_party(text: str) -> str | None:
    """Very light heuristic: look for common invoice header patterns."""
    patterns = [
        r"(?:Von|From|Absender|Lieferant|Rechnungssteller)[:\s]+([A-ZÄÖÜa-zäöü][^\n]{2,60})",
        r"^([A-ZÄÖÜ][A-ZÄÖÜa-zäöü\s&.,\-]{4,50})\n",  # capitalised first line
    ]
    for pat in patterns:
        m = re.search(pat, text, re.MULTILINE)
        if m:
            candidate = m.group(1).strip().rstrip(".,")
            if len(candidate) > 3:
                return candidate
    return None


def _detect_doc_type(text: str) -> str:
    ltext = text.lower()
    # More specific types first
    if any(w in ltext for w in ["lohnsteuerbescheinigung"]):
        return "tax_certificate"
    if any(w in ltext for w in ["jahressteuerbescheinigung", "kapitalertragsteuer bescheinigung",
                                  "freistellungsauftrag"]):
        return "tax_certificate"
    if any(w in ltext for w in ["spendenquittung", "zuwendungsbestätigung"]):
        return "donation_receipt"
    if any(w in ltext for w in ["nebenkostenabrechnung", "heizkostenabrechnung", "nk-abrechnung",
                                  "betriebskostenabrechnung"]):
        return "nk_statement"
    if any(w in ltext for w in ["steuerbescheid", "einkommensteuerbescheid", "grundsteuerbescheid",
                                  "festsetzung", "veranlagung"]):
        return "tax_assessment"
    if any(w in ltext for w in ["rechnung", "invoice", "faktura"]):
        return "invoice"
    if any(w in ltext for w in ["mahnung", "reminder", "zahlungserinnerung"]):
        return "reminder"
    if any(w in ltext for w in ["kontoauszug", "bank statement", "umsatz"]):
        return "bank_statement"
    if any(w in ltext for w in ["gehalt", "lohn", "payslip", "gehaltsabrechnung"]):
        return "salary"
    if any(w in ltext for w in ["mietvertrag", "dienstleistungsvertrag", "vertrag", "contract",
                                  "vereinbarung", "kündigung"]):
        return "contract"
    if any(w in ltext for w in ["quittung", "receipt", "kassenbon"]):
        return "receipt"
    if any(w in ltext for w in ["versicherung", "insurance", "police"]):
        return "insurance"
    return "other"


def _detect_category(text: str, doc_type: str) -> str:
    """Detect category based on document text and type."""
    ltext = text.lower()

    if doc_type in ("tax_assessment", "tax_certificate"):
        return "tax"
    if doc_type == "donation_receipt":
        return "tax"  # Donations are tax-relevant
    if doc_type == "nk_statement":
        return "utilities"
    if doc_type == "salary":
        return "salary"
    if doc_type == "insurance":
        return "insurance"

    # Category keywords
    if any(w in ltext for w in ["finanzamt", "steuerbescheid", "steuer", "lohnsteuer"]):
        return "tax"
    if any(w in ltext for w in ["kita", "kindergarten", "schule", "hort", "elternbeitrag",
                                  "nachmittagsbetreuung", "sportverein", "musikschule"]):
        return "children"
    if any(w in ltext for w in ["arzt", "praxis", "krankenhaus", "apotheke", "rezept",
                                  "krankenkasse", "behandlung"]):
        return "health"
    if any(w in ltext for w in ["behörde", "amt", "bürgeramt", "einwohnermeldeamt",
                                  "aktenzeichen", "gz:", "az:"]):
        return "authority"
    if any(w in ltext for w in ["versicherung", "insurance", "police", "prämie"]):
        return "insurance"
    if any(w in ltext for w in ["miete", "nebenkosten", "mietvertrag", "vermieter"]):
        return "rent"
    if any(w in ltext for w in ["gehalt", "lohn", "payslip"]):
        return "salary"
    if any(w in ltext for w in ["strom", "gas", "wasser", "internet", "telefon", "telekom",
                                  "unitymedia", "vodafone", "o2"]):
        return "utilities"
    if any(w in ltext for w in ["amazon", "netflix", "spotify", "apple", "google",
                                  "abonnement", "subscription", "abo"]):
        return "subscription"
    if any(w in ltext for w in ["software", "hosting", "server", "cloud", "saas", "lizenz"]):
        return "software"
    if any(w in ltext for w in ["hotel", "flug", "bahn", "reise", "booking"]):
        return "travel"
    if any(w in ltext for w in ["restaurant", "lebensmittel", "supermarkt", "rewe", "edeka"]):
        return "food"

    return "other"


def _detect_direction(text: str, doc_type: str) -> str | None:
    ltext = text.lower()
    if doc_type in ("invoice", "reminder", "receipt", "nk_statement"):
        return "outgoing"
    if doc_type == "salary":
        return "incoming"
    if doc_type == "tax_assessment":
        # Could be either — check for Nachzahlung vs Erstattung
        if any(w in ltext for w in ["erstattung", "rückzahlung", "guthaben"]):
            return "incoming"
        return "outgoing"
    if doc_type == "tax_certificate":
        return None  # Informational only
    if any(w in ltext for w in ["gutschrift", "credit note", "erstattung", "rückerstattung"]):
        return "incoming"
    return None


def _extract_extra_contract(text: str) -> dict:
    """Extract contract-specific fields for extra JSONB."""
    extra = {}
    ltext = text.lower()

    # Notice period
    notice_m = re.search(
        r"(?:kündigungsfrist|notice\s*period)[:\s]+([^\n.]{3,80})",
        text, re.IGNORECASE
    )
    if notice_m:
        extra["notice_period"] = notice_m.group(1).strip()

    # Contract type
    if "mietvertrag" in ltext:
        extra["contract_type"] = "Mietvertrag"
    elif "versicherungsvertrag" in ltext or "versicherungspolice" in ltext:
        extra["contract_type"] = "Versicherungsvertrag"
    elif "dienstleistungsvertrag" in ltext:
        extra["contract_type"] = "Dienstleistungsvertrag"
    elif "arbeitsvertrag" in ltext:
        extra["contract_type"] = "Arbeitsvertrag"

    # Contract dates
    start_date = _extract_date(text, label_hints=[r"beginn", r"start", r"ab\s*dem", r"laufzeit\s*ab"])
    if start_date:
        extra["contract_start"] = start_date

    end_date = _extract_date(text, label_hints=[r"ende", r"end", r"bis\s*zum", r"läuft\s*bis"])
    if end_date:
        extra["contract_end"] = end_date

    # Auto renewal
    if any(w in ltext for w in ["automatisch verlängert", "automatische verlängerung",
                                  "auto-renewal", "stillschweigend verlängert"]):
        extra["auto_renewal"] = True

    return extra


def _extract_extra_authority(text: str) -> dict:
    """Extract authority-specific fields for extra JSONB."""
    extra = {}

    # Case number / Aktenzeichen
    case_m = re.search(
        r"(?:aktenzeichen|geschäftszeichen|az\.|gz\.)[:\s]+([A-Z0-9/_\-\s]{3,40}?)(?:\n|$|,|\s{2,})",
        text, re.IGNORECASE
    )
    if case_m:
        extra["case_number"] = case_m.group(1).strip()

    # Authority name heuristics — match until end of line
    authority_patterns = [
        r"(Finanzamt\s+[^\n,]{2,40})",
        r"(Bürgeramt\s+[^\n,]{2,40})",
        r"(Einwohnermeldeamt\s+[^\n,]{2,40})",
        r"(Jobcenter\s+[^\n,]{2,40})",
        r"(Sozialamt\s+[^\n,]{2,40})",
        r"(Jugendamt\s+[^\n,]{2,40})",
    ]
    for pat in authority_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            extra["authority_name"] = m.group(1).strip()
            break

    # Deadline
    deadline = _extract_date(text, label_hints=[r"frist", r"deadline", r"bis zum", r"zahlbar bis",
                                                  r"einspruch\s*bis"])
    if deadline:
        extra["deadline"] = deadline

    return extra


def _extract_extra_tax(text: str, doc_type: str) -> dict:
    """Extract tax-specific fields for extra JSONB."""
    extra = {}

    # Tax year
    year_m = re.search(r"(?:steuerjahr|veranlagungszeitraum|für\s+das\s+jahr)[:\s]+(\d{4})", text, re.IGNORECASE)
    if not year_m:
        # Try to find a 4-digit year that looks like a tax year
        years = re.findall(r"\b(20\d{2})\b", text)
        if years:
            year_m_val = sorted(set(years))[0]  # earliest year
            extra["tax_year"] = int(year_m_val)
    else:
        extra["tax_year"] = int(year_m.group(1))

    # Tax office — match until end of line
    ta_m = re.search(r"(Finanzamt\s+[^\n,]{2,40})", text, re.IGNORECASE)
    if ta_m:
        extra["tax_office"] = ta_m.group(1).strip()

    # Deadline
    deadline = _extract_date(text, label_hints=[r"fällig", r"zahlung.*bis", r"einspruch.*bis",
                                                  r"zahlbar\s*bis"])
    if deadline:
        extra["deadline"] = deadline

    # Assessment type
    ltext = text.lower()
    if "lohnsteuerbescheinigung" in ltext:
        extra["assessment_type"] = "Lohnsteuerbescheinigung"
    elif "jahressteuerbescheinigung" in ltext:
        extra["assessment_type"] = "Jahressteuerbescheinigung"
    elif "einkommensteuerbescheid" in ltext:
        extra["assessment_type"] = "Einkommensteuerbescheid"
    elif "grundsteuerbescheid" in ltext:
        extra["assessment_type"] = "Grundsteuerbescheid"
    elif "vorauszahlung" in ltext:
        extra["assessment_type"] = "Vorauszahlung"

    return extra


def _extract_extra_health(text: str) -> dict:
    """Extract health-specific fields for extra JSONB."""
    extra = {}

    # Doctor / practice name
    doctor_m = re.search(
        r"(?:Dr\.|Prof\.|Praxis)[.\s]+([A-ZÄÖÜ][a-zäöü]+(?:\s+[A-ZÄÖÜ][a-zäöü]+)*)",
        text
    )
    if doctor_m:
        extra["doctor_name"] = doctor_m.group(0).strip()

    # Treatment date
    treatment_date = _extract_date(text, label_hints=[r"behandlung", r"leistung.*datum",
                                                        r"datum\s+der\s+behandlung"])
    if treatment_date:
        extra["treatment_date"] = treatment_date

    return extra


def _extract_extra_children(text: str) -> dict:
    """Extract children-specific fields for extra JSONB."""
    extra = {}
    ltext = text.lower()

    # Institution type
    if "kita" in ltext or "kindergarten" in ltext:
        extra["institution_type"] = "Kita"
    elif "grundschule" in ltext:
        extra["institution_type"] = "Grundschule"
    elif "gymnasium" in ltext or "realschule" in ltext or "gesamtschule" in ltext:
        extra["institution_type"] = "Schule"
    elif "sportverein" in ltext or "fußball" in ltext or "tennis" in ltext:
        extra["institution_type"] = "Sportverein"
    elif "musikschule" in ltext:
        extra["institution_type"] = "Musikschule"
    elif "hort" in ltext or "nachmittagsbetreuung" in ltext or "ogata" in ltext:
        extra["institution_type"] = "Hort/Betreuung"

    # Period
    period_m = re.search(r"(?:monat|zeitraum|periode)[:\s]+([\w\s./-]{3,30}?)(?:\n|$|,)", text, re.IGNORECASE)
    if period_m:
        extra["period"] = period_m.group(1).strip()

    return extra


def _extract_extra_donation(text: str) -> dict:
    """Extract donation-receipt-specific fields for extra JSONB."""
    extra = {}
    ltext = text.lower()

    extra["tax_deductible"] = True  # Assumption: if it's a Zuwendungsbestätigung, it's deductible

    # Donation type
    if "sachspende" in ltext:
        extra["donation_type"] = "Sachspende"
    elif "mitgliedsbeitrag" in ltext:
        extra["donation_type"] = "Mitgliedsbeitrag"
    else:
        extra["donation_type"] = "Geldspende"

    # Donation date
    donation_date = _extract_date(text, label_hints=[r"spende.*datum", r"datum.*spende",
                                                       r"eingegangen\s+am"])
    if donation_date:
        extra["donation_date"] = donation_date

    return extra


def _extract_extra_nk(text: str) -> dict:
    """Extract NK-statement-specific fields for extra JSONB."""
    extra = {}

    # Billing period
    period_m = re.search(
        r"(?:abrechnungszeitraum|abrechnungsperiode)[:\s]+([\d./\s\-]{6,30})",
        text, re.IGNORECASE
    )
    if period_m:
        extra["billing_period"] = period_m.group(1).strip()

    # Balance (Nachzahlung / Guthaben)
    balance_m = re.search(
        r"(?:nachzahlung|guthaben|saldo)[:\s]+([\d.,]+)\s*(?:EUR|€)",
        text, re.IGNORECASE
    )
    if balance_m:
        raw = balance_m.group(1)
        if re.match(r"^\d+,\d{2}$", raw):
            raw = raw.replace(",", ".")
        try:
            balance = float(raw)
            ltext = text.lower()
            if "guthaben" in ltext:
                balance = -balance
            extra["balance"] = balance
        except ValueError:
            pass

    return extra


def _extract_extra_invoice_craftsman(text: str) -> dict:
    """Extract craftsman-invoice-specific fields for extra JSONB."""
    extra = {}
    ltext = text.lower()

    if any(w in ltext for w in ["handwerker", "haushaltsnahe", "§35a", "paragraph 35a",
                                  "haushaltsnah"]):
        extra["craftsman_invoice"] = True

    # Work description
    work_m = re.search(
        r"(?:leistung|arbeiten|gewerk|auftrag)[:\s]+([^\n]{5,120})",
        text, re.IGNORECASE
    )
    if work_m:
        extra["work_description"] = work_m.group(1).strip()

    return extra


def _build_extra_regex(text: str, doc_type: str, category: str) -> dict:
    """Build extra JSONB dict using regex heuristics for known doc types."""
    extra = {}

    if doc_type == "contract":
        extra.update(_extract_extra_contract(text))
    if category == "authority" or any(w in text.lower() for w in ["finanzamt", "behörde", "aktenzeichen"]):
        extra.update(_extract_extra_authority(text))
    if doc_type in ("tax_assessment", "tax_certificate") or category == "tax":
        extra.update(_extract_extra_tax(text, doc_type))
    if category == "health":
        extra.update(_extract_extra_health(text))
    if category == "children":
        extra.update(_extract_extra_children(text))
    if doc_type == "donation_receipt":
        extra.update(_extract_extra_donation(text))
    if doc_type == "nk_statement":
        extra.update(_extract_extra_nk(text))
    if doc_type == "invoice":
        extra.update(_extract_extra_invoice_craftsman(text))

    return extra


def classify_with_regex(markdown: str, source: str, source_id: str) -> dict:
    """Regex/heuristic fallback classifier."""
    amount, currency = _extract_amount(markdown)
    doc_date = _extract_date(markdown, label_hints=[r"datum", r"date", r"ausgestellt"])
    due_date = _extract_date(markdown, label_hints=[r"fällig", r"due\s*date", r"zahlbar\s*bis"])
    party = _extract_party(markdown)
    doc_type = _detect_doc_type(markdown)
    category = _detect_category(markdown, doc_type)
    direction = _detect_direction(markdown, doc_type)
    extra = _build_extra_regex(markdown, doc_type, category)

    # Extract invoice number
    inv_m = re.search(r"(?:Rechnungs(?:nummer|nr\.?)|Invoice\s*(?:No\.?|#))[:\s]+([A-Z0-9/_\-]+)", markdown, re.IGNORECASE)
    invoice_number = inv_m.group(1).strip() if inv_m else None

    # Subject: first non-empty line
    first_line = next((ln.strip().lstrip("#").strip() for ln in markdown.splitlines() if ln.strip()), "")
    subject = first_line[:200] if first_line else None

    return {
        "doc_type": doc_type,
        "category": category,
        "amount": amount,
        "currency": currency,
        "party_name": party,
        "invoice_number": invoice_number,
        "doc_date": doc_date,
        "due_date": due_date,
        "direction": direction,
        "subject": subject,
        "summary": None,
        "extra": extra,
        "source": source,
        "source_id": source_id,
        "raw_markdown": markdown,
        "classification_method": "regex",
    }


# ---------------------------------------------------------------------------
# Main
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
    source = params.get("source", "unknown")
    source_id = params.get("source_id", "")
    filename = params.get("filename", source_id)

    if not markdown:
        result = {
            "error": "No markdown provided",
            "source": source,
            "source_id": source_id,
            "classification_method": "none",
        }
        print(json.dumps(result))
        return

    provider = os.environ.get("BUDDY_LLM_PROVIDER", "anthropic").lower()
    model = os.environ.get("BUDDY_LLM_MODEL", "")
    api_key = os.environ.get("BUDDY_LLM_API_KEY", "")
    base_url = os.environ.get("BUDDY_LLM_BASE_URL", "")

    classified: dict | None = None
    method = "regex"

    # Try LLM classification
    if provider in ("anthropic", "openai", "local"):
        try:
            if provider == "anthropic":
                if not api_key:
                    raise ValueError("BUDDY_LLM_API_KEY not set for anthropic provider")
                classified = call_anthropic(
                    markdown=markdown,
                    source=source,
                    source_id=source_id,
                    model=model,
                    api_key=api_key,
                )
                method = "llm"
            elif provider == "openai":
                if not api_key:
                    raise ValueError("BUDDY_LLM_API_KEY not set for openai provider")
                classified = call_openai(
                    markdown=markdown,
                    source=source,
                    source_id=source_id,
                    model=model,
                    api_key=api_key,
                    base_url=base_url or None,
                )
                method = "llm"
            elif provider == "local":
                classified = call_local(
                    markdown=markdown,
                    source=source,
                    source_id=source_id,
                    model=model,
                    base_url=base_url or None,
                )
                method = "llm"
        except Exception as exc:
            # LLM failed — fall through to regex
            classified = None
            method = "regex"
            sys.stderr.write(f"[buddy_classify] LLM call failed ({provider}): {exc}\n")

    if classified is None:
        classified = classify_with_regex(markdown, source, source_id)
    else:
        # Enrich LLM result with mandatory envelope fields
        classified["source"] = source
        classified["source_id"] = source_id
        classified["raw_markdown"] = markdown
        classified["classification_method"] = method
        if "extra" not in classified or not isinstance(classified.get("extra"), dict):
            classified["extra"] = {}

    # Ensure all expected keys are present (graceful defaults)
    defaults = {
        "doc_type": "other",
        "category": "other",
        "amount": None,
        "currency": None,
        "party_name": None,
        "invoice_number": None,
        "doc_date": None,
        "due_date": None,
        "direction": None,
        "subject": filename or None,
        "summary": None,
        "extra": {},
        "source": source,
        "source_id": source_id,
        "raw_markdown": markdown,
        "classification_method": method,
    }
    for key, default in defaults.items():
        classified.setdefault(key, default)

    print(json.dumps(classified, ensure_ascii=False))


if __name__ == "__main__":
    main()
