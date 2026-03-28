"""Migration templates: mapping of each replaceable helper to its Brick-Pipeline equivalent.

This module is a PLAN — not execution. The real helper files are untouched and
still running in production. This mapping documents how each helper WOULD look
if rewritten as a native Brix brick-pipeline.

Categories:
- single_brick   : one db.query call, no LLM
- pipeline       : multi-step pipeline (db.query → llm.batch → db.upsert)
- not_convertible: domain logic too complex for generic bricks
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# The complete mapping: helper_name → migration descriptor
# ---------------------------------------------------------------------------

HELPER_TO_BRICK_MAPPING: dict[str, dict] = {

    # =========================================================================
    # PURE QUERY HELPERS (8) — simple SELECT, no LLM
    # =========================================================================

    "buddy_onedrive_filter": {
        "type": "single_brick",
        "brick": "db.query",
        "specialist": None,
        "config": {
            "connection": "buddy-db",
            "query": (
                "SELECT id, source_id, file_name, extension, folder_path, mime_type "
                "FROM documents "
                "WHERE source = %(source)s "
                "  AND processing_status = 'pending' "
                "  AND item_type = 'file' "
                "  AND LOWER(extension) = ANY(%(extensions)s) "
                "ORDER BY folder_path, file_name "
                "LIMIT %(limit)s"
            ),
        },
        "notes": "Converts extension list to postgres array param; source default 'onedrive:user@example.com'",
    },

    "dedup_filter": {
        "type": "single_brick",
        "brick": "db.query",
        "specialist": None,
        "config": {
            "connection": "buddy-db",
            "query": (
                "SELECT id, source_id FROM documents "
                "WHERE source_id = ANY(%(ids)s) "
                "  AND processing_status != 'pending'"
            ),
        },
        "notes": "Returns already-processed IDs; caller filters input list against result",
    },

    "filter_mails_by_keywords": {
        "type": "single_brick",
        "brick": "filter",
        "specialist": None,
        "config": {
            "condition": "{{ item.subject | lower | contains_any(keywords) or item.bodyPreview | lower | contains_any(keywords) }}",
        },
        "notes": "Pure in-memory filter; no DB needed. keywords passed as pipeline param",
    },

    "inline_extract": {
        "type": "single_brick",
        "brick": "transform",
        "specialist": None,
        "config": {
            "expression": "{{ input.value[0].id if input.value else None }}",
        },
        "notes": "Extracts first message ID from M365 list-mail-messages response",
    },

    "extract_attachment_urls": {
        "type": "single_brick",
        "brick": "transform",
        "specialist": None,
        "config": {
            "expression": "{{ input.value | map(attribute='@microsoft.graph.downloadUrl') | list }}",
        },
        "notes": "Maps download URLs from M365 attachment list",
    },

    "list_files": {
        "type": "single_brick",
        "brick": "db.query",
        "specialist": None,
        "config": {
            "connection": "buddy-db",
            "query": (
                "SELECT id, file_name, file_path, extension "
                "FROM documents "
                "WHERE folder_path LIKE %(path_pattern)s "
                "  AND LOWER(extension) = ANY(%(extensions)s)"
            ),
        },
        "notes": "Replaces filesystem glob; reads from buddy-db documents instead",
    },

    "debug_id": {
        "type": "single_brick",
        "brick": "transform",
        "specialist": None,
        "config": {
            "expression": "{{ input }}",
        },
        "notes": "Pure debug passthrough; can be replaced by brix__inspect_context",
    },

    "debug_raw": {
        "type": "single_brick",
        "brick": "transform",
        "specialist": None,
        "config": {
            "expression": "{{ input }}",
        },
        "notes": "Pure debug passthrough; can be replaced by brix__inspect_context",
    },

    # =========================================================================
    # EXTRACT HELPERS (20) — pattern: db.query → llm.batch OR regex → db.upsert
    # =========================================================================

    "buddy_extract_contacts": {
        "type": "pipeline",
        "specialist": "kontaktdaten",
        "method": "llm",
        "model": "mistral-large-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('kontaktdaten' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-large-latest",
                    "system_prompt": "Extrahiere Kontaktdaten des Absenders/Kreditors als JSON: {creditor_phone, creditor_address, creditor_website, creditor_email}. Kein Markdown, nur JSON.",
                    "user_template": "{{ item.raw_markdown[:3000] }}",
                    "max_tokens": 300,
                    "temperature": 0.1,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "kontaktdaten",
                },
            },
        ],
    },

    "buddy_extract_persons": {
        "type": "pipeline",
        "specialist": "empfaenger",
        "method": "llm",
        "model": "mistral-large-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('empfaenger' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-large-latest",
                    "system_prompt": "Extrahiere Empfaenger und betroffene Personen als JSON. Wichtig bei Versicherungs-/Beihilfe-Dokumenten: Patient kann von Versicherungsnehmer abweichen.",
                    "user_template": "{{ item.raw_markdown[:3000] }}",
                    "max_tokens": 400,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "empfaenger",
                },
            },
        ],
    },

    "buddy_extract_beihilfe": {
        "type": "pipeline",
        "specialist": "beihilfe_bescheid",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('beihilfe_bescheid' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Beihilfe-Bescheid Daten als JSON: {beihilfestelle, aktenzeichen, gesamtbetrag, beihilfesatz, auszahlungsbetrag, bescheiddatum, positionen[]}.",
                    "user_template": "{{ item.raw_markdown[:4000] }}",
                    "max_tokens": 600,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "beihilfe_bescheid",
                },
            },
        ],
    },

    "buddy_extract_debeka": {
        "type": "pipeline",
        "specialist": "debeka_leistung",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('debeka_leistung' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Debeka Leistungsmitteilung als JSON: {leistungsart, erstattungsbetrag, rechnungssteller, rechnungsbetrag, leistungsdatum, versicherte_person}.",
                    "user_template": "{{ item.raw_markdown[:4000] }}",
                    "max_tokens": 500,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "debeka_leistung",
                },
            },
        ],
    },

    "buddy_extract_deadlines": {
        "type": "pipeline",
        "specialist": "fristen",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('fristen' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Fristen und Deadlines als JSON-Array: [{frist_art, datum, beschreibung, prioritaet}]. Nur explizit genannte Fristen.",
                    "user_template": "{{ item.raw_markdown[:3000] }}",
                    "max_tokens": 400,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "fristen",
                },
            },
        ],
    },

    "buddy_extract_insurance": {
        "type": "pipeline",
        "specialist": "versicherung_details",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('versicherung_details' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Versicherungsdetails als JSON: {versicherungsart, versicherungsnummer, versicherer, praemie, laufzeit_bis, versicherungsnehmer}.",
                    "user_template": "{{ item.raw_markdown[:4000] }}",
                    "max_tokens": 500,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "versicherung_details",
                },
            },
        ],
    },

    "buddy_extract_invoice_review": {
        "type": "pipeline",
        "specialist": "rechnungs_review",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('rechnungs_review' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Review diese Rechnung als JSON: {rechnungsnummer, rechnungsdatum, faelligkeit, netto, mwst_satz, brutto, zahlungsziel_tage, plausibel, auffaelligkeiten[]}.",
                    "user_template": "{{ item.raw_markdown[:4000] }}",
                    "max_tokens": 600,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "rechnungs_review",
                },
            },
        ],
    },

    "buddy_extract_kfz": {
        "type": "pipeline",
        "specialist": "kfz",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('kfz' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere KFZ-Daten als JSON: {kennzeichen, fahrzeugtyp, hersteller, modell, baujahr, fahrgestellnummer, hu_datum}.",
                    "user_template": "{{ item.raw_markdown[:3000] }}",
                    "max_tokens": 400,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "kfz",
                },
            },
        ],
    },

    "buddy_extract_line_items": {
        "type": "pipeline",
        "specialist": "rechnungspositionen",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('rechnungspositionen' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere alle Rechnungspositionen als JSON-Array: [{pos_nr, beschreibung, menge, einheit, einzelpreis, gesamtpreis, mwst_satz}].",
                    "user_template": "{{ item.raw_markdown[:5000] }}",
                    "max_tokens": 1000,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "rechnungspositionen",
                },
            },
        ],
    },

    "buddy_extract_payment_terms": {
        "type": "pipeline",
        "specialist": "zahlungsziele",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('zahlungsziele' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Zahlungsziele als JSON: {faelligkeitsdatum, zahlungsziel_tage, skonto_satz, skonto_frist, zahlungsart}.",
                    "user_template": "{{ item.raw_markdown[:3000] }}",
                    "max_tokens": 300,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "zahlungsziele",
                },
            },
        ],
    },

    "buddy_extract_references": {
        "type": "pipeline",
        "specialist": "zahlungsreferenz",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('zahlungsreferenz' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Zahlungsreferenzen als JSON: {verwendungszweck, referenznummer, mandatsreferenz, glaeubiger_id, transaktions_id}.",
                    "user_template": "{{ item.raw_markdown[:3000] }}",
                    "max_tokens": 300,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "zahlungsreferenz",
                },
            },
        ],
    },

    "buddy_extract_salary": {
        "type": "pipeline",
        "specialist": "gehalt_lohn",
        "method": "llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('gehalt_lohn' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere Gehaltsabrechnungsdaten als JSON: {bruttogehalt, nettogehalt, abrechnungsmonat, arbeitgeber, steuerklasse, sozialversicherungsbeitraege}.",
                    "user_template": "{{ item.raw_markdown[:4000] }}",
                    "max_tokens": 600,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "gehalt_lohn",
                },
            },
        ],
    },

    # --- Regex-based extract helpers ---

    "buddy_extract_language": {
        "type": "pipeline",
        "specialist": "sprach_erkennung",
        "method": "regex",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('sprach_erkennung' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "detect_language",
                "type": "python",
                "config": {
                    "script": "helpers/buddy_extract_language.py",
                    "note": "Regex heuristic on first 1000 chars — no LLM needed",
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "sprach_erkennung",
                },
            },
        ],
        "notes": "Regex-only, no LLM. Language markers counted in first 1000 chars.",
    },

    "buddy_extract_tax_ids": {
        "type": "pipeline",
        "specialist": "steuer_ids",
        "method": "regex",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('steuer_ids' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "regex_extract",
                "type": "python",
                "config": {
                    "script": "helpers/buddy_extract_tax_ids.py",
                    "note": "Steuer-ID (11 digits) and Steuernummer (10-11 digits with slashes) via regex",
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "steuer_ids",
                },
            },
        ],
        "notes": "Regex-only extraction of German tax IDs and Steuernummern.",
    },

    "buddy_extract_promocodes": {
        "type": "pipeline",
        "specialist": "promocode",
        "method": "regex",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('promocode' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "regex_extract",
                "type": "python",
                "config": {
                    "script": "helpers/buddy_extract_promocodes.py",
                    "note": "Pattern: PROMO, GUTSCHEIN, CODE followed by alphanumeric 6-20 chars",
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "promocode",
                },
            },
        ],
        "notes": "Regex-only, no LLM. Promo codes extracted by keyword context patterns.",
    },

    "buddy_extract_tax_hints": {
        "type": "pipeline",
        "specialist": "steuer_hinweise",
        "method": "regex_and_llm",
        "model": "mistral-small-latest",
        "steps": [
            {
                "id": "fetch_docs",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": (
                        "SELECT id, raw_markdown FROM documents "
                        "WHERE processing_status = 'done' "
                        "  AND raw_markdown IS NOT NULL "
                        "  AND NOT ('steuer_hinweise' = ANY(COALESCE(extraction_specialists, '{}'))) "
                        "  AND source LIKE %(source_pattern)s "
                        "ORDER BY id LIMIT %(limit)s"
                    ),
                },
            },
            {
                "id": "regex_prefilter",
                "type": "filter",
                "config": {
                    "condition": "{{ 'steuer' in item.raw_markdown | lower or 'finanzamt' in item.raw_markdown | lower }}",
                    "note": "First pass: regex filter to only LLM-process tax-relevant documents",
                },
            },
            {
                "id": "llm_extract",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Extrahiere steuerrelevante Hinweise als JSON: {absetzbar, kategorie, betrag, hinweis_text, steuerart}.",
                    "user_template": "{{ item.raw_markdown[:4000] }}",
                    "max_tokens": 400,
                },
            },
            {
                "id": "upsert_results",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "document_extractions",
                    "conflict_key": ["document_id", "specialist_name"],
                    "mark_specialist": "steuer_hinweise",
                },
            },
        ],
        "notes": "Two-pass: regex prefilter then LLM. Only docs with 'steuer'/'finanzamt' get LLM.",
    },

    # =========================================================================
    # MARKITDOWN HELPERS (4) — Download → Convert
    # =========================================================================

    "buddy_onedrive_download": {
        "type": "pipeline",
        "specialist": None,
        "steps": [
            {
                "id": "get_file_list",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": "SELECT id, source_id, file_name, extension, folder_path, mime_type FROM documents WHERE source LIKE 'onedrive%' AND processing_status = 'pending' AND item_type = 'file' LIMIT %(limit)s",
                },
            },
            {
                "id": "get_download_url",
                "type": "mcp_call",
                "config": {
                    "server": "m365",
                    "tool": "get-drive-root-item",
                    "args": {"driveId": "{{ item.source_id | split(':')[0] }}", "driveItemId": "{{ item.source_id }}"},
                },
            },
            {
                "id": "convert",
                "type": "markitdown.convert",
                "config": {
                    "url": "{{ step.get_download_url.result['@microsoft.graph.downloadUrl'] }}",
                    "file_name": "{{ item.file_name }}",
                },
            },
            {
                "id": "update_db",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "documents",
                    "conflict_key": ["id"],
                    "fields": {"raw_markdown": "{{ step.convert.markdown }}", "processing_status": "done"},
                },
            },
        ],
        "notes": "Each file: M365 download URL → MarkItDown REST API → update buddy-db",
    },

    "convert_files": {
        "type": "pipeline",
        "specialist": None,
        "steps": [
            {
                "id": "list_input_files",
                "type": "python",
                "config": {
                    "script": "helpers/list_files.py",
                    "args": {"path_pattern": "{{ input.folder }}", "extensions": ["pdf", "docx", "xlsx"]},
                },
            },
            {
                "id": "convert",
                "type": "markitdown.convert",
                "config": {
                    "file_path": "{{ item.path }}",
                    "parallel": True,
                    "concurrency": 4,
                },
            },
            {
                "id": "save",
                "type": "file_write",
                "config": {
                    "path": "{{ input.output_dir }}/{{ item.file_name }}.md",
                    "content": "{{ step.convert.markdown }}",
                },
            },
        ],
        "notes": "Replaces async httpx-based batch conversion; parallel markitdown.convert brick",
    },

    "buddy_onedrive_backfill_familie": {
        "type": "pipeline",
        "specialist": None,
        "steps": [
            {
                "id": "fetch_folder",
                "type": "mcp_call",
                "config": {
                    "server": "m365",
                    "tool": "list-folder-files",
                    "args": {
                        "driveId": "8DF5EDA212792823",
                        "driveItemId": "8DF5EDA212792823!146275",
                        "fetchAllPages": True,
                    },
                },
            },
            {
                "id": "upsert_metadata",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "documents",
                    "conflict_key": ["source_id"],
                    "fields": {"extra": "{{ item | to_json }}"},
                },
            },
        ],
        "notes": "Non-recursive — only folder 8DF5EDA212792823!146275. Single M365 call + DB update.",
    },

    "analyze_unknown_sonstiges": {
        "type": "pipeline",
        "specialist": None,
        "steps": [
            {
                "id": "fetch_candidates",
                "type": "db.query",
                "config": {
                    "connection": "buddy-db",
                    "query": "SELECT id, source_id, file_name, web_url FROM documents WHERE (LOWER(file_name) LIKE '%unknown%' OR LOWER(file_name) LIKE '%sonstiges%') AND processing_status = 'done' LIMIT %(limit)s",
                },
            },
            {
                "id": "get_download_url",
                "type": "mcp_call",
                "config": {
                    "server": "m365",
                    "tool": "get-drive-root-item",
                    "args": {"driveId": "8DF5EDA212792823", "driveItemId": "{{ item.source_id }}"},
                },
            },
            {
                "id": "convert",
                "type": "markitdown.convert",
                "config": {"url": "{{ step.get_download_url.result['@microsoft.graph.downloadUrl'] }}"},
            },
            {
                "id": "classify",
                "type": "llm.batch",
                "config": {
                    "model": "mistral-small-latest",
                    "system_prompt": "Klassifiziere dieses Dokument. Gib doc_type, category und subject zurueck.",
                    "user_template": "{{ step.convert.markdown[:3000] }}",
                    "max_tokens": 200,
                },
            },
            {
                "id": "update_classification",
                "type": "db.upsert",
                "config": {
                    "connection": "buddy-db",
                    "table": "documents",
                    "conflict_key": ["id"],
                    "fields": {"doc_type": "{{ step.classify.doc_type }}", "category": "{{ step.classify.category }}"},
                },
            },
        ],
        "notes": "Pre-collected file list replaces MCP calls in helper. URL → MarkItDown → LLM classify → DB update.",
    },

    # =========================================================================
    # UPSERT / WRITE HELPERS (6)
    # =========================================================================

    "save_attachment": {
        "type": "single_brick",
        "brick": "file_write",
        "specialist": None,
        "config": {
            "path": "{{ input.output_dir }}/{{ item.mail_date[:10] }}_{{ item.mail_subject[:40] | slugify }}_{{ item.name }}",
            "content_bytes": "{{ item.content_bytes | b64decode }}",
        },
        "notes": "Structured filename: date_subject_name. Content decoded from base64.",
    },

    "save_markdown": {
        "type": "single_brick",
        "brick": "file_write",
        "specialist": None,
        "config": {
            "path": "{{ input.output_dir }}/{{ item.file_name }}.md",
            "content": "{{ item.markdown }}",
        },
        "notes": "Pure file write; replaces Path.write_text loop.",
    },

    "structured_save": {
        "type": "single_brick",
        "brick": "file_write",
        "specialist": None,
        "config": {
            "path": "{{ input.output_dir }}/{{ item.category }}/{{ item.date }}_{{ item.name }}",
            "content": "{{ item.content }}",
        },
        "notes": "Category-based directory structure. Requires auto-mkdir support in file_write brick.",
    },

    "buddy_ingest_transactions": {
        "type": "single_brick",
        "brick": "http_post",
        "specialist": None,
        "config": {
            "url": "http://buddy-api:8030/v1/ingest",
            "body": "{{ item }}",
            "headers": {"Content-Type": "application/json"},
        },
        "notes": "POSTs each transaction to buddy-api. Replaces httpx loop in Python helper.",
    },

    "insert_birthdays": {
        "type": "single_brick",
        "brick": "db.upsert",
        "specialist": None,
        "config": {
            "connection": "buddy-db",
            "table": "contacts",
            "conflict_key": ["name", "birthdate"],
            "fields": {"name": "{{ item.name }}", "birthdate": "{{ item.date }}", "source": "ics_import"},
        },
        "notes": "Inserts parsed ICS birthdays; pairs with parse_ics_birthdays pipeline step.",
    },

    "parse_ics_birthdays": {
        "type": "single_brick",
        "brick": "python",
        "specialist": None,
        "config": {
            "script": "helpers/parse_ics_birthdays.py",
            "args": {"ics_path": "{{ input.ics_path }}"},
        },
        "notes": "ICS parser outputs [{name, date}] list for insert_birthdays step.",
    },

    # =========================================================================
    # NOT CONVERTIBLE (9) — domain logic too complex for generic bricks
    # =========================================================================

    "buddy_extract_iban": {
        "type": "not_convertible",
        "reason": (
            "3-pass MOD-97 checksum validation (ISO 7064) with per-country length tables "
            "and BIC association logic. Requires custom Python — no generic brick equivalent."
        ),
        "specialist": "iban_extraktor",
    },

    "buddy_onedrive_scan": {
        "type": "not_convertible",
        "reason": (
            "Recursive folder traversal with depth tracking, parent_document_id linking, "
            "content-hash dedup, and conditional status logic. "
            "Stateful recursion cannot be expressed as a linear brick pipeline."
        ),
    },

    "buddy_intake_onedrive": {
        "type": "not_convertible",
        "reason": (
            "Per-file pipeline: SHA256 content-hash dedup → MarkItDown → LLM classify → buddy-api ingest. "
            "Complex error handling and conditional skipping per file requires orchestrator-level logic."
        ),
    },

    "buddy_intake_process": {
        "type": "not_convertible",
        "reason": (
            "Handles both attachment-based and body-based email documents with different paths, "
            "async httpx, base64 decoding, dedup, MarkItDown, classify, ingest. "
            "Multi-modal branching per mail item is too complex for a single brick chain."
        ),
    },

    "buddy_extract_cases": {
        "type": "not_convertible",
        "reason": (
            "Cross-specialist JOIN logic: links Debeka Leistungsmitteilungen with Arzt-Rechnungen, "
            "Beihilfe-Bescheide, and Transaktionen using date/amount tolerance windows. "
            "Requires multi-table fuzzy matching not expressible as simple db.query bricks."
        ),
    },

    "buddy_classify": {
        "type": "not_convertible",
        "reason": (
            "Single-document real-time LLM classification for intake pipeline (not batch). "
            "Called inline from buddy_intake_process/onedrive with per-item context injection. "
            "Tightly coupled to intake flow; cannot be decoupled without refactoring pipeline."
        ),
    },

    "buddy_parse_bank": {
        "type": "not_convertible",
        "reason": (
            "Parses two Sparkasse CSV formats (Giro CAMT V2 and Kreditkarte) with ISO-8859-15 encoding, "
            "German decimal parsing (comma separator), and format auto-detection logic. "
            "Bank-specific format handling requires custom Python."
        ),
    },

    "buddy_parse_kontoauszug": {
        "type": "not_convertible",
        "reason": (
            "OCR-converted bank statement Markdown parser with regex-first heuristics and LLM fallback. "
            "Multi-pass logic (regex attempt → quality check → LLM retry) requires stateful Python."
        ),
    },

    "buddy_llm_batch": {
        "type": "not_convertible",
        "reason": (
            "Core Mistral Batch API wrapper: submits jobs, polls until completion, handles timeouts. "
            "Infrastructure helper used by all LLM extract helpers. "
            "Will be superseded by the llm.batch brick natively — not a pipeline candidate."
        ),
    },
}


# ---------------------------------------------------------------------------
# MCP Tool: analyze_migration
# ---------------------------------------------------------------------------

def analyze_migration(helper_name: str | None = None) -> dict:
    """Analyse the HELPER_TO_BRICK_MAPPING and return migration summary.

    Args:
        helper_name: If given, returns detail for that specific helper.
                     If None, returns full summary across all helpers.

    Returns:
        dict with keys:
            - summary: counts by type
            - helpers: list of helper migration descriptors
            - total: total helper count
    """
    if helper_name is not None:
        entry = HELPER_TO_BRICK_MAPPING.get(helper_name)
        if entry is None:
            return {
                "success": False,
                "error": f"Helper '{helper_name}' not found in migration mapping.",
                "available": sorted(HELPER_TO_BRICK_MAPPING.keys()),
            }
        return {
            "success": True,
            "helper": helper_name,
            "migration": entry,
        }

    # Full summary
    by_type: dict[str, list[str]] = {}
    detailed: list[dict] = []

    for name, desc in sorted(HELPER_TO_BRICK_MAPPING.items()):
        entry_type = desc.get("type", "unknown")
        by_type.setdefault(entry_type, []).append(name)

        detail: dict = {
            "name": name,
            "type": entry_type,
        }

        if entry_type == "single_brick":
            detail["brick"] = desc.get("brick")
            detail["notes"] = desc.get("notes", "")
        elif entry_type == "pipeline":
            detail["specialist"] = desc.get("specialist")
            detail["method"] = desc.get("method", "llm")
            detail["step_count"] = len(desc.get("steps", []))
            detail["steps"] = [s.get("id") for s in desc.get("steps", [])]
            if desc.get("notes"):
                detail["notes"] = desc["notes"]
        elif entry_type == "not_convertible":
            detail["reason"] = desc.get("reason", "")
            detail["specialist"] = desc.get("specialist")

        detailed.append(detail)

    convertible = [
        name for name, d in HELPER_TO_BRICK_MAPPING.items()
        if d.get("type") != "not_convertible"
    ]
    not_convertible = [
        name for name, d in HELPER_TO_BRICK_MAPPING.items()
        if d.get("type") == "not_convertible"
    ]

    return {
        "success": True,
        "total": len(HELPER_TO_BRICK_MAPPING),
        "convertible_count": len(convertible),
        "not_convertible_count": len(not_convertible),
        "summary": {
            "by_type": {k: len(v) for k, v in sorted(by_type.items())},
            "convertible": sorted(convertible),
            "not_convertible": sorted(not_convertible),
        },
        "helpers": detailed,
    }
