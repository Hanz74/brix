#!/usr/bin/env python3
"""Analyze OneDrive PDFs with 'unknown' or 'sonstiges' in filename via MarkItDown classify.

Expects pre-collected file list as input (no MCP calls needed).
Downloads via pre-signed URLs from M365 MCP, classifies via MarkItDown.
"""
import json
import subprocess
import sys
import base64
import re
import time

MCP_CONTAINER = "m365"
MCP_CMD = "ms-365-mcp-server"
MCP_TIMEOUT = 120


def _mcp_call(tool_name, tool_params):
    """Execute a single MCP tool call via docker exec subprocess."""
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "analyze_unknown_sonstiges", "version": "1.0"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": tool_params,
            },
        },
    ]
    input_data = "\n".join(json.dumps(m) for m in messages) + "\n"

    proc = subprocess.run(
        ["docker", "exec", "-i", MCP_CONTAINER, MCP_CMD],
        input=input_data,
        capture_output=True,
        text=True,
        timeout=MCP_TIMEOUT,
    )

    for line in proc.stdout.strip().splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("id") == 2 and "result" in msg:
            content = msg["result"].get("content", [])
            for c in content:
                if c.get("type") == "text":
                    try:
                        return json.loads(c["text"])
                    except json.JSONDecodeError:
                        return c["text"]
    return None


def main():
    if len(sys.argv) > 1:
        params = json.loads(sys.argv[1])
    elif not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
        params = json.loads(raw) if raw else {}
    else:
        params = {}

    import httpx

    drive_id = params.get("drive_id", "8DF5EDA212792823")
    # files list: [{id, name, size, folder}, ...]
    files = params.get("files", [])

    if not files:
        print(json.dumps({"error": "no files provided in input"}))
        return

    print(f"[INFO] Processing {len(files)} files", file=sys.stderr)

    results = []
    errors = []

    for i, f in enumerate(files):
        fname = f["name"]
        print(f"[{i+1}/{len(files)}] Processing: {fname}", file=sys.stderr)

        # Parse sender and type from filename pattern: YYYYMMDD-sender-type-rest.pdf
        parts = fname.split("-", 3)
        current_sender = parts[1] if len(parts) > 1 else "unknown"
        current_typ = parts[2] if len(parts) > 2 else "unknown"

        # Skip files > 10MB to avoid timeouts
        if f.get("size", 0) > 10_000_000:
            print(f"  SKIP: too large ({f['size']} bytes)", file=sys.stderr)
            errors.append({"file": fname, "error": "too large", "size": f["size"]})
            continue

        try:
            # Download file via M365 MCP
            dl_resp = _mcp_call("download-onedrive-file-content", {
                "driveId": drive_id,
                "driveItemId": f["id"],
            })
            download_url = None
            if isinstance(dl_resp, dict):
                download_url = dl_resp.get("@microsoft.graph.downloadUrl") or dl_resp.get("downloadUrl")
            if not download_url:
                print(f"  ERROR: no downloadUrl in response: {str(dl_resp)[:200]}", file=sys.stderr)
                errors.append({"file": fname, "error": "no downloadUrl"})
                continue

            # Download actual content
            with httpx.Client(follow_redirects=True, timeout=60) as client:
                file_resp = client.get(download_url)
                file_resp.raise_for_status()
                file_bytes = file_resp.content

            b64 = base64.b64encode(file_bytes).decode()

            # Call MarkItDown with classify
            with httpx.Client(timeout=120) as client:
                mid_resp = client.post("http://markitdown-mcp:8081/v1/convert", json={
                    "base64": b64,
                    "filename": fname,
                    "classify": True,
                    "language": "de",
                })
                mid_resp.raise_for_status()
                mid_data = mid_resp.json()

            meta = mid_data.get("meta", {})
            markdown = mid_data.get("markdown", "")
            doc_type = meta.get("document_type", "unknown")
            confidence = meta.get("document_type_confidence", 0)

            # Try to detect vendor from first 500 chars of markdown
            md_snippet = markdown[:500] if markdown else ""
            detected_vendor = _detect_vendor(md_snippet, fname)

            results.append({
                "file_name": fname,
                "folder": f.get("folder", ""),
                "current_sender": current_sender,
                "current_typ": current_typ,
                "markitdown_doc_type": doc_type,
                "markitdown_confidence": confidence,
                "detected_vendor": detected_vendor,
                "markdown_first_200": markdown[:200] if markdown else "",
            })
            print(f"  OK: type={doc_type} conf={confidence} vendor={detected_vendor}", file=sys.stderr)

        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            errors.append({"file": fname, "error": str(e)})
            continue

    # Build summary
    by_type = {}
    for r in results:
        t = r["markitdown_doc_type"]
        by_type[t] = by_type.get(t, 0) + 1

    unknown_sender_fixable = sum(1 for r in results if r["current_sender"] == "unknown" and r["detected_vendor"] and r["detected_vendor"] != "unknown")
    sonstiges_reclassifiable = sum(1 for r in results if r["current_typ"] == "sonstiges" and r["markitdown_doc_type"] not in ("unknown", "other", "sonstiges", None))

    report = {
        "total_analysed": len(results),
        "total_errors": len(errors),
        "results": results,
        "errors": errors,
        "summary": {
            "by_markitdown_type": by_type,
            "unknown_sender_fixable": unknown_sender_fixable,
            "sonstiges_reclassifiable": sonstiges_reclassifiable,
        }
    }

    print(json.dumps(report, ensure_ascii=False))


def _detect_vendor(markdown_snippet, filename):
    """Try to identify vendor/sender from markdown content and filename."""
    snippet = markdown_snippet.lower()
    fname = filename.lower()

    vendors = {
        "Vodafone": ["vodafone"],
        "Telekom": ["telekom", "t-mobile", "deutsche telekom"],
        "IONOS": ["ionos", "1&1"],
        "Amazon": ["amazon", "amzn"],
        "Thalia": ["thalia"],
        "Onfy": ["onfy"],
        "Nike": ["nike"],
        "Bershka": ["bershka"],
        "Mydays": ["mydays"],
        "HanseMerkur": ["hansemerkur", "hanse merkur", "hanse-merkur"],
        "Debeka": ["debeka"],
        "PFG Distribution": ["pfg distribution", "pfg_distribution"],
        "PME Legend": ["pme legend", "pme_legend"],
        "Petrol Industries": ["petrol industries", "petrol_industries"],
        "Deutsche Post": ["deutsche post"],
        "Sparkasse": ["sparkasse"],
        "Postbank": ["postbank"],
        "ERV/ERGO Reiseversicherung": ["ergo reiseversicherung", "ergo versicherung", "erv ", "europäische reiseversicherung"],
        "Booking.com": ["booking.com", "booking"],
        "Triple / EIS": ["triple", "eis.de", "eis gmbh"],
        "Birkenstock": ["birkenstock"],
        "DHL": ["dhl"],
        "Caritasverband": ["caritas", "caritasverband"],
        "Diakonie": ["diakonie"],
        "PVS": ["pvs", "privatärztliche verrechnungsstelle"],
        "Amtsgericht": ["amtsgericht"],
        "Hetzner": ["hetzner"],
        "Mistral": ["mistral"],
        "Immobilien": ["immobilien", "immoscout", "immowelt"],
        "Reha Team": ["reha team", "reha-team"],
        "Sanitätshaus": ["sanitätshaus", "sanittshaus"],
        "Sanifuchs": ["sanifuchs", "sani-fuchs"],
        "Bildung GmbH": ["bildung"],
        "VDI": ["vdi"],
        "Gemeinschaftspraxis": ["gemeinschaftspraxis"],
    }

    for vendor, keywords in vendors.items():
        for kw in keywords:
            if kw in snippet or kw in fname:
                return vendor

    # Try to find company names from common patterns
    patterns = [
        r"([A-ZÄÖÜ][a-zäöüß]+ (?:GmbH|AG|SE|KG|e\.V\.|Co\. KG))",
        r"(?:firma|von|absender|company)[:\s]+([A-ZÄÖÜ][a-zäöüß]+(?: [A-ZÄÖÜ][a-zäöüß]+){0,3})",
    ]
    for p in patterns:
        m = re.search(p, markdown_snippet)
        if m:
            return m.group(1).strip()

    return "unknown"


if __name__ == "__main__":
    main()
