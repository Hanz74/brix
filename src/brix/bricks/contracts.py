"""Standard inter-brick payload contracts.

Contracts name stable domain payload shapes so bricks can be composed without
relying on ad hoc dict conventions hidden inside individual pipelines.
"""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

ContractKind = Literal["object", "list", "scalar"]


@dataclass(frozen=True)
class BrickContract:
    """Versioned domain payload contract used by brick input/output types."""

    name: str
    domain: str
    kind: ContractKind
    description: str
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...] = ()
    compatible_with: tuple[str, ...] = ()
    validator_notes: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a serializable representation for MCP/docs callers."""
        return {
            "name": self.name,
            "domain": self.domain,
            "kind": self.kind,
            "description": self.description,
            "required_fields": list(self.required_fields),
            "optional_fields": list(self.optional_fields),
            "compatible_with": list(self.compatible_with),
            "validator_notes": self.validator_notes,
        }


_CONTRACTS: dict[str, BrickContract] = {
    "file_blob": BrickContract(
        name="file_blob",
        domain="file",
        kind="object",
        description="Local file payload prepared for extraction or persistence.",
        required_fields=("file_bytes_path",),
        optional_fields=("file_size", "mime_type", "content_hash", "name", "extension", "base64"),
        compatible_with=("dict", "object", "document_extract_input"),
        validator_notes="Treat as dict-like; can feed document extraction preparation.",
    ),
    "remote_download_result": BrickContract(
        name="remote_download_result",
        domain="download",
        kind="object",
        description="Result of downloading a remote object to a managed local file.",
        required_fields=("file_bytes_path",),
        optional_fields=("source_url", "file_size", "mime_type", "content_hash", "extractable", "file"),
        compatible_with=("dict", "object", "file_blob", "document_extract_input"),
        validator_notes="Treat as dict-like; can feed file_blob and document_extract_input consumers.",
    ),
    "document_extract_input": BrickContract(
        name="document_extract_input",
        domain="extract",
        kind="object",
        description="Normalized payload passed into a document extraction brick.",
        required_fields=("file_bytes_path",),
        optional_fields=("mime_type", "base64", "markdown", "language", "metadata"),
        compatible_with=("dict", "object"),
        validator_notes="Treat as dict-like input for extraction bricks.",
    ),
    "document_extraction_result": BrickContract(
        name="document_extraction_result",
        domain="extract",
        kind="object",
        description="Structured document extraction output with normalized fields and quality metadata.",
        required_fields=("normalized",),
        optional_fields=("document_type", "quality_score", "_quality_score", "markdown", "raw", "warnings"),
        compatible_with=("dict", "object", "db_mutation_result"),
        validator_notes="Treat as dict-like; persistence bricks should consume it through explicit params.",
    ),
    "db_query_result": BrickContract(
        name="db_query_result",
        domain="db",
        kind="list",
        description="Rows returned by a read-only database query.",
        required_fields=(),
        optional_fields=("rows", "row_count", "columns"),
        compatible_with=("list[dict]", "list[object]", "list[*]", "object|list"),
        validator_notes="Treat as list-like and list[dict]-compatible.",
    ),
    "db_mutation_result": BrickContract(
        name="db_mutation_result",
        domain="db",
        kind="object",
        description="Database write result for INSERT, UPDATE, DELETE, or UPSERT operations.",
        required_fields=("affected_rows",),
        optional_fields=("inserted_rows", "updated_rows", "deleted_rows", "returned_rows", "errors"),
        compatible_with=("dict", "object"),
        validator_notes="Treat as dict-like write outcome, not as query rows.",
    ),
    "classification_result": BrickContract(
        name="classification_result",
        domain="classification",
        kind="object",
        description="Classification output with category and rationale.",
        required_fields=("category",),
        optional_fields=("confidence", "rationale", "labels", "metadata"),
        compatible_with=("dict", "object"),
        validator_notes="Treat as dict-like.",
    ),
}

STANDARD_BRICK_CONTRACTS = MappingProxyType(_CONTRACTS)

STANDARD_CONTRACT_COMPATIBILITY: dict[str, list[str]] = {
    name: [name, *contract.compatible_with]
    for name, contract in STANDARD_BRICK_CONTRACTS.items()
}

DICT_LIKE_CONTRACTS = frozenset(
    name for name, contract in STANDARD_BRICK_CONTRACTS.items() if contract.kind == "object"
)
LIST_LIKE_CONTRACTS = frozenset(
    name for name, contract in STANDARD_BRICK_CONTRACTS.items() if contract.kind == "list"
)


def list_contracts() -> list[BrickContract]:
    """Return all standard contracts in stable name order."""
    return [STANDARD_BRICK_CONTRACTS[name] for name in sorted(STANDARD_BRICK_CONTRACTS)]


def contract_names() -> tuple[str, ...]:
    """Return the canonical standard contract names."""
    return tuple(sorted(STANDARD_BRICK_CONTRACTS))


def get_contract(name: str) -> BrickContract | None:
    """Return a standard contract by case-insensitive name."""
    return STANDARD_BRICK_CONTRACTS.get((name or "").strip().lower())


def is_standard_contract(name: str) -> bool:
    """Return True when name is a canonical standard contract."""
    return get_contract(name) is not None


def is_dict_like_contract(name: str) -> bool:
    """Return True when a contract behaves like a single object/dict payload."""
    contract = get_contract(name)
    return bool(contract and contract.kind == "object")


def is_list_like_contract(name: str) -> bool:
    """Return True when a contract behaves like a list payload."""
    contract = get_contract(name)
    return bool(contract and contract.kind == "list")
