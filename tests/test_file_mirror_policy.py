from __future__ import annotations

import pytest

from brix.file_mirror_policy import (
    FILE_MIRROR_POLICY,
    allowed_file_mirror_purposes,
    is_authoritative_file_purpose,
)


def test_files_are_never_authoritative_authoring_truth() -> None:
    assert FILE_MIRROR_POLICY.authoritative_authoring_truth == "db"
    assert FILE_MIRROR_POLICY.files_are_authoritative is False


def test_allowed_purposes_are_non_authoritative() -> None:
    assert allowed_file_mirror_purposes() == (
        "export",
        "backup",
        "bundle",
        "debug",
        "legacy_import",
    )
    for purpose in allowed_file_mirror_purposes():
        assert is_authoritative_file_purpose(purpose) is False


def test_unknown_file_mirror_purpose_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown file mirror purpose"):
        is_authoritative_file_purpose("authoring")
