from brix.engine_types import _capture_environment


def test_capture_environment_prefers_current_brix_version():
    snapshot = _capture_environment()

    installed = snapshot.get("installed_packages", [])
    assert isinstance(installed, list)
    brix_entries = [entry for entry in installed if isinstance(entry, str) and entry.startswith("brix==")]
    assert len(brix_entries) == 1
    assert snapshot.get("brix_version")
    assert brix_entries[0] == f"brix=={snapshot['brix_version']}"
