from types import SimpleNamespace

import pandas as pd
import pytest

from conftest import load_script

upa = load_script("annotations/update_pending_annotations.py", "upa_main_mod")


def test_dryrun_does_not_upload(monkeypatch, tmp_path):
    uploads = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("builtins.input", lambda *_: "y")
    monkeypatch.setattr(upa._portal_utils, "syn_login", lambda: object())
    monkeypatch.setattr(
        upa,
        "get_args",
        lambda: SimpleNamespace(manifest_path="m.csv", table_id="syn1", dryrun=True),
    )
    monkeypatch.setattr(upa, "get_updated_df", lambda p: pd.DataFrame())
    monkeypatch.setattr(upa, "get_annotations", lambda *a: pd.DataFrame())
    # edit_annotations returns None on a dry run
    monkeypatch.setattr(upa, "edit_annotations", lambda *a: None)
    monkeypatch.setattr(upa, "manifest_upload", lambda *a: uploads.append(a))
    upa.main()
    assert uploads == []


def test_real_run_uploads(monkeypatch, tmp_path):
    uploads = []
    monkeypatch.setattr("builtins.input", lambda *_: "y")
    monkeypatch.setattr(upa._portal_utils, "syn_login", lambda: object())
    monkeypatch.setattr(
        upa,
        "get_args",
        lambda: SimpleNamespace(manifest_path="m.csv", table_id="syn1", dryrun=False),
    )
    monkeypatch.setattr(upa, "get_updated_df", lambda p: pd.DataFrame())
    monkeypatch.setattr(upa, "get_annotations", lambda *a: pd.DataFrame())
    monkeypatch.setattr(upa, "edit_annotations", lambda *a: pd.DataFrame({"a": [1]}))
    monkeypatch.setattr(upa, "manifest_upload", lambda *a: uploads.append(a))
    upa.main()
    assert len(uploads) == 1


def test_get_args_defaults_to_the_staging_publications_table(monkeypatch):
    monkeypatch.setattr("sys.argv", ["update_pending_annotations.py"])
    assert upa.get_args().table_id == "syn78748028"


def test_get_args_rejects_a_production_table(monkeypatch):
    monkeypatch.setattr("sys.argv", ["update_pending_annotations.py", "-t", "syn21868591"])
    with pytest.raises(SystemExit):
        upa.get_args()
