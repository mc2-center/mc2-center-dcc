from types import SimpleNamespace

import pandas as pd

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
