import pandas as pd

from conftest import load_script
from fakes import make_stub_table

ela = load_script("annotations/edit_legacy_annotations.py", "ela_mod")


def test_writeback_strips_newlines(monkeypatch):
    frame = pd.DataFrame({"Publication Title": ["line one\nline two\r\nline three"], "n": [1]})
    table = make_stub_table(frame)
    monkeypatch.setattr(ela, "Table", table)
    ela.update_manifest_tables(
        None, ["syn1"], pd.Series(dtype=object), dryrun=False
    )
    (df,) = table.stored
    assert df["Publication Title"].tolist() == ["line one line two line three"]
    assert df["n"].tolist() == [1]
