import pandas as pd

from conftest import load_script

ela = load_script("annotations/edit_legacy_annotations.py", "ela_mod")


class StubTable:
    stored = []

    def __init__(self, id=None):
        self.id = id

    def query(self, query=None, synapse_client=None):
        return pd.DataFrame(
            {"Publication Title": ["line one\nline two\r\nline three"], "n": [1]}
        )

    def store_rows(self, values, synapse_client=None):
        StubTable.stored.append(values)


def test_writeback_strips_newlines(monkeypatch):
    StubTable.stored = []
    monkeypatch.setattr(ela, "Table", StubTable)
    ela.update_manifest_tables(
        None, ["syn1"], pd.Series(dtype=object), dryrun=False
    )
    (df,) = StubTable.stored
    assert df["Publication Title"].tolist() == ["line one line two line three"]
    assert df["n"].tolist() == [1]
