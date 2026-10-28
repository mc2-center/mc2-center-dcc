import pandas as pd
from types import SimpleNamespace

from conftest import load_script

upa = load_script("annotations/update_pending_annotations.py", "upa_mod")


class StubTable:
    column_types = {}
    stored = None

    def __init__(self, id=None):
        self.id = id

    def get(self, include_columns=True, synapse_client=None):
        cols = {n: SimpleNamespace(column_type=t) for n, t in self.column_types.items()}
        return SimpleNamespace(columns=cols)

    def store_rows(self, values, synapse_client=None):
        StubTable.stored = values


def _edit(monkeypatch, types, annots, updated, dryrun=False):
    StubTable.column_types = types
    monkeypatch.setattr(upa, "Table", StubTable)
    return upa.edit_annotations(updated, annots, None, "syn1", dryrun)


def test_unsupported_types_and_nullable_booleans(monkeypatch):
    types = {
        "pubMedId": "INTEGER",
        "title": "STRING",
        "abstract": "MEDIUMTEXT",
        "pubDate": "DATE",
        "entity": "ENTITYID",
        "flag": "BOOLEAN",
    }
    annots = pd.DataFrame(
        {
            "pubMedId": [1, 2],
            "title": ["a", "b"],
            "abstract": ["x", None],
            "pubDate": ["2020-01-01", None],
            "entity": ["syn1", None],
            "flag": [True, None],
        }
    )
    updated = pd.DataFrame({"pubMedId": [1], "title": ["new"], "Component": ["x"]})
    out = _edit(monkeypatch, types, annots, updated)
    assert out.loc[0, "title"] == "new"
    # Nulls stay null: not "" and not False.
    assert pd.isna(out.loc[1, "flag"])
    assert bool(out.loc[0, "flag"]) is True
    assert pd.isna(out.loc[1, "pubDate"])
    assert pd.isna(out.loc[1, "entity"])
