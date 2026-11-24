import pandas as pd

from conftest import load_script
from fakes import make_stub_table

upa = load_script("annotations/update_pending_annotations.py", "upa_mod")


def _edit(monkeypatch, types, annots, updated, dryrun=False):
    monkeypatch.setattr(upa, "Table", make_stub_table(column_types=types))
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
