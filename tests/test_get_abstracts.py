import os

import pandas as pd

from conftest import ROOT, load_script

ga = load_script("utils/get_abstracts.py", "ga_mod")
portal_utils = load_script("portal_tables/utils.py", "portal_utils_mod")


class StubTable:
    stored = []

    def __init__(self, id=None):
        self.id = id

    def store_rows(self, values, synapse_client=None):
        StubTable.stored.append(values)


def test_uses_shared_strip_newlines(monkeypatch):
    StubTable.stored = []
    monkeypatch.setattr(ga, "Table", StubTable)
    df = pd.DataFrame({"abstract": ["a\nb\r\nc"], "year": [2020]})
    ga.store_edited_publications(None, "syn1", df)
    assert StubTable.stored[0]["abstract"].tolist() == ["a b c"]
    # The helper is the portal_tables one, not a private copy of its regex.
    assert os.path.realpath(ga.strip_newlines.__code__.co_filename) == os.path.realpath(
        os.path.join(ROOT, "portal_tables", "utils.py")
    )
