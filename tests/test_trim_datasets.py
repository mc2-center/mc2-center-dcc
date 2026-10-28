from types import SimpleNamespace

from synapseclient import operations

from conftest import load_script

td = load_script("utils/trim_datasets.py", "td_mod")


class StubDataset:
    def __init__(self, id, items):
        self.id = id
        self.items = items
        self.removed = []
        self.stored = False

    def remove_item(self, ref, synapse_client=None):
        self.removed.append(ref.id)
        self.items = [i for i in self.items if i.id != ref.id]

    def store(self, synapse_client=None):
        self.stored = True


def _patch(monkeypatch, datasets):
    monkeypatch.setattr(
        operations, "get", lambda ident, **kw: datasets[ident]
    )


def test_empty_dataset_does_not_raise(monkeypatch):
    empty = StubDataset("syn1", [])
    _patch(monkeypatch, {"syn1": empty})
    dataset_id, removed = td.remove_files_from_dataset(None, "syn1", ["syn9"])
    assert (dataset_id, removed) == ("syn1", [])


def test_removes_only_listed_files(monkeypatch):
    items = [SimpleNamespace(id="syn5", version=1), SimpleNamespace(id="syn6", version=1)]
    ds = StubDataset("syn2", items)
    _patch(monkeypatch, {"syn2": ds})
    _, removed = td.remove_files_from_dataset(None, "syn2", ["syn5", "syn9"])
    assert removed == ["syn5"]
    assert ds.removed == ["syn5"]
    assert ds.stored
