from synapseclient import operations

from conftest import load_script

tta = load_script("utils/table_to_annotations.py", "tta_mod")


class StubSyn:
    def __init__(self):
        self.annotations = {"existing": "keep"}
        self.set_calls = []

    def get_annotations(self, entity_id):
        return self.annotations

    def set_annotations(self, annots):
        self.set_calls.append(dict(annots))


def test_apply_annotations_is_annotations_only(monkeypatch):
    """No full entity get/store (RecordSets come back as dicts, store() is a PUT)."""

    def boom(*a, **k):
        raise AssertionError("full-entity operations.get must not be used")

    monkeypatch.setattr(operations, "get", boom)
    syn = StubSyn()
    tta.apply_annotations_to_entity(
        syn,
        "Study",
        "syn1",
        [("Study Key", "k"), ("Dataset Name", "x"), ("Empty Col", " , ")],
        keys_to_drop=["Study Key"],
    )
    assert syn.set_calls == [{"existing": "keep", "DatasetName": "x"}]
