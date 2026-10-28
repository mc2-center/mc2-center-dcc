"""utils/table_to_annotations.py and utils/merge_and_correct_manifests.py, offline."""
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import model_contract

ROOT = Path(__file__).resolve().parent.parent


def _load(relpath, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeEntity:
    """The entity's annotations, read and written through a fake Synapse client's annotations-only calls."""

    def __init__(self):
        self.annotations = {}
        self.stored = False
        self.syn = SimpleNamespace(get_annotations=lambda entity_id: dict(self.annotations),
                                   set_annotations=self._set)

    def _set(self, annotations):
        self.annotations = dict(annotations)
        self.stored = True


def _dataset_df(headers):
    row = {h: "" for h in headers}
    row["DatasetView_id" if "DatasetView_id" in row else headers[0]] = "syn1"
    return pd.DataFrame([row])


def _patch(tta, monkeypatch, df):
    entity = FakeEntity()

    class FakeTable:
        queries = []

        def __init__(self, id):
            pass

        def query(self, query, **kw):
            FakeTable.queries.append(query)
            return df.copy()

    monkeypatch.setattr(tta, "Table", FakeTable)
    # whole-table reads go through model_contract.read_manifest
    def fake_read_manifest(syn, entity_id):
        FakeTable.queries.append(f"SELECT * FROM {entity_id}")
        return df.copy()

    monkeypatch.setattr(tta.model_contract, "read_manifest", fake_read_manifest)
    return entity, FakeTable


def test_keys_to_drop_empty_list_does_not_raise(monkeypatch):
    # main() used to pass keys_to_drop=None here, so `key not in None` raised.
    tta = _load("utils/table_to_annotations.py", "tta")
    entity, _ = _patch(tta, monkeypatch, pd.DataFrame())
    tta.apply_annotations_to_entity(entity.syn, "DatasetView", "syn1", [("Dataset Name", "n")], [])
    assert entity.annotations == {"DatasetName": "n"} and entity.stored


def test_dataset_columns_come_from_template():
    tta = _load("utils/table_to_annotations.py", "tta")
    src = (ROOT / "utils" / "table_to_annotations.py").read_text()
    assert "template_headers(\"DatasetView\")" in src
    headers = [h for h in model_contract.template_headers("DatasetView") if h != "Component"]
    assert headers[0] == "DatasetView_id" and "Dataset Data Use Codes" in headers
    assert hasattr(tta, "get_table")


def _run_dataset(monkeypatch, source_headers):
    tta = _load("utils/table_to_annotations.py", "tta")
    df = _dataset_df(source_headers)
    df.loc[0, [c for c in source_headers if c.endswith("Assay")][0]] = "scRNA-seq"
    entity, fake = _patch(tta, monkeypatch, df)
    cols = [h for h in model_contract.template_headers("DatasetView") if h != "Component"]
    tta.collect_database_annotations(entity.syn, "syn1", ("DatasetView", "syn9", cols), [])
    return entity, fake


def test_dataset_annotations_from_14_table(monkeypatch):
    headers = model_contract.template_headers("DatasetView")
    entity, fake = _run_dataset(monkeypatch, headers)
    assert entity.annotations["Assay"] == "scRNA-seq" and entity.stored
    assert fake.queries[-1].startswith("SELECT * FROM")


def test_dataset_annotations_from_legacy_table(monkeypatch):
    legacy = {"Assay": "Dataset Assay", "Species": "Dataset Species",
              "Tumor Type": "Dataset Tumor Type", "Tissue": "Dataset Tissue",
              "Dataset Data Use Codes": "Data Use Codes"}
    headers = [legacy.get(h, h) for h in model_contract.template_headers("DatasetView")]
    entity, _ = _run_dataset(monkeypatch, headers)
    assert entity.annotations["Assay"] == "scRNA-seq"


def test_merge_and_correct_accepts_legacy_and_new_headers(tmp_path, monkeypatch):
    mc = _load("utils/merge_and_correct_manifests.py", "mc")
    legacy = pd.DataFrame([{
        "Component": "PublicationView", "Pubmed Id": "1", "Publication Doi": "10.1/x",
        "Publication Assay": "", "Publication Tumor Type": " a ", "Publication Tissue": "t",
        "Publication Dataset Alias": "", "Publication Accessibility": "Open Access",
        "Publication Grant Number": "CA1",
    }])
    db = tmp_path / "PublicationView_x.csv"
    legacy.to_csv(db, index=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["x", "-d", str(db)])
    mc.main()
    out = pd.read_csv(tmp_path / "PublicationView_merged_corrected.csv", dtype=str, keep_default_na=False)
    assert {"Assay", "Tumor Type", "Tissue", "GrantView Key"} <= set(out.columns)
    assert out.loc[0, "Assay"] == "Not Applicable"
    assert out.loc[0, "Tumor Type"] == "a"
    assert out.loc[0, "Publication Doi"] == "https://doi.org/10.1/x"
