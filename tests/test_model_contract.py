import csv
from pathlib import Path

import pandas as pd
import pytest

import model_contract as mc

FIXTURES = Path(__file__).parent / "fixtures"
TEMPLATES = sorted((FIXTURES / "templates").glob("*.csv"))


def _header(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def test_model_ref_pinned():
    assert mc.MODEL_REF == "v14.0.0"
    assert mc.RAW.endswith("/v14.0.0")


def test_fixture_set_is_all_33_templates():
    assert len(TEMPLATES) == 33


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: p.stem)
def test_round_trip_display_keys_display(path):
    header = _header(path)
    df = pd.DataFrame(columns=header)
    keys = mc.normalize_headers(df, to="keys")
    assert all(" " not in c for c in keys.columns)
    back = mc.normalize_headers(keys, to="display")
    assert list(back.columns) == header


def test_unknown_headers_raise():
    df = pd.DataFrame(columns=["Component", "Not A Real Header"])
    with pytest.raises(ValueError, match="Not A Real Header"):
        mc.normalize_headers(df, to="keys")
    with pytest.raises(ValueError):
        mc.normalize_headers(pd.DataFrame(columns=["NotALabel"]), to="display")


def test_passthrough_columns():
    df = pd.DataFrame(columns=["Component", "Id", "entityId", "Portal Extra", "Grant Name"])
    out = mc.normalize_headers(df, to="keys", passthrough=["Portal Extra"])
    assert list(out.columns) == ["Component", "Id", "entityId", "Portal Extra", "GrantName"]


def test_dataset_view_id():
    assert mc.label_map()["DatasetView_id"] == "DatasetViewId"
    assert mc.label_map()["Dataset Data Use Codes"] == "DatasetDataUseCodes"
    out = mc.normalize_headers(pd.DataFrame(columns=["DatasetView_id"]), to="keys")
    assert list(out.columns) == ["DatasetViewId"]


def test_duo_map():
    duo = mc.duo_id_to_code()
    assert duo["DUO:0000042"] == "GRU"
    assert "Pending Annotation" not in duo


def test_template_headers_ordered():
    assert mc.template_headers("DatasetView") == _header(FIXTURES / "templates/DatasetView.csv")
    assert mc.template_headers("DatasetView")[:2] == ["Component", "DatasetView_id"]


@pytest.mark.parametrize("component", ["ProjectView", "Consortium", "Institution", "Theme"])
def test_template_headers_fallback_to_schema(component):
    assert not (FIXTURES / "templates" / f"{component}.csv").exists()
    headers = mc.template_headers(component)
    assert headers[0] == "Component"
    assert len(headers) > 1
    # every fallback header must convert cleanly
    mc.normalize_headers(pd.DataFrame(columns=headers), to="keys")


def test_project_view_fallback_contents():
    headers = mc.template_headers("ProjectView")
    assert "Investigator" in headers and "Consortium Key" in headers


def test_ambiguous_display_name_raises(tmp_path):
    import json

    graph = {
        "@graph": [
            {"rdfs:label": "A", "sms:displayName": "Same"},
            {"rdfs:label": "B", "sms:displayName": "Same"},
            {
                "rdfs:label": "T",
                "sms:requiresDependency": [{"@id": "bts:A"}, {"@id": "bts:B"}],
            },
        ]
    }
    p = tmp_path / "m.jsonld"
    p.write_text(json.dumps(graph))
    with pytest.raises(ValueError, match="more than one label"):
        mc._load_model(p)


def test_enum_value_name_is_not_a_header():
    # "Breast" is a valid-value node (never a dependency), not an attribute.
    assert "Breast" not in mc.label_map()
    with pytest.raises(ValueError, match="Breast"):
        mc.normalize_headers(pd.DataFrame(columns=["Component", "Breast"]), to="keys")
    with pytest.raises(ValueError, match="Breast"):
        mc.normalize_headers(pd.DataFrame(columns=["Breast"]), to="display")


def test_duplicate_display_name_among_values_is_not_ambiguous(tmp_path):
    import json

    graph = {
        "@graph": [
            {"rdfs:label": "A", "sms:displayName": "Same"},
            {"rdfs:label": "B", "sms:displayName": "Same"},
            {"rdfs:label": "T", "sms:requiresDependency": [{"@id": "bts:A"}]},
        ]
    }
    p = tmp_path / "m.jsonld"
    p.write_text(json.dumps(graph))
    assert mc._load_model(p)[0] == {"Same": "A"}


@pytest.mark.parametrize("component", sorted(mc.LEGACY_ADMIN_HEADERS))
def test_legacy_admin_headers_targets_are_14_headers(component):
    allowed = set(mc.template_headers(component))
    for old, new in mc.LEGACY_ADMIN_HEADERS[component].items():
        assert new in allowed, (old, new)


def test_legacy_person_manifest_normalizes():
    live = list(mc.LEGACY_ADMIN_HEADERS["PersonView"])
    with pytest.raises(ValueError):
        mc.normalize_headers(pd.DataFrame(columns=live), to="keys")
    out = mc.normalize_headers(
        pd.DataFrame(columns=live), to="keys", legacy_component="PersonView"
    )
    assert "GrantViewKey" in out.columns and "ConsortiumKey" in out.columns


def test_legacy_project_manifest_normalizes():
    live = ["Component", "ProjectView_id", "Project Name", "Project Type",
            "Project Description", "Project Investigator", "Project Consortium Name",
            "Project Grant Number", "Id", "entityId"]
    out = mc.normalize_headers(
        pd.DataFrame(columns=live), to="keys", legacy_component="ProjectView"
    )
    assert list(out.columns) == ["Component", "ProjectViewId", "ProjectName", "ProjectType",
                                 "ProjectDescription", "Investigator", "ConsortiumKey",
                                 "GrantViewKey", "Id", "entityId"]


def test_renames_13_to_14_targets_exist():
    for component, renames in mc.RENAMES_13_TO_14.items():
        allowed = set(mc.template_headers(component))
        assert set(renames.values()) <= allowed, component


class _FakeSyn:
    def __init__(self, concrete):
        self.concrete = concrete
        self.calls = []

    def restGET(self, uri):
        self.calls.append(uri)
        return {"id": "syn1", "concreteType": self.concrete}


def _csv(tmp_path):
    path = tmp_path / "m.csv"
    path.write_text("Component,Year,Name\nGrantView,2020,A\nGrantView,,B\n")
    return str(path)


@pytest.mark.parametrize(
    "concrete",
    [
        "org.sagebionetworks.repo.model.table.TableEntity",
        "org.sagebionetworks.repo.model.table.EntityView",
    ],
)
def test_read_manifest_table(monkeypatch, concrete):
    seen = {}
    frame = pd.DataFrame({"Name": ["A"]})

    class FakeTable:
        def __init__(self, id):
            seen["id"] = id

        def query(self, **kwargs):
            seen.update(kwargs)
            return frame

    monkeypatch.setattr(mc, "Table", FakeTable)
    syn = _FakeSyn(concrete)
    out = mc.read_manifest(syn, "syn1")
    assert out is frame
    assert seen["id"] == "syn1"
    assert seen["query"] == "SELECT * FROM syn1"
    assert seen["include_row_id_and_row_version"] is False
    assert seen["synapse_client"] is syn
    assert syn.calls == ["/entity/syn1"]


@pytest.mark.parametrize(
    "concrete,attr",
    [
        ("org.sagebionetworks.repo.model.RecordSet", "RecordSet"),
        ("org.sagebionetworks.repo.model.FileEntity", "File"),
    ],
)
def test_read_manifest_csv_entities(monkeypatch, tmp_path, concrete, attr):
    path = _csv(tmp_path)
    seen = {}

    class FakeEntity:
        def __init__(self, id):
            seen["id"] = id

        def get(self, **kwargs):
            seen["get"] = kwargs
            self.path = path
            return self

    class NoTable:
        def __init__(self, *a, **k):
            raise AssertionError("a CSV entity must not be queried")

    monkeypatch.setattr(mc, attr, FakeEntity)
    monkeypatch.setattr(mc, "Table", NoTable)
    syn = _FakeSyn(concrete)
    out = mc.read_manifest(syn, "syn1")
    assert seen["id"] == "syn1" and seen["get"] == {"synapse_client": syn}
    assert list(out.columns) == ["Component", "Year", "Name"]
    assert out["Year"].tolist()[0] == "2020" and pd.isna(out["Year"].tolist()[1])
    assert all(dt == object or str(dt) == "str" for dt in out.dtypes)


def test_read_manifest_unsupported_type():
    syn = _FakeSyn("org.sagebionetworks.repo.model.Folder")
    with pytest.raises(ValueError, match="unsupported entity type"):
        mc.read_manifest(syn, "syn1")


def test_fetch_ref_uses_a_per_ref_cache_and_url(tmp_path, monkeypatch):
    monkeypatch.delenv("MC2_MODEL_DIR", raising=False)
    monkeypatch.setattr(mc, "CACHE_DIR", tmp_path / mc.MODEL_REF)
    urls = []

    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"{}"

    monkeypatch.setattr(mc.urllib.request, "urlopen", lambda url, timeout: (urls.append(url), Resp())[1])
    path = mc.fetch("json_schemas/X.json", ref="v14.0.1")
    assert path == tmp_path / "v14.0.1" / "json_schemas" / "X.json"
    assert urls == ["https://raw.githubusercontent.com/mc2-center/data-models/v14.0.1/json_schemas/X.json"]
    mc.fetch("json_schemas/X.json", ref="v14.0.1")  # cached
    assert len(urls) == 1
