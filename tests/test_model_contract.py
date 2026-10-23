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
        ]
    }
    p = tmp_path / "m.jsonld"
    p.write_text(json.dumps(graph))
    with pytest.raises(ValueError, match="more than one label"):
        mc._load_model(p)


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
