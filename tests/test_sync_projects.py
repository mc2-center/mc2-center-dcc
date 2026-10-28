import pandas as pd
import pytest

import crosswalk as cw
import sync_projects as sp
import utils

KNOWN = {"CSBC", "PS-ON", "MetNet", "CCBIR"}
IDS = {"program.csbc": "CSBC", "program.ps-on": "PS-ON"}

GRANTS = pd.DataFrame(
    {
        "grantNumber": ["CA1", "CA2"],
        "grantName": ["Grant One", "Grant Two"],
        "theme": [["Heterogeneity", "Microenvironment"], ["Microenvironment", "Tumor-Immune"]],
        "consortium": [["CSBC"], ["PS-ON"]],
        "grantType": ["U54", "U01"],
    }
)

LIVE = {c: "STRING" for c in cw.expected_columns("ProjectView", [])}
LIVE.update({c: "STRING_LIST" for c in ("theme", "consortium", "grant", "grantName", "grantType")})

ROWS = [
    # name, type, description, investigator, consortium, grants
    ("Core A", "Core", "Not available", "Jane Doe", "csbc", "CA1"),
    ("Proj B", "Project", "Text", "John Roe", "ps-on", "CA1,CA2"),
    ("Aff C", "Project", "Text", "Ann Poe", "csbc", "Affiliated/Non-Grant Associated"),
]


@pytest.fixture(autouse=True)
def fake_lookup(monkeypatch):
    def lookup(syn, ids):
        unknown = [i for i in ids if i not in IDS]
        if unknown:
            raise ValueError(f"Unknown Consortium ID(s) {unknown}")
        return [IDS[i] for i in ids]

    monkeypatch.setattr(utils, "consortium_display_names", lookup)


def _manifest(legacy: bool, consortium_form: str) -> pd.DataFrame:
    def cons(slug):
        return f"program.{slug}" if consortium_form == "id" else IDS[f"program.{slug}"]

    if legacy:
        cols = ["Component", "ProjectView_id", "Project Name", "Project Type",
                "Project Description", "Project Investigator",
                "Project Consortium Name", "Project Grant Number", "Id", "entityId"]
    else:
        cols = ["Component", "ProjectView_id", "Project Name", "Project Type",
                "Project Description", "Investigator", "Consortium Key",
                "GrantView Key", "Id", "entityId"]
    return pd.DataFrame(
        [["ProjectView", f"P{i}", n, t, d, inv, cons(c), g, "x", "syn1"]
         for i, (n, t, d, inv, c, g) in enumerate(ROWS)],
        columns=cols,
    )


def _run(manifest, known=KNOWN):
    m = sp.prepare_manifest(manifest)
    consortia = sp.resolve_consortium_keys(None, m["ConsortiumKey"], known_names=known)
    return sp.clean_table(sp.add_missing_info(m, GRANTS, consortia), LIVE)


@pytest.mark.parametrize("form", ["name", "id"])
def test_legacy_and_14_headers_give_identical_output(form):
    legacy = _run(_manifest(True, form))
    new = _run(_manifest(False, form))
    pd.testing.assert_frame_equal(legacy, new)
    assert list(new.columns) == cw.expected_columns("ProjectView", [])
    assert "projectShortName" not in new.columns


def test_display_names_and_ids_resolve_identically():
    pd.testing.assert_frame_equal(
        _run(_manifest(False, "name")), _run(_manifest(False, "id"))
    )


def test_values():
    out = _run(_manifest(True, "id"))
    assert out["investigator"].tolist() == ["Jane Doe", "John Roe", "Ann Poe"]
    assert out["consortium"].tolist() == [["CSBC"], ["PS-ON"], ["CSBC"]]
    assert out["grant"][1] == ["CA1", "CA2"]
    assert out["grantName"][1] == ["Grant One", "Grant Two"]
    assert out["theme"][1] == ["Heterogeneity", "Microenvironment", "Tumor-Immune"]
    assert out["grantName"][2] == []


def test_short_name_mapped_when_live_column_exists():
    live = dict(LIVE, projectShortName="STRING")
    m = _manifest(False, "name")
    m["Project Short Name"] = ["a", "b", "c"]
    prepared = sp.prepare_manifest(m)
    consortia = sp.resolve_consortium_keys(None, prepared["ConsortiumKey"], known_names=KNOWN)
    out = sp.clean_table(sp.add_missing_info(prepared, GRANTS, consortia), live)
    assert out["projectShortName"].tolist() == ["a", "b", "c"]
    assert out.columns[-1] == "projectShortName"


def test_unknown_values_are_fatal_and_all_reported():
    with pytest.raises(ValueError) as e:
        sp.resolve_consortium_keys(
            None, ["CSBC", "Bogus", "program.nope", "Other"], known_names=KNOWN
        )
    msg = str(e.value)
    assert all(v in msg for v in ("Bogus", "program.nope", "Other"))


def test_multiple_keys_dedupe_in_order():
    assert sp.resolve_consortium_keys(
        None, ["program.ps-on, CSBC,PS-ON"], known_names=KNOWN
    ) == [["PS-ON", "CSBC"]]
