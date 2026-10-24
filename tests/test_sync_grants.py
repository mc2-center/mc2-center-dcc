from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import create_grant_projects as cgp
import crosswalk as cw
import sync_grants
import utils

CONSORTIA = ["CSBC", "PS-ON", "TEC", "ICBP"]
V14 = [
    "Component", "GrantView_id", "Grant Name", "Grant Number", "Grant Abstract",
    "Grant Type", "Grant Theme Name", "Grant Institution Name",
    "Grant Institution Alias", "Investigator", "Consortium Key",
    "Grant Start Date", "Grant End Date", "NIH RePORTER Link",
    "Duration of Funding", "Embargo End Date", "Grant Synapse Team",
    "Grant Synapse Project",
]
LEGACY_RENAMES = {"Investigator": "Grant Investigator", "Consortium Key": "Grant Consortium Name"}


def _manifest(legacy):
    rows = [
        ["GrantView", "OLD1", "Old grant", "OLD1", "abs", "U01", "Metastasis",
         "Inst A", "IA", "A. Person", "ICBP", "6/1/14", "5/31/20", "http://r",
         5, None, "t", "p"],
        ["GrantView", "NEW1", "New grant", "NEW1", "abs2", "U54", "Heterogeneity, Metastasis",
         "Inst B, Inst C", "IB, IC", "B. Person, C. Person", "CSBC, PS-ON", "1/1/24",
         "12/31/28", "http://r2", 5, None, None, None],
    ]
    df = pd.DataFrame(rows, columns=V14)
    if legacy:
        df = df.rename(columns=LEGACY_RENAMES)
        df["Grant Consortium Name"] = ["ICBP", "CSBC, PS-ON"]
    else:
        df["Consortium Key"] = ["program.icbp", "program.csbc, program.ps-on"]
    df["Id"] = ["a", "b"]
    df["entityId"] = "syn53259587"
    return df


def _portal():
    return pd.DataFrame(
        [["syn1", "OLD1", "Old grant", "OLD1", "abs", "U01", "['Metastasis']",
          "['IA']", "['Inst A']", "A. Person", "['ICBP']", "6/1/14", "http://r",
          "5", None, "https://www.synapse.org/#!Team:1", "https://www.synapse.org/#!Synapse:syn1"]],
        columns=[c for _, c, _ in cw.CROSSWALK["GrantView"]],
    )


def _live_types():
    types = {c: "STRING" for _, c, _ in cw.CROSSWALK["GrantView"]}
    for _, c, kind in cw.CROSSWALK["GrantView"]:
        if kind == "list" and c != "investigator":
            types[c] = "STRING_LIST"
    return types


def _table_factory(manifest):
    consortium = pd.DataFrame({"consortiumDisplayName": CONSORTIA})
    frames = {"syn53259587": manifest, "syn21918972": _portal(), "syn36345930": consortium}

    def table(id):
        m = MagicMock()
        m.query.side_effect = lambda **kw: frames[id].copy()
        return m

    return table


def _run(manifest, dryrun=True):
    factory = _table_factory(manifest)
    with patch.object(cgp, "Table", factory), patch.object(utils, "Table", factory), \
            patch.object(cgp, "create_grant_projects") as create:
        create.return_value = {"NEW1": ("syn99", "123")}
        staged = cgp.process_new_grants("syn53259587", "syn21918972", dryrun, MagicMock())
    return cw.to_portal(staged, "GrantView", _live_types()), create


@pytest.mark.parametrize("legacy", [False, True])
def test_new_grant_row(legacy):
    out, _ = _run(_manifest(legacy))
    assert list(out.columns) == [c for _, c, _ in cw.CROSSWALK["GrantView"]]
    assert len(out) == 2
    new = out[out.grantViewId == "NEW1"].iloc[0]
    assert new.consortium == ["CSBC", "PS-ON"]
    assert new.investigator == "B. Person, C. Person"
    assert new.theme == ["Heterogeneity", "Metastasis"]
    assert new.grantId == "None"  # dryrun: nothing created
    old = out[out.grantViewId == "OLD1"].iloc[0]
    assert old.consortium == ["ICBP"] and old.theme == ["Metastasis"]


def test_legacy_and_current_headers_give_identical_output():
    a, _ = _run(_manifest(legacy=False))
    b, _ = _run(_manifest(legacy=True))
    pd.testing.assert_frame_equal(a, b)


def test_real_run_uses_created_project_ids():
    out, create = _run(_manifest(False), dryrun=False)
    new = out[out.grantViewId == "NEW1"].iloc[0]
    assert new.grantId == "syn99"
    assert new.grantSynapseTeam.endswith("Team:123")
    # project creation sees key-named columns and resolved consortium names
    grants = create.call_args.args[1]
    assert grants.iloc[0]["consortium"] == "CSBC, PS-ON"
    assert grants.iloc[0]["GrantInstitutionAlias"] == "IB, IC"


@pytest.mark.parametrize("value", ["Not A Consortium", "program.nope"])
def test_unknown_consortium_is_fatal(value):
    manifest = _manifest(False)
    manifest.loc[1, "Consortium Key"] = f"program.csbc, {value}"
    with pytest.raises(ValueError, match="Unknown"):
        _run(manifest)


def test_unknown_consortium_on_existing_grant_is_not_fatal():
    manifest = _manifest(False)
    manifest.loc[0, "Consortium Key"] = "Sage Bionetworks"
    out, _ = _run(manifest)
    assert out[out.grantViewId == "OLD1"].iloc[0].consortium == ["ICBP"]


def test_resolve_consortium_both_forms():
    factory = _table_factory(_manifest(False))
    with patch.object(cgp, "Table", factory), patch.object(utils, "Table", factory):
        out = cgp.resolve_consortium(
            MagicMock(), pd.Series(["CSBC", "program.ps-on", "program.tec, ICBP", ""])
        )
    assert list(out) == ["CSBC", "PS-ON", "TEC, ICBP", ""]


def test_update_scopes_checks_each_view_against_full_list():
    views = {
        "v1": MagicMock(scope_ids={"syn1", "syn2"}),
        "v2": MagicMock(scope_ids={"syn1"}),
    }
    with patch.object(sync_grants, "EntityView", lambda id: MagicMock(get=lambda **k: views[id])):
        sync_grants.update_scopes(MagicMock(), ["v1", "v2"], ["syn1", "syn2", "None", ""])
    views["v1"].store.assert_not_called()
    assert views["v2"].scope_ids == {"syn1", "syn2"}
    views["v2"].store.assert_called_once()
