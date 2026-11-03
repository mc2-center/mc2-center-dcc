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


# Live GrantView column types (syn21918972), from crosswalk.check_crosswalk.
LIVE_TYPES = {
    "grantId": "ENTITYID", "grantViewId": "STRING", "grantName": "STRING",
    "grantNumber": "STRING", "abstract": "LARGETEXT", "grantType": "STRING",
    "theme": "STRING_LIST", "institutionAlias": "STRING_LIST",
    "grantInstitution": "STRING_LIST", "investigator": "STRING",
    "consortium": "STRING_LIST", "grantStartDate": "STRING",
    "nihReporterLink": "LINK", "durationOfFunding": "INTEGER",
    "embargoEndDate": "STRING", "grantSynapseTeam": "LINK",
    "grantSynapseProject": "LINK",
}


def _live_types():
    return dict(LIVE_TYPES)


def _table_factory(manifest):
    consortium = pd.DataFrame({"consortiumDisplayName": CONSORTIA})
    frames = {"syn53259587": manifest, "syn21918972": _portal(), "syn36345930": consortium}

    def table(id):
        m = MagicMock()
        m.query.side_effect = lambda **kw: frames[id].copy()
        return m

    return table


def _run(manifest, dryrun=True, create_result=None, live_types=None):
    factory = _table_factory(manifest)
    with patch.object(cgp, "Table", factory), patch.object(utils, "Table", factory), \
            patch.object(cgp.model_contract, "read_manifest",
                         lambda syn, entity_id, component=None: manifest.copy()), \
            patch.object(cgp, "create_grant_projects") as create:
        create.return_value = create_result or {"NEW1": ("syn99", "123")}
        staged = cgp.process_new_grants(
            "syn53259587", "syn21918972", dryrun, MagicMock(), live_types=live_types
        )
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


def test_live_types_match_crosswalk_columns_and_integer_duration():
    assert list(LIVE_TYPES) == [c for _, c, _ in cw.CROSSWALK["GrantView"]]
    assert LIVE_TYPES["durationOfFunding"] == "INTEGER"


def test_duration_is_converted_to_integer():
    out, _ = _run(_manifest(False))
    assert str(out.durationOfFunding.dtype) == "Int64"


def test_conversion_error_creates_nothing():
    manifest = _manifest(False)
    manifest["Duration of Funding"] = manifest["Duration of Funding"].astype(object)
    manifest.loc[1, "Duration of Funding"] = "60 months"
    factory = _table_factory(manifest)
    with patch.object(cgp, "Table", factory), patch.object(utils, "Table", factory), \
            patch.object(cgp.model_contract, "read_manifest", lambda syn, entity_id, component=None: manifest.copy()), \
            patch.object(cgp, "create_grant_projects") as create:
        with pytest.raises(ValueError, match="60 months"):
            cgp.process_new_grants("syn53259587", "syn21918972", False, MagicMock(),
                                   live_types=_live_types())
    create.assert_not_called()


def test_failed_grant_is_left_out_and_reported():
    factory = _table_factory(_manifest(False))
    manifest = _manifest(False)
    with patch.object(cgp, "Table", factory), patch.object(utils, "Table", factory), \
            patch.object(cgp.model_contract, "read_manifest", lambda syn, entity_id, component=None: manifest.copy()), \
            patch.object(cgp, "create_grant_projects") as create:
        create.return_value = {"NEW1": ("None", "None")}
        staged = cgp.process_new_grants("syn53259587", "syn21918972", False, MagicMock(),
                                        live_types=_live_types())
    assert list(staged.GrantViewId) == ["OLD1"]
    assert staged.attrs["failed_grants"] == ["NEW1"]
    assert "None" not in staged.grantId.to_list()


def test_check_grant_ids_refuses_none_and_empty():
    good = pd.DataFrame({"grantId": ["syn1"], "grantViewId": ["A"]})
    sync_grants.check_grant_ids(good)
    for bad in ["None", "", None]:
        with pytest.raises(ValueError, match="without a project"):
            sync_grants.check_grant_ids(pd.DataFrame({"grantId": [bad], "grantViewId": ["A"]}))


def _grant_row(**over):
    row = {
        "GrantViewId": "NEW1", "GrantName": "New grant", "GrantNumber": "N1",
        "consortium": "CSBC", "GrantType": "U54", "GrantInstitutionAlias": "IB",
        "GrantInstitutionName": "Inst B", "GrantAbstract": "abs", "Investigator": "A, B",
    }
    row.update(over)
    return pd.DataFrame([row])


def _syn_mock(wiki_exists=False):
    syn = MagicMock()
    syn.store.side_effect = lambda e: MagicMock(id="syn7" if isinstance(e, cgp.Project) else "55")
    if wiki_exists:
        syn.getWiki.return_value = MagicMock()
    else:
        resp = MagicMock(status_code=404)
        syn.getWiki.side_effect = cgp.synapseclient.core.exceptions.SynapseHTTPError(
            "none", response=resp)
    return syn


def test_existing_project_and_team_are_reused():
    syn = _syn_mock(wiki_exists=True)
    with patch.object(cgp, "find_entity_id", return_value="syn123") as find, \
            patch.object(cgp, "find_team_id", return_value="999"):
        out = cgp.create_grant_projects(syn, _grant_row())
    assert out == {"NEW1": ("syn123", "999")}
    find.assert_called_once_with(name="New grant", parent=None, synapse_client=syn)
    stored = [type(c.args[0]).__name__ for c in syn.store.call_args_list]
    assert "Project" not in stored and "Team" not in stored and "Wiki" not in stored


def test_new_project_is_created_when_not_found():
    syn = _syn_mock()
    with patch.object(cgp, "find_entity_id", return_value=None), \
            patch.object(cgp, "find_team_id", return_value=None):
        out = cgp.create_grant_projects(syn, _grant_row())
    assert out == {"NEW1": ("syn7", "55")}


def test_creation_failure_maps_to_none_pair():
    syn = _syn_mock()
    err = cgp.synapseclient.core.exceptions.SynapseHTTPError("boom", response=MagicMock(status_code=500))
    with patch.object(cgp, "find_entity_id", side_effect=err):
        out = cgp.create_grant_projects(syn, _grant_row())
    assert out == {"NEW1": ("None", "None")}


def test_nan_abstract_and_none_investigator_are_safe():
    syn = _syn_mock()
    row = _grant_row(GrantAbstract=float("nan"), Investigator=None).iloc[0]
    cgp.create_wiki_pages(syn, "syn7", row)
    wikis = [c.args[0] for c in syn.store.call_args_list]
    assert all("nan" not in w.markdown for w in wikis)
    assert len(wikis) == 1  # no investigators: no sub-wiki


def test_wiki_heading_format_and_investigators():
    syn = _syn_mock()
    cgp.create_wiki_pages(syn, "syn7", _grant_row().iloc[0])
    main, pis = [c.args[0] for c in syn.store.call_args_list]
    assert main.markdown.startswith("### The CSBC U54 Research Project \\@ IB\n")
    assert pis.markdown == "* A\n* B"


def test_main_writes_successes_and_exits_nonzero_on_failure():
    staged = pd.DataFrame(
        {"grantId": ["syn1"]}  # placeholder; to_portal is patched below
    )
    staged.attrs["failed_grants"] = ["NEW1"]
    final = pd.DataFrame({"grantId": ["syn1"], "grantViewId": ["OLD1"]})
    args = MagicMock(dryrun=False, verbose=False, noprint=True, manifest_id="m",
                     portal_table_id="t", output_csv="x")
    with patch.object(sync_grants.utils, "syn_login"), \
            patch.object(sync_grants.utils, "get_args", return_value=args), \
            patch.object(sync_grants.crosswalk, "check_crosswalk", return_value={}), \
            patch.object(sync_grants, "check_portal_grant_ids"), \
            patch.object(sync_grants, "process_new_grants", return_value=staged), \
            patch.object(sync_grants.crosswalk, "to_portal", return_value=final), \
            patch.object(sync_grants, "update_scopes"), \
            patch.object(sync_grants.utils, "update_table") as update:
        assert sync_grants.main() == 1
    update.assert_called_once()
    assert update.call_args.args[2] is final


@pytest.mark.parametrize("dryrun", [False, True])
def test_main_checks_live_grant_ids_before_creating_anything(dryrun):
    live = pd.DataFrame({"grantId": ["syn1", "None"], "grantViewId": ["OLD1", "BAD1"]})
    args = MagicMock(dryrun=dryrun, verbose=False, noprint=True, manifest_id="m",
                     portal_table_id="t", output_csv="x")
    table = MagicMock()
    table.return_value.query.return_value = live
    with patch.object(sync_grants.utils, "syn_login"), \
            patch.object(sync_grants.utils, "get_args", return_value=args), \
            patch.object(sync_grants.crosswalk, "check_crosswalk", return_value={}), \
            patch.object(sync_grants, "Table", table), \
            patch.object(sync_grants, "process_new_grants") as process:
        with pytest.raises(ValueError, match="BAD1"):
            sync_grants.main()
    process.assert_not_called()
    assert "FROM t" in table.return_value.query.call_args.kwargs["query"]
