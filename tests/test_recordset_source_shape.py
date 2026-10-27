"""A RecordSet manifest gives the same portal strings as the table manifest it replaces (per sync)."""
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from synapseclient.core.constants import concrete_types

import create_grant_projects as cgp
import crosswalk as cw
import model_contract as mc
import sync_people as sper
import sync_projects as sproj
import utils

from test_sync_grants import _live_types, _portal, _table_factory


class FakeSyn:
    def restGET(self, uri):
        return {"concreteType": concrete_types.RECORD_SET_ENTITY}


def recordset_reader(monkeypatch, tmp_path, frame):
    """Make model_contract.read_manifest read ``frame`` (class-label headers, JSON-array lists) as a RecordSet."""
    path = tmp_path / "rs.csv"
    out = frame.copy()
    for c in out.columns:
        out[c] = out[c].map(lambda v: json.dumps(v) if isinstance(v, list) else v)
    out.to_csv(path, index=False)

    class FakeRecordSet:
        def __init__(self, id):
            pass

        def get(self, **kw):
            self.path = str(path)
            return self

    monkeypatch.setattr(mc, "RecordSet", FakeRecordSet)


def test_crosswalk_joins_list_cells_for_string_columns_only():
    live = {"investigator": "STRING", "consortium": "STRING_LIST", "projectName": "STRING"}
    df = pd.DataFrame({
        "ProjectName": [["a", "b"]], "ProjectType": ["Core"], "ProjectDescription": ["d"],
        "Investigator": [["X Y", "Z W"]], "theme": [[]], "consortium": [["CSBC"]], "GrantViewKey": [["CA1"]],
        "grantName": [[]], "grantType": [[]], "ProjectShortName": [""],
    })
    out = cw.to_portal(df, "ProjectView", {c: live.get(c, "STRING_LIST") for c in cw.expected_columns("ProjectView", [])}
                       | {"projectName": "STRING", "investigator": "STRING", "projectType": "STRING"})
    assert out["investigator"].tolist() == ["X Y, Z W"]
    assert out["projectName"].tolist() == ["a, b"]
    assert out["consortium"].tolist() == [["CSBC"]] and out["grant"].tolist() == [["CA1"]]  # STRING_LIST: untouched


GRANTS = pd.DataFrame({
    "grantNumber": ["CA1", "CA2"], "grantName": ["Grant One", "Grant Two"],
    "theme": [["Heterogeneity"], ["Microenvironment"]], "consortium": [["CSBC"], ["PS-ON"]], "grantType": ["U54", "U01"]})
KNOWN = {"CSBC", "PS-ON", "TEC"}


def test_project_recordset_and_table_give_the_same_portal_strings(monkeypatch, tmp_path):
    live = {c: "STRING" for c in cw.expected_columns("ProjectView", [])}
    live.update({c: "STRING_LIST" for c in ("theme", "consortium", "grant", "grantName", "grantType")})
    table = pd.DataFrame({
        "Component": "ProjectView", "ProjectView_id": ["p1", "p2"], "Project Name": ["Core A", "Proj B"],
        "Project Type": ["Core", "Project"], "Project Description": ["d", "e"],
        "Project Investigator": ["Jane Doe", "John Roe, Ann Poe"], "Project Consortium Name": ["CSBC", "PS-ON"],
        "Project Grant Number": ["CA1", "CA1, CA2"], "Id": ["a", "b"], "entityId": "syn1"})
    recordset = pd.DataFrame({
        "ProjectViewId": ["p1", "p2"], "ProjectName": ["Core A", "Proj B"], "ProjectType": ["Core", "Project"],
        "ProjectDescription": ["d", "e"], "Investigator": [["Jane Doe"], ["John Roe", "Ann Poe"]],
        "ConsortiumKey": ["program.csbc", "program.ps-on"], "GrantViewKey": ["CA1", "CA1, CA2"]})
    recordset_reader(monkeypatch, tmp_path, recordset)

    def run(manifest, known):
        m = sproj.prepare_manifest(utils.fill_blank(manifest))
        consortia = sproj.resolve_consortium_keys(None, m["ConsortiumKey"], known_names=known)
        return sproj.clean_table(sproj.add_missing_info(m, GRANTS, consortia), live)

    monkeypatch.setattr(utils, "consortium_display_names", lambda syn, ids: [{"program.csbc": "CSBC", "program.ps-on": "PS-ON"}[i] for i in ids])
    from_table = run(table, KNOWN)
    from_recordset = run(mc.read_manifest(FakeSyn(), "synRS", component="ProjectView"), KNOWN)
    assert from_recordset["investigator"].tolist() == ["Jane Doe", "John Roe, Ann Poe"]
    pd.testing.assert_frame_equal(from_recordset, from_table)


def test_people_recordset_and_table_give_the_same_portal_strings(monkeypatch, tmp_path):
    live = {c: "STRING" for c in cw.expected_columns("PersonView", [])}
    live.update({c: "STRING_LIST" for c in ("alternativeNames", "grantName", "grantNumber", "consortium",
                                            "workingGroupParticipation")})
    table = pd.DataFrame({
        "component": "PersonView", "personGrantNumber": [["CA1"], ["CA1", "CA2"]],
        "personConsortiumName": [["CSBC"], ["CSBC", "PS-ON"]], "name": ["Ann", "Bob"],
        "alternativeNames": [[], ["Rob"]], "email": ["a@x.org", "b@x.org"], "url": ["", ""], "orcidId": ["", ""],
        "synapseProfileId": ["123", ""], "lastKnownInstitution": ["Inst", "Inst"],
        "workingGroupParticipation": [["WG1"], ["WG1", "WG2"]],
        "chairRoles": ["Steering Committee, Working Group", ""], "consentForPortalDisplay": ["Yes", "Yes"],
        "portalDisplay": ["TRUE", "TRUE"], "personPublications": [["P1", "P2"], []], "personDatasets": [[], []],
        "personTools": [["T1"], []]})
    recordset = pd.DataFrame({
        "PersonViewId": ["person.ann", "person.bob"], "GrantViewKey": ["CA1", "CA1, CA2"],
        "ConsortiumKey": ["program.csbc", "program.csbc, program.ps-on"], "Name": ["Ann", "Bob"],
        "AlternativeNames": [[], ["Rob"]], "Email": ["a@x.org", "b@x.org"], "Url": ["", ""], "OrcidId": ["", ""],
        "SynapseProfileId": ["123", ""], "LastKnownInstitution": ["Inst", "Inst"],
        "WorkingGroupParticipation": [["WG1"], ["WG1", "WG2"]],
        "ChairRoles": [["Steering Committee", "Working Group"], []], "ConsentForPortalDisplay": ["Yes", "Yes"],
        "PortalDisplay": ["TRUE", "TRUE"], "PersonPublications": [["P1", "P2"], []], "PersonDatasets": [[], []],
        "PersonTools": [["T1"], []]})
    recordset_reader(monkeypatch, tmp_path, recordset)
    monkeypatch.setattr(utils, "consortium_display_names", lambda syn, ids: [{"program.csbc": "CSBC", "program.ps-on": "PS-ON"}[i] for i in ids])

    def run(manifest):
        m = sper.prepare_manifest(utils.fill_blank(manifest))
        consortia = sper.resolve_consortium_keys(None, m["ConsortiumKey"], known_names=KNOWN)
        return sper.clean_table(sper.add_missing_info(m, GRANTS, consortia), live)

    from_table = run(table)
    from_recordset = run(mc.read_manifest(FakeSyn(), "synRS", component="PersonView"))
    assert from_recordset["chairRoles"].tolist() == ["Steering Committee, Working Group", ""]
    pd.testing.assert_frame_equal(from_recordset, from_table)


def test_grant_recordset_and_table_give_the_same_portal_strings(monkeypatch, tmp_path):
    table = pd.DataFrame({
        "Component": "GrantView", "GrantView_id": ["NEW1"], "Grant Name": ["New"], "Grant Number": ["NEW1"],
        "Grant Abstract": ["abs"], "Grant Type": ["U54"], "Grant Theme Name": ["Heterogeneity, Metastasis"],
        "Grant Institution Name": ["Inst B"], "Grant Institution Alias": ["IB"],
        "Grant Investigator": ["B. Person, C. Person"], "Grant Consortium Name": ["CSBC, PS-ON"],
        "Grant Start Date": ["2024-01-01"], "Grant End Date": ["2028-12-31"], "NIH RePORTER Link": ["http://r2"],
        "Duration of Funding": [5], "Embargo End Date": [None], "Grant Synapse Team": [None],
        "Grant Synapse Project": [None], "Id": ["b"], "entityId": "syn53259587"})
    recordset = pd.DataFrame({
        "GrantViewId": ["NEW1"], "GrantName": ["New"], "GrantNumber": ["NEW1"], "GrantAbstract": ["abs"],
        "GrantType": ["U54"], "GrantThemeName": [["Heterogeneity", "Metastasis"]],
        "GrantInstitutionName": [["Inst B"]], "GrantInstitutionAlias": [["IB"]],
        "Investigator": [["B. Person", "C. Person"]], "ConsortiumKey": ["CSBC, PS-ON"],
        "GrantStartDate": ["2024-01-01"], "GrantEndDate": ["2028-12-31"], "NIHRePORTERLink": ["http://r2"],
        "DurationofFunding": ["5"], "EmbargoEndDate": [""], "GrantSynapseTeam": [""], "GrantSynapseProject": [""]})
    recordset_reader(monkeypatch, tmp_path, recordset)
    real = mc.read_manifest

    def run(source):
        factory = _table_factory(table)
        reader = (lambda syn, eid, component=None: table.copy()) if source == "table" else (
            lambda syn, eid, component=None: real(FakeSyn(), eid, component=component))
        with patch.object(cgp, "Table", factory), patch.object(utils, "Table", factory), \
                patch.object(cgp.model_contract, "read_manifest", reader), \
                patch.object(cgp, "create_grant_projects", return_value={"NEW1": ("syn99", "123")}):
            staged = cgp.process_new_grants("syn53259587", "syn21918972", True, MagicMock(), live_types=_live_types())
        return cw.to_portal(staged, "GrantView", _live_types())

    from_table, from_recordset = run("table"), run("recordset")
    new = from_recordset[from_recordset.grantViewId == "NEW1"].iloc[0]
    assert new.investigator == "B. Person, C. Person"
    pd.testing.assert_frame_equal(from_recordset, from_table, check_dtype=False)  # blank cells: NaN vs None
