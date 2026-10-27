"""The admin-manifest source switch: legacy tables by default, admin RecordSets on request."""
import inspect
from types import SimpleNamespace

import pandas as pd
import pytest
from synapseclient.core.constants import concrete_types

import model_contract as mc
import sync_people
import sync_projects
import utils


class FakeSyn:
    def __init__(self, concrete):
        self.concrete = concrete

    def restGET(self, uri):
        return {"concreteType": self.concrete}


def test_constants_name_one_folder_and_one_recordset_per_component():
    assert utils.ADMIN_PROJECT == "syn28742843"
    assert utils.ADMIN_RECORDSET_FOLDER == "admin_recordsets"
    assert utils.ADMIN_COMPONENTS == {"grant": "GrantView", "project": "ProjectView", "people": "PersonView"}
    assert [utils.admin_recordset_name(c) for c in utils.ADMIN_COMPONENTS.values()] == [
        "MC2Center_GrantView_RecordSet", "MC2Center_ProjectView_RecordSet", "MC2Center_PersonView_RecordSet"]


def test_the_source_defaults_to_the_legacy_tables():
    assert utils.MANIFEST_SOURCE == "table"


def test_the_flip_is_documented_as_waiting_for_d1(monkeypatch, capsys):
    source = inspect.getsource(utils).split("MANIFEST_SOURCE = ")[0].rsplit("# Where the grant", 1)[1]
    assert "D-1" in source and "consortiumId" in source and "Sage Bionetworks" in source
    monkeypatch.setattr("sys.argv", ["prog", "--help"])
    with pytest.raises(SystemExit):
        utils.get_args("project")
    help_text = " ".join(capsys.readouterr().out.split())
    assert "D-1" in help_text and "only after the admin migration" in help_text


@pytest.mark.parametrize("resource", ["grant", "project", "people"])
def test_get_args_defaults_to_table_and_accepts_recordset(resource, monkeypatch):
    monkeypatch.setattr("sys.argv", ["prog"])
    assert utils.get_args(resource).manifest_source == "table"
    monkeypatch.setattr("sys.argv", ["prog", "--manifest-source", "recordset"])
    assert utils.get_args(resource).manifest_source == "recordset"


def test_other_resources_have_no_source_switch(monkeypatch):
    monkeypatch.setattr("sys.argv", ["prog"])
    assert not hasattr(utils.get_args("tool"), "manifest_source")


def test_resolve_manifest_id_table_is_the_config_default(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("the table source must not look up a RecordSet")

    monkeypatch.setattr(utils, "find_admin_recordset", boom)
    for resource in utils.ADMIN_COMPONENTS:
        args = SimpleNamespace(manifest_source="table", manifest_id=utils.CONFIG[resource]["manifest"])
        assert utils.resolve_manifest_id(None, args, resource) == utils.CONFIG[resource]["manifest"]


def test_resolve_manifest_id_recordset_looks_up_by_name_and_m_overrides(monkeypatch):
    monkeypatch.setattr(utils, "find_admin_recordset", lambda syn, r: f"synRS-{r}")
    args = SimpleNamespace(manifest_source="recordset", manifest_id=utils.CONFIG["grant"]["manifest"])
    assert utils.resolve_manifest_id(None, args, "grant") == "synRS-grant"
    args = SimpleNamespace(manifest_source="recordset", manifest_id="synOWN")
    assert utils.resolve_manifest_id(None, args, "grant") == "synOWN"


def test_find_admin_recordset_by_folder_and_name(monkeypatch):
    calls = []
    known = {("admin_recordsets", "syn28742843"): "synF", ("MC2Center_PersonView_RecordSet", "synF"): "synRS"}

    def find(name, parent, synapse_client=None):
        calls.append((name, parent))
        return known.get((name, parent))

    monkeypatch.setattr(utils.operations, "find_entity_id", find)
    assert utils.find_admin_recordset(object(), "people") == "synRS"
    assert calls == [("admin_recordsets", "syn28742843"), ("MC2Center_PersonView_RecordSet", "synF")]


def test_find_admin_recordset_before_the_migration_says_so(monkeypatch):
    monkeypatch.setattr(utils.operations, "find_entity_id", lambda name, parent, synapse_client=None: None)
    with pytest.raises(LookupError, match="migrate_admin_manifests_14.py --execute"):
        utils.find_admin_recordset(object(), "grant")


def _recordset(monkeypatch, tmp_path, text):
    path = tmp_path / "rs.csv"
    path.write_text(text)

    class FakeRecordSet:
        def __init__(self, id):
            pass

        def get(self, **kw):
            self.path = str(path)
            return self

    monkeypatch.setattr(mc, "RecordSet", FakeRecordSet)
    return FakeSyn(concrete_types.RECORD_SET_ENTITY)


def test_read_manifest_recordset_with_component_gives_the_table_shape(monkeypatch, tmp_path):
    syn = _recordset(monkeypatch, tmp_path, 'PersonViewId,Name,GrantViewKey\n'
                     'p1,Ada,"CA123456, CA654321"\np2,Bob,CA111111\n')
    out = mc.read_manifest(syn, "synRS", component="PersonView")
    assert list(out.columns) == ["PersonView_id", "Name", "GrantView Key"]
    assert out["GrantView Key"].tolist() == ["CA123456, CA654321", "CA111111"]  # a string column stays text


def test_read_manifest_recordset_parses_list_columns(monkeypatch, tmp_path):
    syn = _recordset(monkeypatch, tmp_path, 'PersonViewId,PersonPublications,ChairRoles\n'
                     'p1,"[""1"",""2""]","[""Working Group"",""Annual Meeting""]"\np2,[],\np3,,\n')
    out = mc.read_manifest(syn, "synRS", component="PersonView")
    assert out["Person Publications"].tolist()[:2] == [["1", "2"], []]
    assert out["Chair Roles"].tolist()[0] == ["Working Group", "Annual Meeting"]
    assert pd.isna(out["Chair Roles"].tolist()[2])


def test_read_manifest_recordset_without_component_is_unchanged(monkeypatch, tmp_path):
    syn = _recordset(monkeypatch, tmp_path, "PersonViewId,Name\np1,Ada\n")
    assert list(mc.read_manifest(syn, "synRS").columns) == ["PersonViewId", "Name"]


def test_a_recordset_manifest_goes_through_prepare_manifest(monkeypatch, tmp_path):
    syn = _recordset(monkeypatch, tmp_path, "ProjectViewId,ProjectName,GrantViewKey,ConsortiumKey,Investigator\n"
                     'a,Core,CA123456,program.csbc,"[""X Y"",""Z W""]"\n')
    manifest = utils.fill_blank(mc.read_manifest(syn, "synRS", component="ProjectView"))
    out = sync_projects.prepare_manifest(manifest)
    assert list(out.columns) == ["ProjectViewId", "ProjectName", "GrantViewKey", "ConsortiumKey", "Investigator"]
    assert out["Investigator"].tolist() == [["X Y", "Z W"]]


@pytest.mark.parametrize("module,resource", [(sync_projects, "project"), (sync_people, "people")])
def test_syncs_read_through_the_switch(module, resource):
    source = inspect.getsource(module.main)
    assert f'resolve_manifest_id(syn, args, "{resource}")' in source
    assert "component=COMPONENT" in source


def test_sync_grants_passes_the_resolved_manifest_to_process_new_grants():
    from unittest.mock import MagicMock, patch

    import sync_grants

    staged = pd.DataFrame({"grantId": ["syn1"]})
    final = pd.DataFrame({"grantId": ["syn1"], "grantViewId": ["OLD1"]})
    args = MagicMock(dryrun=True, verbose=False, noprint=True, manifest_id=utils.CONFIG["grant"]["manifest"],
                     manifest_source="recordset", portal_table_id="t", output_csv="x")
    with patch.object(sync_grants.utils, "syn_login"), \
            patch.object(sync_grants.utils, "get_args", return_value=args), \
            patch.object(sync_grants.utils, "find_admin_recordset", return_value="synRS"), \
            patch.object(sync_grants.crosswalk, "check_crosswalk", return_value={}), \
            patch.object(sync_grants, "check_portal_grant_ids"), \
            patch.object(sync_grants, "process_new_grants", return_value=staged) as process, \
            patch.object(sync_grants.crosswalk, "to_portal", return_value=final):
        sync_grants.main()
    assert process.call_args.args[0] == "synRS"
