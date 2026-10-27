"""annotations/migrate_admin_manifests_14.py with every Synapse call mocked."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs
import utils

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("migrate_admin_manifests_14", ROOT / "annotations" / "migrate_admin_manifests_14.py")
adm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adm)
grants = adm.grants

GRANT_HEADERS = ["Component", "GrantView_id", "Grant Name", "Grant Number", "Grant Abstract", "Grant Type",
                 "Grant Theme Name", "Grant Institution Name", "Grant Institution Alias", "Grant Investigator",
                 "Grant Consortium Name", "Grant Start Date", "Grant End Date", "NIH RePORTER Link",
                 "Duration of Funding", "Id", "entityId"]


def grant_table(rows=None):
    rows = rows if rows is not None else [
        ["GrantView", "CA111111", "One", "CA111111", "abs", "U54", "Heterogeneity, Evolution", "Columbia University",
         "Columbia", "Ada Lovelace, Alan Turing", "CSBC", "6/1/14", "5/31/20", "https://reporter.nih.gov/a", 5, "i1", "syn53259587"],
        ["GrantView", "CA222222", "Two", "CA222222", "abs", "U01", "Heterogeneity", "Columbia University",
         "Columbia", "Grace Hopper", "Sage Bionetworks", "2015-07-01", "2020-06-30", "https://reporter.nih.gov/b", 5, "i2", "syn53259587"],
    ]
    return pd.DataFrame(rows, columns=GRANT_HEADERS)


PROJECT_HEADERS = ["Component", "ProjectView_id", "Project Name", "Project Type", "Project Description",
                   "Project Investigator", "Project Consortium Name", "Project Grant Number", "Id", "entityId"]


def project_table():
    return pd.DataFrame([
        ["ProjectView", "CA1-1", "Core", "Core", "d", "Ada Lovelace", "CSBC", "CA111111", "i1", "syn59074382"],
        ["ProjectView", "CA1-2", "Proj", "Project", "d", "Alan Turing, Grace Hopper", "PS-ON", "CA111111", "i2", "syn59074382"],
    ], columns=PROJECT_HEADERS)


PERSON_HEADERS = ["component", "personGrantNumber", "personConsortiumName", "name", "alternativeNames", "email",
                  "url", "orcidId", "synapseProfileId", "lastKnownInstitution", "workingGroupParticipation",
                  "chairRoles", "consentForPortalDisplay", "portalDisplay", "personPublications", "personDatasets",
                  "personTools"]


def person_table():
    return pd.DataFrame([
        ["PersonView", ["CA111111", "CA222222"], ["CSBC", "HTAN"], "Ada", [""], "a@x.org", None, "0000-0001-5727-8992",
         None, "Columbia University ", ["Image Analysis"], "Working Group, Annual Meeting", "Yes", True, ["1", "2"], [], [""]],
        ["PersonView", ["Affiliated/Non-Grant Associated"], ["PS-ON"], "Bob", ["Robert"], None, "https://b.org", None,
         "123", "MIT", ["None"], None, "Yes", False, [], [], []],
    ], columns=PERSON_HEADERS)


class FakeSynapse:
    """The admin source tables, folders and RecordSets (versions as DataFrames)."""

    def __init__(self, component="GrantView", source=None, existing=None):
        self.component = component
        self.key = f"{component}Id"
        self.source = source if source is not None else grant_table()
        self.folders = {}
        self.rs = dict(existing or {})
        self.rs_folder = {}
        self.created = []
        self.created_folders = []
        self.writes = []
        self.reads = []
        self.next_id = 100

    def read_source(self, syn, table_id):
        self.reads.append(table_id)
        return self.source.copy()

    def find_entity_id(self, name, parent, synapse_client=None):
        return self.folders.get((parent, name))

    def Folder(self, name, parent_id):
        outer = self

        class _F:
            def store(self, synapse_client=None):
                outer.next_id += 1
                fid = f"synF{outer.next_id}"
                outer.folders[(parent_id, name)] = fid
                outer.created_folders.append((parent_id, name))
                return SimpleNamespace(id=fid)

        return _F()

    def find_recordset(self, syn, folder, component):
        return self.rs_folder.get(folder)

    def create_task(self, **kw):
        self.created.append(kw)
        self.next_id += 1
        rs_id = f"synRS{self.next_id}"
        self.rs[rs_id] = [pd.DataFrame(columns=[self.key])]
        self.rs_folder[kw["folder_id"]] = rs_id
        return SimpleNamespace(id=rs_id), SimpleNamespace(task_id=1)

    def info(self, syn, rs_id):
        return len(self.rs[rs_id]), [self.key]

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        return self.rs[rs_id][(version or len(self.rs[rs_id])) - 1].copy()

    def write(self, syn, rs_id, csv_path, on_version=None, **kw):
        written = rs._load_csv(csv_path, rs.list_columns(self.component))
        self.writes.append(written)
        self.rs[rs_id].append(written)
        if on_version:
            on_version(len(self.rs[rs_id]))
        return [len(self.rs[rs_id])]

    def validation(self, syn, rs_id, version, loc, keys):
        return pd.DataFrame({"key": keys, "row_index": range(len(keys)), "is_valid": [True] * len(keys),
                             "validation_error_message": [""] * len(keys), "all_validation_messages": [""] * len(keys)})


def run(tmp_path, fake, *extra, execute=False, run_id="r1", component=None, out="out"):
    component = component or fake.component
    argv = ["--component", component, "--out-dir", str(tmp_path / out), "--run-id", run_id,
            "--cache-dir", str(tmp_path / "cache"), *extra]
    if execute:
        argv += ["--execute", "--write-log-table", "synLOG"]
    with mock.patch.object(adm, "read_source", fake.read_source), \
         mock.patch.object(grants.operations, "find_entity_id", fake.find_entity_id), \
         mock.patch.object(grants, "Folder", fake.Folder), \
         mock.patch.object(grants, "create_record_based_metadata_task", fake.create_task), \
         mock.patch.object(grants.recordsets, "find_recordset", fake.find_recordset), \
         mock.patch.object(grants.recordsets, "recordset_info", fake.info), \
         mock.patch.object(grants.recordsets, "read_recordset", fake.read), \
         mock.patch.object(grants.recordsets, "write_via_grid", fake.write), \
         mock.patch.object(grants.recordsets, "get_validation", fake.validation), \
         mock.patch.object(adm.recordsets, "SynapseTableWriteLog", lambda syn, tid: rs.CsvWriteLog(tmp_path / "log.csv")), \
         mock.patch.object(grants.recordsets, "principal_id", lambda syn: "42"):
        code = adm.main(argv, syn=object())
    return code, tmp_path / out / run_id / component


def status_of(rep):
    return (rep / "summary.txt").read_text().split()[1]


def csv_of(rep, name):
    return pd.read_csv(rep / name, dtype=str, keep_default_na=False)


# --- location and names -----------------------------------------------------------


def test_target_location_and_name_constants():
    assert utils.ADMIN_PROJECT == "syn28742843"
    assert utils.ADMIN_RECORDSET_FOLDER == "admin_recordsets"
    assert [utils.admin_recordset_name(c) for c in adm.COMPONENTS] == [
        "MC2Center_GrantView_RecordSet", "MC2Center_ProjectView_RecordSet", "MC2Center_PersonView_RecordSet"]
    assert adm.KEYS == {"GrantView": "GrantViewId", "ProjectView": "ProjectViewId", "PersonView": "PersonViewId"}


@pytest.mark.parametrize("component,source", [("GrantView", "syn53259587"), ("ProjectView", "syn59074382"),
                                              ("PersonView", "syn38301033")])
def test_default_source_is_the_component_manifest(component, source):
    assert adm.get_args(["--component", component]).source == source


def test_unknown_component_is_refused():
    with pytest.raises(SystemExit):
        adm.get_args(["--component", "PublicationView"])


# --- conversion -------------------------------------------------------------------


def convert(source, component):
    return adm.convert_admin_table(source, component, adm.build_normalizer(component))


def test_grant_conversion_renames_headers_splits_lists_and_drops_non_schema_columns():
    converted, changes, unmapped, dropped, duplicates, key_missing = convert(grant_table(), "GrantView")
    assert not key_missing and duplicates.empty
    assert converted["GrantViewId"].tolist() == ["CA111111", "CA222222"]
    assert "Investigator" in converted.columns and "Grant Investigator" not in converted.columns
    assert converted["Investigator"].tolist()[0] == ["Ada Lovelace", "Alan Turing"]
    assert converted["GrantThemeName"].tolist()[0] == ["Heterogeneity", "Evolution"]
    assert sorted(dropped) == ["Component", "Id", "entityId"]
    assert set(changes["rule"]) == {"consortium_key", "date_iso"}
    assert unmapped.empty


def test_consortium_names_become_program_ids_by_the_vendored_map():
    converted, changes, unmapped, *_ = convert(grant_table(), "GrantView")
    assert converted["ConsortiumKey"].tolist() == ["program.csbc", "program.sage-bionetworks"]
    rows = changes[changes["rule"] == "consortium_key"]
    assert rows[["key", "from", "to"]].values.tolist() == [
        ["CA111111", "CSBC", "program.csbc"], ["CA222222", "Sage Bionetworks", "program.sage-bionetworks"]]
    assert unmapped.empty


def test_the_consortium_map_file_covers_the_approved_names():
    keys = utils.load_consortium_keys()
    assert keys["CSBC"] == "program.csbc" and keys["PS-ON"] == "program.ps-on" and keys["MetNet"] == "program.metnet"
    assert keys["NCI"] == "program.nci" and keys["HTAN"] == "program.htan"
    assert keys["Sage Bionetworks"] == "program.sage-bionetworks" and len(keys) == 11


def test_an_unknown_consortium_stays_and_is_reported_and_a_program_id_is_left_alone():
    source = grant_table()
    source.loc[0, "Grant Consortium Name"] = "program.csbc"
    source.loc[1, "Grant Consortium Name"] = "Mystery Network"
    converted, changes, unmapped, *_ = convert(source, "GrantView")
    assert converted["ConsortiumKey"].tolist() == ["program.csbc", "Mystery Network"]
    assert unmapped[unmapped["column"] == "ConsortiumKey"].values.tolist() == [["CA222222", "ConsortiumKey", "Mystery Network"]]
    assert "consortium_key" not in set(changes["rule"])


def test_consortium_names_match_case_insensitively_and_trimmed():
    source = grant_table()
    source.loc[0, "Grant Consortium Name"] = "  csbc "
    source.loc[1, "Grant Consortium Name"] = "SAGE BIONETWORKS, metnet"
    converted, changes, unmapped, *_ = convert(source, "GrantView")
    assert converted["ConsortiumKey"].tolist() == ["program.csbc", "program.sage-bionetworks, program.metnet"]
    assert unmapped.empty and (changes["rule"] == "consortium_key").sum() == 3


def test_a_program_id_outside_the_map_is_reported_unmapped():
    source = grant_table()
    source.loc[0, "Grant Consortium Name"] = "program.nope, program.htan"
    converted, _, unmapped, *_ = convert(source, "GrantView")
    assert converted["ConsortiumKey"].tolist()[0] == "program.nope, program.htan"
    assert unmapped[unmapped["column"] == "ConsortiumKey"]["value"].tolist() == ["program.nope"]


def test_an_override_maps_a_name_before_the_consortium_map(tmp_path):
    path = tmp_path / "o.csv"
    path.write_text("column,current_value,proposed_value\nConsortiumKey,CSBC,program.other\n")
    norm = adm.build_normalizer("GrantView", adm.value_maps.load_overrides(path))
    converted, changes, *_ = adm.convert_admin_table(grant_table(), "GrantView", norm)
    assert converted["ConsortiumKey"].tolist()[0] == "program.other"
    assert changes[changes["rule"] == "override"]["to"].tolist() == ["program.other"]


def test_dates_become_iso_and_iso_dates_and_junk_are_left_alone():
    source = grant_table()
    source.loc[1, "Grant End Date"] = "6/30/2021"
    source.loc[1, "Grant Start Date"] = "soon"
    converted, changes, unmapped, *_ = convert(source, "GrantView")
    assert converted["GrantStartDate"].tolist() == ["2014-06-01", "soon"]
    assert converted["GrantEndDate"].tolist() == ["2020-05-31", "2021-06-30"]
    dates = changes[changes["rule"] == "date_iso"]
    assert dates[["key", "column", "from", "to"]].values.tolist() == [
        ["CA111111", "GrantStartDate", "6/1/14", "2014-06-01"], ["CA111111", "GrantEndDate", "5/31/20", "2020-05-31"],
        ["CA222222", "GrantEndDate", "6/30/2021", "2021-06-30"]]
    assert unmapped[unmapped["column"] == "GrantStartDate"].values.tolist() == [["CA222222", "GrantStartDate", "soon"]]


def test_project_conversion_uses_the_admin_header_map():
    converted, _, unmapped, dropped, _, _ = convert(project_table(), "ProjectView")
    assert list(converted.columns) == ["ProjectViewId", "ProjectName", "ProjectType", "ProjectDescription",
                                       "Investigator", "ConsortiumKey", "GrantViewKey"]
    assert converted["Investigator"].tolist()[1] == ["Alan Turing", "Grace Hopper"]
    assert converted["GrantViewKey"].tolist() == ["CA111111", "CA111111"]
    assert converted["ConsortiumKey"].tolist() == ["program.csbc", "program.ps-on"]
    assert unmapped.empty


def test_person_conversion_joins_list_cells_of_string_columns_and_fixes_enum_case():
    converted, changes, unmapped, dropped, duplicates, key_missing = convert(person_table(), "PersonView")
    assert not key_missing and duplicates.empty
    assert converted["PersonViewId"].tolist() == ["person.ada", "person.bob"] and list(converted.columns)[0] == "PersonViewId"
    assert converted["GrantViewKey"].tolist() == ["CA111111, CA222222", "Affiliated/Non-Grant Associated"]
    assert converted["ConsortiumKey"].tolist() == ["program.csbc, program.htan", "program.ps-on"]
    assert converted["AlternativeNames"].tolist() == [[], ["Robert"]]
    assert converted["ChairRoles"].tolist() == [["Working Group", "Annual Meeting"], []]
    assert converted["PortalDisplay"].tolist() == ["TRUE", "FALSE"]
    assert converted["LastKnownInstitution"].tolist()[0] == "Columbia University"
    assert set(changes["rule"]) == {"list_to_string", "enum_case", "minted_key", "consortium_key"}
    assert changes[changes["rule"] == "minted_key"][["key", "column", "from", "to"]].values.tolist() == [
        ["person.ada", "PersonViewId", "", "person.ada"], ["person.bob", "PersonViewId", "", "person.bob"]]
    assert unmapped.empty


@pytest.mark.parametrize("name,key", [
    ("Meenal Datta", "person.meenal-datta"), ("Cristina Caescu", "person.cristina-caescu"),
    ("José  Núñez-O'Brien Jr.", "person.jose-nunez-o-brien-jr"), ("  Anil   Korkut ", "person.anil-korkut"),
    ("Zoë Müller", "person.zoe-muller"), ("", ""), ("!!!", ""), (None, "")])
def test_minted_person_keys(name, key):
    assert adm.mint_person_key(name) == key


@pytest.mark.parametrize("name,key", [
    ("Christine M. Lovly, MD, PhD", "person.christine-m-lovly"),
    ("Robert C. G. Martin II, M.D., Ph.D.", "person.robert-c-g-martin-ii"),
    ("Robert C. G. Martin II M.D. Ph.D.", "person.robert-c-g-martin-ii"),
    ("Ada Lovelace Ph. D.", "person.ada-lovelace"),
    ("Ada Lovelace phd", "person.ada-lovelace"),
    ("Ann Poe, DVM, MPH, MBA", "person.ann-poe"),
    ("Ann Poe MSc", "person.ann-poe"), ("Ann Poe, PharmD, DrPH, ScD, RN", "person.ann-poe"),
    ("Ann Poe, DO", "person.ann-poe"), ("Ann Poe D.O.", "person.ann-poe"), ("Ann Poe M.S.", "person.ann-poe"),
    ("Thuy Do", "person.thuy-do"), ("Pat Ms", "person.pat-ms"),  # a surname is not a credential
    ("Julia M D'Angelo", "person.julia-m-d-angelo"),
    ("Martin Luther King Jr., PhD", "person.martin-luther-king-jr"), ("John Smith III", "person.john-smith-iii"),
    ("PhD", "person.phd"),
])
def test_trailing_credentials_are_dropped_from_minted_keys(name, key):
    assert adm.mint_person_key(name) == key


def test_a_key_override_sets_the_key_and_is_noted(tmp_path):
    path = tmp_path / "o.csv"
    path.write_text("column,current_value,proposed_value\nPersonViewId,Bob,person.robert-b\n")
    norm = adm.build_normalizer("PersonView", adm.value_maps.load_overrides(path))
    converted, changes, *_ = adm.convert_admin_table(person_table(), "PersonView", norm)
    assert converted["PersonViewId"].tolist() == ["person.ada", "person.robert-b"]
    minted = changes[changes["rule"] == "minted_key"]
    assert minted[["key", "from"]].values.tolist() == [["person.ada", ""], ["person.robert-b", "(override for Name 'Bob')"]]


def test_slug_collisions_and_blank_names_are_reported_not_disambiguated():
    source = person_table()
    source.loc[1, "name"] = "ADA"
    source = pd.concat([source, source.iloc[[0]].assign(name="")], ignore_index=True)
    converted, changes, unmapped, dropped, duplicates, key_missing = convert(source, "PersonView")
    assert duplicates.to_dict("records") == [{"key": "person.ada", "rows": "1, 2"}, {"key": "", "rows": "3"}]
    assert converted["PersonViewId"].tolist() == ["person.ada", "person.ada", ""]


def test_duplicate_and_blank_keys_are_reported_not_resolved():
    source = grant_table()
    source.loc[1, "GrantView_id"] = "CA111111"
    *_, duplicates, _ = convert(source, "GrantView")
    assert duplicates.to_dict("records") == [{"key": "CA111111", "rows": "1, 2"}]
    source.loc[1, "GrantView_id"] = " "
    *_, duplicates, _ = convert(source, "GrantView")
    assert duplicates.to_dict("records") == [{"key": "", "rows": "2"}]


def test_unknown_header_fails_the_conversion():
    with pytest.raises(grants.ConversionError, match="unknown headers"):
        convert(grant_table().rename(columns={"Grant Name": "Mystery"}), "GrantView")


# --- dry run ----------------------------------------------------------------------


def test_dry_run_writes_nothing_to_synapse_and_writes_the_reports(tmp_path):
    fake = FakeSynapse()
    code, rep = run(tmp_path, fake)
    assert code == 0  # every value maps and validates
    assert status_of(rep) == "dryrun"
    assert not fake.created and not fake.created_folders and not fake.writes
    for name in ("converted.csv", "changes.csv", "unmapped.csv", "validation.csv", "validation_counts.csv", "summary.txt"):
        assert (rep / name).exists()
    text = (rep.parent / "comparison.txt").read_text()
    assert "source table rows=2 unique keys=2; compared rows=2 unique keys=2" in text and "EQUAL" in text


def test_duplicate_keys_fail_the_component_and_nothing_is_written(tmp_path):
    source = grant_table()
    source.loc[1, "GrantView_id"] = "CA111111"
    fake = FakeSynapse(source=source)
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and status_of(rep) == "failed"
    assert csv_of(rep, "duplicates.csv").to_dict("records") == [{"key": "CA111111", "rows": "1, 2"}]
    assert not fake.created and not fake.writes


def test_person_keys_are_minted_and_compared_as_row_count_and_uniqueness(tmp_path):
    fake = FakeSynapse("PersonView", person_table())
    code, rep = run(tmp_path, fake)
    assert code == 2  # migrate-and-flag: ChairRoles required etc.
    assert status_of(rep) == "dryrun"
    assert csv_of(rep, "converted.csv")["PersonViewId"].tolist() == ["person.ada", "person.bob"]
    assert set(csv_of(rep, "changes.csv")["rule"]) >= {"minted_key"}
    text = (rep.parent / "comparison.txt").read_text()
    assert "source table rows=2; converted rows=2; minted keys unique and non-blank=True" in text and "EQUAL" in text


def test_person_key_collision_fails_and_writes_nothing_even_with_execute(tmp_path):
    source = person_table()
    source.loc[1, "name"] = "ADA"
    fake = FakeSynapse("PersonView", source)
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and status_of(rep) == "failed"
    assert csv_of(rep, "duplicates.csv").to_dict("records") == [{"key": "person.ada", "rows": "1, 2"}]
    assert not fake.created and not fake.created_folders and not fake.writes
    assert "DIFFERENT" in (rep.parent / "comparison.txt").read_text()


def test_person_without_a_name_column_has_no_key_and_nothing_is_written(tmp_path):
    fake = FakeSynapse("PersonView", person_table().drop(columns=["name"]))
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and status_of(rep) == "failed"
    assert "PersonViewId cannot be filled" in (rep / "summary.txt").read_text()
    assert not fake.created and not fake.writes
    assert csv_of(rep, "converted.csv")["PersonViewId"].tolist() == ["", ""]
    assert "PersonViewId" in set(csv_of(rep, "validation.csv")["field"])


# --- execute ----------------------------------------------------------------------


def test_execute_creates_the_admin_folder_and_recordset_loads_through_grid_and_logs(tmp_path):
    fake = FakeSynapse()
    code, rep = run(tmp_path, fake, execute=True)
    assert fake.created_folders == [("syn28742843", "admin_recordsets")]
    kw = fake.created[0]
    assert kw["create_grid"] is False and kw["bind_schema_to_record_set"] is True
    assert kw["record_set_name"] == "MC2Center_GrantView_RecordSet"
    assert kw["project_id"] == "syn28742843" and kw["folder_id"] == fake.folders[("syn28742843", "admin_recordsets")]
    assert kw["upsert_keys"] == ["GrantViewId"] and kw["schema_uri"] == "MC2Center-GrantView-14.0.0"
    assert "admin manifest" in kw["record_set_description"] and "syn53259587" in kw["record_set_description"]
    rs_id = next(iter(fake.rs))
    assert len(fake.writes) == 1 and len(fake.rs[rs_id]) == 2
    log = rs.CsvWriteLog(tmp_path / "log.csv")._read(rs_id)
    assert log["status"].tolist() == ["created", "pending", "written", "done", "verified"]
    assert status_of(rep) == "migrated"
    assert (rep / "synapse_validation.csv").exists()


@pytest.mark.parametrize("component,source", [("ProjectView", project_table), ("GrantView", grant_table)])
def test_each_component_gets_its_own_named_recordset(tmp_path, component, source):
    fake = FakeSynapse(component, source())
    run(tmp_path, fake, execute=True)
    assert fake.created[0]["record_set_name"] == f"MC2Center_{component}_RecordSet"
    assert fake.created[0]["upsert_keys"] == [f"{component}Id"]


def test_execute_uses_the_schema_version_argument(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, "--schema-version", "14.0.1", execute=True)
    assert fake.created[0]["schema_uri"] == "MC2Center-GrantView-14.0.1"


def test_execute_needs_a_write_log_table(tmp_path):
    with pytest.raises(SystemExit):
        adm.get_args(["--component", "GrantView", "--execute"])


def test_redo_is_refused_with_execute():
    with pytest.raises(SystemExit):
        adm.get_args(["--component", "GrantView", "--execute", "--write-log-table", "synL", "--redo"])


def test_execute_verifies_the_read_back_and_reports_a_mismatch(tmp_path):
    fake = FakeSynapse()
    real = fake.write

    def corrupt(syn, rs_id, csv_path, on_version=None, **kw):
        out = real(syn, rs_id, csv_path, on_version, **kw)
        fake.rs[rs_id][-1].loc[0, "GrantName"] = "Changed"
        return out

    fake.write = corrupt
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and status_of(rep) == "verification_mismatch"


# --- resume -----------------------------------------------------------------------


def test_resume_skips_a_done_dry_run_and_redo_repeats_it(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, run_id="r1")
    fake.reads.clear()
    code, rep = run(tmp_path, fake, run_id="r2")
    assert fake.reads == [] and "already dryrun in run r1; skipped" in (rep / "summary.txt").read_text()
    assert "EQUAL" in (rep.parent / "comparison.txt").read_text()
    run(tmp_path, fake, "--redo", run_id="r3")
    assert fake.reads == ["syn53259587"]


def test_resume_does_not_skip_when_the_options_change(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, run_id="r1")
    fake.reads.clear()
    path = tmp_path / "o.csv"
    path.write_text("column,current_value,proposed_value\nConsortiumKey,CSBC,program.csbc\n")
    run(tmp_path, fake, "--overrides", str(path), run_id="r2")
    assert fake.reads == ["syn53259587"]


def test_resume_does_not_skip_when_the_source_etag_or_the_rules_version_changes(tmp_path, monkeypatch):
    fake = FakeSynapse()
    etag = ["e1"]
    monkeypatch.setattr(adm, "source_etag", lambda syn, table_id: etag[0])
    run(tmp_path, fake, run_id="r1")
    fake.reads.clear()
    run(tmp_path, fake, run_id="r2")
    assert fake.reads == []
    etag[0] = "e2"
    run(tmp_path, fake, run_id="r3")
    assert fake.reads == ["syn53259587"]
    run(tmp_path, fake, run_id="r4")
    assert fake.reads == ["syn53259587"]
    monkeypatch.setattr(adm, "CONVERSION_RULES", adm.CONVERSION_RULES + 1)
    run(tmp_path, fake, run_id="r5")
    assert fake.reads == ["syn53259587"] * 2


def test_source_etag_is_read_from_the_entity_metadata():
    class Syn:
        def restGET(self, uri):
            assert uri == "/entity/syn1"
            return {"etag": "abc"}

    assert adm.source_etag(Syn(), "syn1") == "abc"
    assert adm.source_etag(object(), "syn1") == ""


def test_resume_does_not_skip_a_failed_component(tmp_path):
    bad = grant_table().rename(columns={"Grant Name": "Mystery"})
    fake = FakeSynapse(source=bad)
    run(tmp_path, fake, run_id="r1")
    fake.source = grant_table()
    code, rep = run(tmp_path, fake, run_id="r2")
    assert status_of(rep) == "dryrun"


def test_a_dry_run_does_not_make_the_execute_skip(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, run_id="r1")
    code, rep = run(tmp_path, fake, run_id="r2", execute=True)
    assert status_of(rep) == "migrated"


def test_a_migrated_recordset_is_not_written_again(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, run_id="r1", execute=True)
    code, rep = run(tmp_path, fake, run_id="r2", execute=True)
    assert len(fake.writes) == 1 and len(fake.created) == 1
    assert "already" in (rep / "summary.txt").read_text()


def test_an_existing_recordset_without_a_logged_write_is_never_overwritten(tmp_path):
    fake = FakeSynapse()
    fake.folders[("syn28742843", "admin_recordsets")] = "synFx"
    fake.rs["synRSx"] = [pd.DataFrame({"GrantViewId": ["CA1"]})]
    fake.rs_folder["synFx"] = "synRSx"
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and status_of(rep) == "failed"
    assert "exists but not migrated" in (rep / "summary.txt").read_text()
    assert not fake.writes and not fake.created
