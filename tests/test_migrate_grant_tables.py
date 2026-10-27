"""annotations/migrate_grant_tables_14.py with every Synapse call mocked."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("migrate_grant_tables_14", ROOT / "annotations" / "migrate_grant_tables_14.py")
mig = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mig)

KEY = "PublicationViewId"
HEADERS = ["Component", "PublicationView_id", "GrantView Key", "Publication Title", "Publication Doi",
           "Publication Assay", "Publication Tumor Type", "Publication Accessibility", "Id", "entityId"]


def source_table(rows=None, headers=HEADERS):
    rows = rows if rows is not None else [
        ["PublicationView", "p1", "CA123456", "First", "10.1038/abc", "rna-seq, Mass Spectrometry", "Adenocarcinoma", "open access", "1", "syn1"],
        ["PublicationView", "p2", "CA123456", "Second", "No DOI Listed", "RNA Sequencing", "", "Open Access", "2", "syn2"],
    ]
    return pd.DataFrame(rows, columns=headers)


class FakeSynapse:
    """Grant tables, projects, folders and RecordSets (versions as DataFrames)."""

    def __init__(self, tables=None, existing=None, grants=None):
        # tables: {table id: (grant number, project, frame)}
        self.tables = tables or {"synT1": ("CA1", "synP1", source_table())}
        self.grants = grants or {p: g for g, p, _ in self.tables.values()}
        self.folders = {}        # (project, name) -> folder id
        self.rs = dict(existing or {})  # recordset id -> list of version frames
        self.rs_folder = {}      # folder id -> recordset id
        self.created = []        # kwargs of every create_record_based_metadata_task call
        self.created_folders = []
        self.writes = []
        self.next_id = 100
        self.reference_source = None  # merged-manifest frame (13.1.0 headers) or None
        self.reference_reads = 0
        self.portal_source = None     # live portal table frame or None

    def read_portal(self, syn, component):
        self.portal_reads = getattr(self, "portal_reads", 0) + 1
        if self.portal_source is None:
            raise RuntimeError("no portal table")
        return self.portal_source.copy()

    def read_reference(self, syn, component):
        self.reference_reads += 1
        if self.reference_source is None:
            raise RuntimeError("no merged manifest")
        return self.reference_source.copy()

    # views / tables
    def list_tables(self, syn, component):
        return pd.DataFrame({"id": list(self.tables), "name": ["x"] * len(self.tables)})

    def grants_map(self, syn):
        return dict(self.grants)

    def project_of(self, syn, table_id):
        project = self.tables[table_id][1]
        return project, f"Project {project}"

    def read_source(self, syn, table_id):
        return self.tables[table_id][2].copy()

    # folders / recordsets
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
        self.rs[rs_id] = [pd.DataFrame(columns=[KEY])]
        self.rs_folder[kw["folder_id"]] = rs_id
        return SimpleNamespace(id=rs_id), SimpleNamespace(task_id=1)

    def info(self, syn, rs_id):
        return len(self.rs[rs_id]), [KEY]

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        return self.rs[rs_id][(version or len(self.rs[rs_id])) - 1].copy()

    def write(self, syn, rs_id, csv_path, on_version=None, **kw):
        written = rs._load_csv(csv_path, {"Assay", "TumorType"})
        self.writes.append(written)
        self.rs[rs_id].append(written)
        if on_version:
            on_version(len(self.rs[rs_id]))
        return [len(self.rs[rs_id])]

    def validation(self, syn, rs_id, version, loc, keys):
        return pd.DataFrame({"key": keys, "row_index": range(len(keys)), "is_valid": [True] * len(keys),
                             "validation_error_message": [""] * len(keys), "all_validation_messages": [""] * len(keys)})


def run(tmp_path, fake, *extra, execute=False, log=None, run_id="r1", component="PublicationView", out="out"):
    log = log or tmp_path / "log.csv"
    argv = ["--component", component, "--out-dir", str(tmp_path / out), "--run-id", run_id,
            "--cache-dir", str(tmp_path / "cache"), *extra]
    if execute:
        argv += ["--execute", "--write-log-table", "synLOG"]
    with mock.patch.object(mig, "list_grant_tables", fake.list_tables), \
         mock.patch.object(mig, "grants_map", fake.grants_map), \
         mock.patch.object(mig, "project_of", fake.project_of), \
         mock.patch.object(mig, "read_source", fake.read_source), \
         mock.patch.object(mig, "read_reference", fake.read_reference), \
         mock.patch.object(mig, "read_portal", fake.read_portal), \
         mock.patch.object(mig.operations, "find_entity_id", fake.find_entity_id), \
         mock.patch.object(mig, "Folder", fake.Folder), \
         mock.patch.object(mig, "create_record_based_metadata_task", fake.create_task), \
         mock.patch.object(mig.recordsets, "find_recordset", fake.find_recordset), \
         mock.patch.object(mig.recordsets, "recordset_info", fake.info), \
         mock.patch.object(mig.recordsets, "read_recordset", fake.read), \
         mock.patch.object(mig.recordsets, "write_via_grid", fake.write), \
         mock.patch.object(mig.recordsets, "get_validation", fake.validation), \
         mock.patch.object(mig.recordsets, "SynapseTableWriteLog", lambda syn, tid: rs.CsvWriteLog(log)), \
         mock.patch.object(mig.recordsets, "principal_id", lambda syn: "42"), \
         mock.patch.object(mig.merge_tables, "build_union", lambda syn, comp, grants: fake.union(grants)):
        code = mig.main(argv, syn=object())
    return code, tmp_path / out / run_id


def union_of(fake):
    """A stand-in for merge_tables.build_union: the latest version of each project's RecordSet."""
    def build(grants):
        frames = []
        for g in grants:
            folder = fake.folders.get((g, "publications"))
            rs_id = fake.rs_folder.get(folder)
            if rs_id:
                frames.append(fake.rs[rs_id][-1])
        union = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[KEY])
        union = union.rename(columns={KEY: "PublicationView_id"})
        return union, {"grants_scanned": len(grants), "recordsets_found": len(frames),
                       "missing": len(grants) - len(frames), "rows": len(union), "duplicate_keys": 0}
    return build


@pytest.fixture(autouse=True)
def _union(monkeypatch):
    monkeypatch.setattr(FakeSynapse, "union", lambda self, grants: union_of(self)(grants), raising=False)


def status_of(rep, grant="CA1"):
    return (rep / grant / "summary.txt").read_text().split()[1]


# --- conversion -----------------------------------------------------------------


def test_conversion_renames_headers_normalizes_values_and_keeps_schema_columns():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    converted, changes, unmapped, dropped, resolutions = mig.convert_table(source_table(), "PublicationView", norm)
    assert converted.columns.tolist() == [KEY, "GrantViewKey", "PublicationTitle", "PublicationDoi", "Assay",
                                          "TumorType", "PublicationAccessibility"]
    assert sorted(dropped) == ["Component", "Id", "entityId"]
    row = converted.set_index(KEY).loc["p1"]
    assert row["Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert row["PublicationDoi"] == "https://doi.org/10.1038/abc"
    assert row["PublicationAccessibility"] == "Open Access"
    assert converted.set_index(KEY).loc["p2", "PublicationDoi"] == ""
    assert sorted(changes["rule"]) == ["doi", "doi_placeholder", "enum_case", "nonpreferred"]
    assert unmapped.empty


def test_unknown_header_fails_the_conversion():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    table = source_table().rename(columns={"Publication Title": "Mystery Column"})
    with pytest.raises(mig.ConversionError, match="unknown headers: \\['Mystery Column'\\]"):
        mig.convert_table(table, "PublicationView", norm)


def test_blank_keys_still_fail_the_conversion():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    table = source_table()
    table.loc[1, "PublicationView_id"] = " "
    with pytest.raises(mig.ConversionError, match="blank key"):
        mig.convert_table(table, "PublicationView", norm)


def dup_table(second_title="Second copy", first_id="10", second_id="11", blank_second=False):
    table = source_table()
    table.loc[1, "PublicationView_id"] = "p1"
    table.loc[1, "Publication Title"] = second_title
    for column in ("Publication Doi", "Publication Tumor Type", "Publication Assay", "Publication Accessibility"):
        table.loc[1, column] = table.loc[0, column]
    table.loc[0, "Id"], table.loc[1, "Id"] = first_id, second_id
    if blank_second:
        table.loc[1, "Publication Tumor Type"] = ""
        table.loc[0, "Publication Tumor Type"] = "Adenocarcinoma"
    return table


def reference_for(title):
    return pd.DataFrame([["p1", "CA123456", title, "Adenocarcinoma"]],
                        columns=["PublicationView_id", "GrantView Key", "Publication Title", "Tumor Type"])


def convert_dup(table, ref_frame):
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    ref = None if ref_frame is None else mig.reference_frame(ref_frame, "PublicationView", norm)
    return mig.convert_table(table, "PublicationView", norm, lambda: ref)


def test_duplicate_key_keeps_the_row_that_matches_the_merged_manifest():
    converted, _, _, _, res = convert_dup(dup_table(), reference_for("Second copy"))
    assert converted["PublicationTitle"].tolist() == ["Second copy"]
    assert res.to_dict("records") == [{"key": "p1", "kept_row": "row 2 (Id 11)", "dropped_row": "row 1 (Id 10)",
                                       "reason": "matches the merged manifest"}]
    converted, _, _, _, res = convert_dup(dup_table(), reference_for("First"))
    assert converted["PublicationTitle"].tolist() == ["First"] and res.iloc[0]["kept_row"] == "row 1 (Id 10)"


def test_duplicate_key_neither_match_keeps_the_most_complete_row():
    converted, _, _, _, res = convert_dup(dup_table(blank_second=True), reference_for("Something else"))
    assert converted["PublicationTitle"].tolist() == ["First"]
    assert res.iloc[0]["reason"].startswith("no row matches the merged manifest; most complete row")


def test_duplicate_key_both_match_keeps_the_most_complete_row():
    table = dup_table(second_title="First", blank_second=True)
    converted, _, _, _, res = convert_dup(table, reference_for("First").drop(columns=["Tumor Type"]))
    assert converted["TumorType"].tolist() == [["Adenocarcinoma"]]
    assert res.iloc[0]["reason"].startswith("both rows match the merged manifest; most complete row")


def test_duplicate_key_tie_keeps_the_higher_legacy_id():
    converted, _, _, _, res = convert_dup(dup_table(first_id="9", second_id="10"), None)
    assert converted["PublicationTitle"].tolist() == ["Second copy"]
    assert "tie on completeness" in res.iloc[0]["reason"] and "higher legacy Id" in res.iloc[0]["reason"]
    assert res.iloc[0]["reason"].startswith("no merged manifest to compare")
    converted, _, _, _, _ = convert_dup(dup_table(first_id="100", second_id="20"), None)
    assert converted["PublicationTitle"].tolist() == ["First"]  # numeric, not text, comparison


def test_duplicate_key_tie_compares_uuid_ids_as_text():
    converted, _, _, _, res = convert_dup(dup_table(first_id="3fbe8ce2-83cb", second_id="9d2b6960-a613"), None)
    assert converted["PublicationTitle"].tolist() == ["Second copy"]
    converted, _, _, _, _ = convert_dup(dup_table(first_id="9d2b6960-a613", second_id="3fbe8ce2-83cb"), None)
    assert converted["PublicationTitle"].tolist() == ["First"]


def test_duplicate_key_absent_from_the_manifest_is_reported_as_such():
    other = reference_for("x")
    other["PublicationView_id"] = "zzz"
    _, _, _, _, res = convert_dup(dup_table(), other)
    assert res.iloc[0]["reason"].startswith("key is not in the merged manifest")


def test_resolution_leaves_changes_of_the_dropped_row_out():
    table = dup_table(second_title="Second copy")
    table.loc[0, "Publication Doi"] = "10.1000/first"
    table.loc[1, "Publication Doi"] = "10.1000/second"
    _, changes, _, _, _ = convert_dup(table, reference_for("Second copy"))
    assert "https://doi.org/10.1000/second" in changes["to"].tolist()
    assert "https://doi.org/10.1000/first" not in changes["to"].tolist()


def test_every_component_has_a_merged_manifest_reference():
    for component, resource in mig.REFERENCE_RESOURCE.items():
        assert mig.utils.CONFIG[resource]["manifest"].startswith("syn")


def test_local_validation_reports_key_field_and_message():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    converted, *_ = mig.convert_table(source_table(), "PublicationView", norm)
    validation = mig.validate_frame(converted, "PublicationView")
    assert validation.columns.tolist() == ["key", "field", "validator", "message"]
    required = validation[validation["validator"] == "required"]
    assert {"PublicationAbstract", "PublicationJournal"} <= set(required["field"])
    counts = mig.validation_counts(validation)
    assert counts["count"].iloc[0] >= 1


def test_local_validation_flags_a_non_enum_value_and_a_bad_uri():
    frame = pd.DataFrame({KEY: ["p1"], "PublicationAccessibility": ["Embargoed"], "PublicationDoi": ["not a uri"],
                          "Assay": [["RNA Sequencing"]]}).astype(object)
    validation = mig.validate_frame(frame, "PublicationView")
    by_field = validation.set_index("field")["validator"].to_dict()
    assert by_field["PublicationAccessibility"] == "enum"
    assert by_field["PublicationDoi"] == "format"


# --- dry run ----------------------------------------------------------------------


def test_dry_run_writes_nothing_to_synapse_and_writes_the_reports(tmp_path):
    fake = FakeSynapse()
    code, rep = run(tmp_path, fake)
    assert fake.writes == [] and fake.created == [] and fake.created_folders == []
    assert not (tmp_path / "log.csv").exists()
    grant = rep / "CA1"
    for name in ("converted.csv", "changes.csv", "unmapped.csv", "validation.csv", "summary.txt"):
        assert (grant / name).exists(), name
    assert status_of(rep) == "dryrun"
    changes = pd.read_csv(grant / "changes.csv", dtype=str, keep_default_na=False)
    assert changes.columns.tolist() == ["key", "column", "from", "to", "rule"]
    assert code == 2  # local validation failures (the fixture rows lack required fields)
    assert (rep / "comparison.txt").exists()
    assert "EQUAL" in (rep / "comparison.txt").read_text()


def test_unknown_header_fails_that_grant_and_the_others_continue(tmp_path):
    bad = source_table().rename(columns={"Publication Title": "Mystery Column"})
    fake = FakeSynapse({"synT1": ("CA1", "synP1", bad), "synT2": ("CA2", "synP2", source_table())})
    code, rep = run(tmp_path, fake)
    assert status_of(rep, "CA1") == "failed" and status_of(rep, "CA2") == "dryrun"
    assert "Mystery Column" in (rep / "CA1" / "summary.txt").read_text()
    assert code == 1


def test_duplicate_key_is_resolved_not_failed_and_reported(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", dup_table())})
    fake.reference_source = reference_for("Second copy")
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "dryrun"
    resolved = pd.read_csv(rep / "CA1" / "duplicates_resolved.csv", dtype=str)
    assert resolved.columns.tolist() == ["key", "kept_row", "dropped_row", "reason"] and len(resolved) == 1
    assert "duplicates_resolved=1" in (rep / "CA1" / "summary.txt").read_text()
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str)
    assert converted["PublicationTitle"].tolist() == ["Second copy"]
    assert "duplicate keys resolved: 1" in (rep / "run_summary.txt").read_text()


def test_merged_manifest_is_read_once_per_run_and_only_when_needed(tmp_path):
    tables = {"synT1": ("CA1", "synP1", dup_table()), "synT2": ("CA2", "synP2", dup_table()),
              "synT3": ("CA3", "synP3", source_table())}
    fake = FakeSynapse(tables)
    fake.reference_source = reference_for("Second copy")
    run(tmp_path, fake)
    assert fake.reference_reads == 1
    fake = FakeSynapse({"synT3": ("CA3", "synP3", source_table())})
    run(tmp_path, fake, out="out2")
    assert fake.reference_reads == 0


def test_unreadable_merged_manifest_falls_back_to_completeness(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", dup_table(first_id="9", second_id="10"))})
    code, rep = run(tmp_path, fake)  # read_reference raises
    assert status_of(rep) == "dryrun"
    assert "no merged manifest to compare" in (rep / "CA1" / "duplicates_resolved.csv").read_text()


def test_grants_and_limit_filters(tmp_path):
    tables = {f"synT{i}": (f"CA{i}", f"synP{i}", source_table()) for i in (1, 2, 3)}
    code, rep = run(tmp_path, FakeSynapse(tables), "--grants", "CA2")
    assert sorted(p.name for p in rep.iterdir() if p.is_dir()) == ["CA2"]
    code, rep = run(tmp_path, FakeSynapse(tables), "--limit", "2", out="out2")
    assert sorted(p.name for p in rep.iterdir() if p.is_dir()) == ["CA1", "CA2"]


def test_overrides_are_applied(tmp_path):
    ov = tmp_path / "o.csv"
    pd.DataFrame({"column": ["Publication Accessibility"], "current_value": ["open access"],
                  "proposed_value": ["Restricted Access"]}).to_csv(ov, index=False)
    code, rep = run(tmp_path, FakeSynapse(), "--overrides", str(ov))
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str, keep_default_na=False).set_index(KEY)
    assert converted.loc["p1", "PublicationAccessibility"] == "Restricted Access"
    changes = pd.read_csv(rep / "CA1" / "changes.csv", dtype=str, keep_default_na=False)
    assert "override" in changes["rule"].tolist()


def test_unmapped_value_gives_exit_2_without_failures(tmp_path):
    table = source_table()
    table.loc[0, "Publication Tumor Type"] = "Not a tumor type"
    fake = FakeSynapse({"synT1": ("CA1", "synP1", table)})
    code, rep = run(tmp_path, fake)
    assert code == 2
    unmapped = pd.read_csv(rep / "CA1" / "unmapped.csv", dtype=str, keep_default_na=False)
    assert unmapped["value"].tolist() == ["Not a tumor type"]


def test_unlisted_projects_are_skipped_with_no_include_unlisted(tmp_path, capsys):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", source_table()), "synT2": ("CA2", "synP2", source_table())},
                       grants={"synP1": "CA1"})
    code, rep = run(tmp_path, fake, "--no-include-unlisted")
    assert sorted(p.name for p in rep.iterdir() if p.is_dir()) == ["CA1"]
    assert "synT2: project synP2 is not in the Grants table" in capsys.readouterr().out


def test_unlisted_projects_are_included_by_default_labelled_by_name(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", source_table()), "synT2": ("x", "synP2", source_table()),
                        "synT3": ("y", "syn52963211", source_table())},
                       grants={"synP1": "CA1"})
    code, rep = run(tmp_path, fake)
    dirs = sorted(p.name for p in rep.iterdir() if p.is_dir())
    assert dirs == ["Affiliated_Non-Grant_Associated", "CA1", "Project_synP2"]
    assert "Affiliated/Non-Grant Associated" in (rep / "Affiliated_Non-Grant_Associated" / "summary.txt").read_text()
    assert "Project synP2" in (rep / "Project_synP2" / "summary.txt").read_text()


def test_unlisted_grant_can_be_selected_by_its_label(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", source_table()), "synT3": ("y", "syn52963211", source_table())},
                       grants={"synP1": "CA1"})
    code, rep = run(tmp_path, fake, "--grants", "Affiliated/Non-Grant Associated")
    assert [p.name for p in rep.iterdir() if p.is_dir()] == ["Affiliated_Non-Grant_Associated"]


# --- execute ----------------------------------------------------------------------


def test_execute_creates_folder_and_recordset_loads_through_grid_and_logs(tmp_path):
    fake = FakeSynapse()
    code, rep = run(tmp_path, fake, execute=True)
    assert fake.created_folders == [("synP1", "publications")]
    kw = fake.created[0]
    assert kw["create_grid"] is False and kw["bind_schema_to_record_set"] is True
    assert kw["record_set_name"] == "MC2Center_PublicationView_RecordSet"
    assert kw["upsert_keys"] == [KEY] and kw["schema_uri"] == "MC2Center-PublicationView-14.0.0"
    assert kw["project_id"] == "synP1" and kw["folder_id"] == fake.folders[("synP1", "publications")]
    rs_id = next(iter(fake.rs))
    assert len(fake.writes) == 1 and len(fake.rs[rs_id]) == 2
    log = rs.CsvWriteLog(tmp_path / "log.csv")._read(rs_id)
    assert log["status"].tolist() == ["pending", "written", "done"]
    assert log["baseVersion"].tolist() == [1, 1, 1]
    assert rs.CsvWriteLog(tmp_path / "log.csv").pipeline_map(rs_id) == {2: 1}
    assert status_of(rep) == "migrated"
    assert (rep / "CA1" / "synapse_validation.csv").exists()
    assert "RecordSet union vs legacy union" in (rep / "comparison.txt").read_text()
    assert "EQUAL" in (rep / "comparison.txt").read_text()


def test_execute_uses_the_schema_version_argument(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, "--schema-version", "14.0.1", execute=True)
    assert fake.created[0]["schema_uri"] == "MC2Center-PublicationView-14.0.1"


def test_execute_verifies_the_read_back_and_reports_a_mismatch(tmp_path):
    fake = FakeSynapse()
    real_write = fake.write

    def corrupting(syn, rs_id, csv_path, on_version=None, **kw):
        out = real_write(syn, rs_id, csv_path, on_version=on_version, **kw)
        fake.rs[rs_id][-1].loc[0, "PublicationTitle"] = "Changed by Grid"
        return out

    fake.write = corrupting
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "verification_mismatch" and code == 1
    assert "Changed by Grid" in (rep / "CA1" / "verification.txt").read_text()


def test_execute_read_back_accepts_grids_json_array_lists(tmp_path):
    fake = FakeSynapse()
    code, rep = run(tmp_path, fake, execute=True)
    rs_id = next(iter(fake.rs))
    stored = fake.rs[rs_id][-1].set_index(KEY)
    assert stored.loc["p1", "Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert not (rep / "CA1" / "verification.txt").exists()


def seed_existing(fake, rs_id="synRSX", logged=False, log_path=None):
    folder = "synFX"
    fake.folders[("synP1", "publications")] = folder
    fake.rs_folder[folder] = rs_id
    fake.rs[rs_id] = [pd.DataFrame(columns=[KEY]), pd.DataFrame({KEY: ["p1"]})]
    if logged:
        log = rs.CsvWriteLog(log_path)
        log.start(rs_id, 1, "old", "42")
        log.record(rs_id, 2, "old", 1, base=1, principal="42")
        log.finish(rs_id, 1, "old", "42")


def test_execute_skips_a_logged_existing_recordset(tmp_path):
    fake = FakeSynapse()
    seed_existing(fake, logged=True, log_path=tmp_path / "log.csv")
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "already_migrated" and fake.writes == [] and fake.created == []
    assert len(fake.rs["synRSX"]) == 2


def test_execute_fails_an_unlogged_existing_recordset_and_never_overwrites(tmp_path):
    fake = FakeSynapse()
    seed_existing(fake, logged=False)
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "failed" and code == 1
    assert "exists but not migrated; investigate" in (rep / "CA1" / "summary.txt").read_text()
    assert fake.writes == [] and fake.created == []


def test_execute_aborts_the_pending_row_when_the_write_fails(tmp_path):
    fake = FakeSynapse()

    def boom(syn, rs_id, csv_path, on_version=None, **kw):
        raise RuntimeError("grid down")

    fake.write = boom
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "failed" and "grid down" in (rep / "CA1" / "summary.txt").read_text()
    rs_id = next(iter(fake.rs))
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    assert log._read(rs_id)["status"].tolist() == ["pending", "aborted"]
    assert log.unresolved_pending(rs_id) == []


def test_execute_needs_a_write_log_table(tmp_path):
    with pytest.raises(SystemExit):
        mig.get_args(["--component", "PublicationView", "--execute"])


def test_redo_is_refused_with_execute():
    with pytest.raises(SystemExit):
        mig.get_args(["--component", "PublicationView", "--execute", "--write-log-table", "synL", "--redo", "CA1"])


# --- resume -----------------------------------------------------------------------


def test_resume_skips_done_grants_and_redo_repeats_one(tmp_path):
    tables = {"synT1": ("CA1", "synP1", source_table()), "synT2": ("CA2", "synP2", source_table())}
    fake = FakeSynapse(tables)
    run(tmp_path, fake, run_id="r1")
    reads = []
    real = fake.read_source
    fake.read_source = lambda syn, t: (reads.append(t), real(syn, t))[1]
    code, rep = run(tmp_path, fake, run_id="r2")
    assert reads == [] and "skipped" in (rep / "summary.csv").read_text()
    assert "EQUAL" in (rep / "comparison.txt").read_text()
    code, rep = run(tmp_path, fake, "--redo", "CA2", run_id="r3")
    assert reads == ["synT2"]
    state = mig.load_state(tmp_path / "out" / "state.json")
    assert state["grants"]["CA1"]["run_id"] == "r1" and state["grants"]["CA2"]["run_id"] == "r3"


def test_resume_does_not_skip_a_failed_grant(tmp_path):
    bad = source_table().rename(columns={"Publication Title": "Mystery Column"})
    fake = FakeSynapse({"synT1": ("CA1", "synP1", bad)})
    run(tmp_path, fake, run_id="r1")
    fake.tables["synT1"] = ("CA1", "synP1", source_table())
    code, rep = run(tmp_path, fake, run_id="r2")
    assert status_of(rep) == "dryrun"


def test_a_dry_run_does_not_make_the_execute_skip(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake, run_id="r1")
    code, rep = run(tmp_path, fake, run_id="r2", execute=True)
    assert status_of(rep) == "migrated"


# --- comparison -------------------------------------------------------------------


def test_compare_keys_equal_and_unequal():
    ok, lines = mig.compare_keys("x", ["a", "b"], ["b", "a"], "legacy")
    assert ok and lines[-1].strip() == "EQUAL"
    ok, lines = mig.compare_keys("x", ["a", "b"], ["a"], "legacy")
    assert not ok and any("missing" in line and "'b'" in line for line in lines)
    ok, lines = mig.compare_keys("x", ["a"], ["a", "c"], "legacy")
    assert not ok and any("extra" in line for line in lines)
    ok, _ = mig.compare_keys("x", ["a", "a"], ["a"], "legacy")  # same keys, different row counts
    assert not ok


def test_execute_comparison_that_differs_exits_1(tmp_path):
    fake = FakeSynapse()
    real_union = fake.union
    fake.union = lambda grants: (lambda u, s: (u.iloc[:1], s))(*real_union(grants))
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and "DIFFERENT" in (rep / "comparison.txt").read_text()


def test_dry_run_compares_with_the_legacy_view_when_given(tmp_path):
    fake = FakeSynapse()
    view = pd.DataFrame({"PublicationView_id": ["p1", "p2"]})
    table = mock.Mock()
    table.query.return_value = view
    with mock.patch.object(mig, "Table", lambda id: table):
        code, rep = run(tmp_path, fake, "--union-id", "synMV")
    text = (rep / "comparison.txt").read_text()
    assert "legacy view synMV" in text and "EQUAL" in text
    view2 = pd.DataFrame({"PublicationView_id": ["p1", "p9"]})
    table.query.return_value = view2
    with mock.patch.object(mig, "Table", lambda id: table):
        code, rep = run(tmp_path, fake, "--union-id", "synMV", run_id="r2")
    assert code == 1 and "DIFFERENT" in (rep / "comparison.txt").read_text()


# --- schema version ---------------------------------------------------------------


def test_validation_uses_the_schema_at_the_requested_tag(monkeypatch):
    mig.load_schema.cache_clear()
    seen = []
    real = mig.model_contract.fetch
    monkeypatch.setattr(mig.model_contract, "fetch", lambda path, ref=None: (seen.append(ref), real(path))[1])
    mig.load_schema("PublicationView", "v14.0.1")
    assert seen == ["v14.0.1"]
    mig.load_schema.cache_clear()


def test_unavailable_schema_tag_warns_and_falls_back(monkeypatch, caplog):
    mig.load_schema.cache_clear()
    real = mig.model_contract.fetch

    def fetch(path, ref=None):
        if ref:
            raise mig.urllib.error.HTTPError("u", 404, "Not Found", None, None)
        return real(path)

    monkeypatch.setattr(mig.model_contract, "fetch", fetch)
    schema = mig.load_schema("PublicationView", "v99.0.0")
    assert "properties" in schema and "not available" in caplog.text
    mig.load_schema.cache_clear()


# --- worklists, truncated values, validation never blocks -------------------------


def test_validation_worklist_is_combined_sorted_by_field_with_current_values(tmp_path):
    table = source_table()
    table.loc[0, "Publication Accessibility"] = "Embargoed"
    tables = {"synT1": ("CA1", "synP1", table), "synT2": ("CA2", "synP2", source_table())}
    code, rep = run(tmp_path, FakeSynapse(tables))
    work = pd.read_csv(rep / "validation_worklist.csv", dtype=str, keep_default_na=False)
    assert work.columns.tolist() == ["grant", "key", "field", "validator", "message", "current_value"]
    assert work["field"].tolist() == sorted(work["field"].tolist())
    row = work[work["field"] == "PublicationAccessibility"].iloc[0]
    assert (row["grant"], row["key"], row["validator"], row["current_value"]) == ("CA1", "p1", "enum", "Embargoed")
    assert set(work["grant"]) == {"CA1", "CA2"}
    assert code == 2


def test_truncated_values_are_listed_separately(tmp_path):
    table = source_table()
    table.loc[0, "Publication Assay"] = "Sin(Read more on Pubmed), Mass Spectrometry"
    code, rep = run(tmp_path, FakeSynapse({"synT1": ("CA1", "synP1", table)}))
    truncated = pd.read_csv(rep / "truncated.csv", dtype=str)
    assert truncated.to_dict("records") == [
        {"grant": "CA1", "key": "p1", "column": "Assay", "value": "Sin(Read more on Pubmed)"}]
    text = (rep / "run_summary.txt").read_text()
    assert "truncated values (need recovering from source records): 1" in text
    assert "truncated" not in text.split("changes by rule:")[1].splitlines()[0]
    assert pd.read_csv(rep / "CA1" / "unmapped.csv").empty


def test_validation_failures_do_not_block_a_migration(tmp_path):
    fake = FakeSynapse()  # the fixture rows lack required fields
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "migrated" and len(fake.writes) == 1
    assert int((rep / "CA1" / "validation.csv").read_text().count("\n")) > 1
    assert code == 2


def test_comparison_accounts_for_resolved_duplicates(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", dup_table())})
    fake.reference_source = reference_for("Second copy")
    code, rep = run(tmp_path, fake)
    assert "EQUAL" in (rep / "comparison.txt").read_text()
    table = mock.Mock()
    table.query.return_value = pd.DataFrame({"PublicationView_id": ["p1", "p1"]})  # the view holds both rows
    with mock.patch.object(mig, "Table", lambda id: table):
        code, rep = run(tmp_path, fake, "--union-id", "synMV", out="out2")
    assert "legacy view synMV" in (rep / "comparison.txt").read_text()
    assert "EQUAL" in (rep / "comparison.txt").read_text()


def test_view_comparison_covers_only_the_tables_in_the_views_scope(tmp_path):
    tables = {"synT1": ("CA1", "synP1", source_table()), "synT2": ("CA2", "synP2", source_table(
        [["PublicationView", "new1", "CA123456", "N", "", "", "", "", "", ""]]))}
    fake = FakeSynapse(tables)
    table = mock.Mock()
    table.query.return_value = pd.DataFrame({"PublicationView_id": ["p1", "p2"]})  # built before synT2 existed
    with mock.patch.object(mig, "Table", lambda id: table), \
         mock.patch.object(mig, "view_scope", lambda syn, view: {"synT1"}):
        code, rep = run(tmp_path, fake, "--union-id", "synMV")
    text = (rep / "comparison.txt").read_text()
    assert "not in the view's scope" in text and "'CA2'" in text and "EQUAL" in text


# --- duplicate tie-break against the live portal row -------------------------------

DS_HEADERS = ["DatasetView_id", "GrantView Key", "Dataset Name", "Dataset Alias", "Dataset Description",
              "Dataset Design", "Dataset File Formats", "Dataset Doi", "Dataset Url", "Id"]
PORTAL_COLUMNS = ["datasetId", "datasetName", "datasetAlias", "description", "overallDesign", "fileFormats",
                  "doi", "externalLink", "theme"]


def ds_row(key, desc, design, formats, doi, url, legacy_id, alias="alias"):
    return [key, "CA123456", "Name", alias, desc, design, formats, doi, url, legacy_id]


def ds_convert(rows, portal_rows=None, reference=None):
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    table = pd.DataFrame(rows, columns=DS_HEADERS)
    portal = None if portal_rows is None else pd.DataFrame(portal_rows, columns=PORTAL_COLUMNS)
    return mig.convert_table(table, "DatasetView", norm, lambda: reference, lambda: portal)


def test_portal_row_decides_where_the_manifest_holds_both(tmp_path):
    # syn77037247: the two rows differ in description, design, DOI and file formats;
    # the portal row is the e2fa65c2 one (the text tie-break alone would keep e9e03dd9)
    rows = [
        ds_row("syn77037247", "Desc A", "Design A", "FASTQ", "10.1000/a", "https://x/1", "e2fa65c2-22e7"),
        ds_row("syn77037247", "Desc B", "Design B", "SRA", "10.1000/b", "https://x/1", "e9e03dd9-a181"),
    ]
    portal = [["syn77037247", "Name", "alias", "Desc A", "Design A", "FASTQ", "https://doi.org/10.1000/a", "", ""]]
    converted, _, _, _, res = ds_convert(rows, portal)
    assert converted["DatasetDescription"].tolist() == ["Desc A"]
    assert res.iloc[0]["kept_row"] == "row 1 (Id e2fa65c2-22e7)"
    assert "matches more portal cells" in res.iloc[0]["reason"]
    assert "arbitrary" not in res.iloc[0]["reason"]


def test_portal_tie_on_the_main_fields_is_decided_on_the_remaining_mapped_columns():
    # syn77038892: description, design, DOI and file formats tie; the alias column differs
    rows = [
        ds_row("syn77038892", "Desc", "Design", "FASTQ", "10.1000/a", "https://x", "6ad2b0e3-89d0", alias="old alias"),
        ds_row("syn77038892", "Desc", "Design", "FASTQ", "10.1000/a", "https://x", "5620faa3-a924", alias="new alias"),
    ]
    portal_rows = [["syn77038892", "Name", "new alias", "Desc", "Design", "FASTQ", "https://doi.org/10.1000/a", "", ""]]
    converted, _, _, _, res = ds_convert(rows, portal_rows)
    assert converted["DatasetAlias"].tolist() == ["new alias"]
    assert res.iloc[0]["kept_row"] == "row 2 (Id 5620faa3-a924)"
    assert "matches more portal cells" in res.iloc[0]["reason"]


def test_portal_full_tie_falls_through_to_completeness_then_an_arbitrary_id_choice():
    rows = [
        ds_row("k", "Desc", "Design", "FASTQ", "10.1000/a", "https://x", "aaa"),
        ds_row("k", "Desc", "Design", "FASTQ", "10.1000/a", "https://x", "bbb"),
    ]
    portal_rows = [["k", "Name", "alias", "Desc", "Design", "FASTQ", "https://doi.org/10.1000/a", "https://x", ""]]
    converted, _, _, _, res = ds_convert(rows, portal_rows)
    reason = res.iloc[0]["reason"]
    assert "tie on portal cells" in reason and "tie on completeness" in reason and "arbitrary" in reason
    assert res.iloc[0]["kept_row"] == "row 2 (Id bbb)"


def test_portal_decides_before_completeness():
    rows = [
        ds_row("k", "Desc", "", "FASTQ", "10.1000/a", "https://x", "1"),   # less complete, matches the portal
        ds_row("k", "Other", "Design", "FASTQ", "10.1000/a", "https://x", "2"),
    ]
    portal_rows = [["k", "Name", "alias", "Desc", "", "FASTQ", "https://doi.org/10.1000/a", "https://x", ""]]
    converted, _, _, _, res = ds_convert(rows, portal_rows)
    assert converted["DatasetDescription"].tolist() == ["Desc"]


def test_manifest_still_decides_first_when_exactly_one_row_matches():
    rows = [
        ds_row("k", "Desc A", "Design", "FASTQ", "10.1000/a", "https://x", "1"),
        ds_row("k", "Desc B", "Design", "FASTQ", "10.1000/a", "https://x", "2"),
    ]
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    ref = mig.reference_frame(pd.DataFrame([rows[1][:-1]], columns=DS_HEADERS[:-1]), "DatasetView", norm)
    portal_rows = [["k", "Name", "alias", "Desc A", "Design", "FASTQ", "https://doi.org/10.1000/a", "https://x", ""]]
    converted, _, _, _, res = ds_convert(rows, portal_rows, ref)
    assert converted["DatasetDescription"].tolist() == ["Desc B"] and res.iloc[0]["reason"] == "matches the merged manifest"


def test_portal_is_read_once_per_run_and_only_with_a_duplicate(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", dup_table()), "synT2": ("CA2", "synP2", dup_table())})
    fake.portal_source = pd.DataFrame({"pubMedId": ["p1"], "publicationTitle": ["Second copy"]})
    run(tmp_path, fake)
    assert fake.portal_reads == 1
    fake = FakeSynapse({"synT3": ("CA3", "synP3", source_table())})
    run(tmp_path, fake, out="out2")
    assert getattr(fake, "portal_reads", 0) == 0


def test_publication_duplicates_are_matched_to_the_portal_by_pubmed_id():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    portal = pd.DataFrame({"pubMedId": ["p1"], "publicationTitle": ["Second copy"]})
    converted, _, _, _, res = mig.convert_table(
        dup_table(first_id="9", second_id="10"), "PublicationView", norm, lambda: None, lambda: portal)
    assert converted["PublicationTitle"].tolist() == ["Second copy"] and "matches more portal cells" in res.iloc[0]["reason"]
