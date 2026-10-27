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
        self.pubmed_authors = {}      # PMID -> author list PubMed returns
        self.pubmed_calls = []
        self.pubmed_failed = set()    # PMIDs whose request fails

    def fetch_authors(self, pmids, cache_dir=None, failed=None, **kw):
        self.pubmed_calls.append((list(pmids), cache_dir))
        if failed is not None:
            failed.update(p for p in pmids if p in self.pubmed_failed)
        return {p: self.pubmed_authors[p] for p in pmids if p in self.pubmed_authors}

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
         mock.patch.object(mig.pubmed, "fetch_authors", fake.fetch_authors), \
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


def test_merged_manifest_is_read_once_per_run(tmp_path):
    tables = {"synT1": ("CA1", "synP1", dup_table()), "synT2": ("CA2", "synP2", dup_table()),
              "synT3": ("CA3", "synP3", source_table())}
    fake = FakeSynapse(tables)
    fake.reference_source = reference_for("Second copy")
    run(tmp_path, fake)
    assert fake.reference_reads == 1
    fake = FakeSynapse({"synT3": ("CA3", "synP3", source_table())})
    run(tmp_path, fake, out="out2")
    assert fake.reference_reads == 1  # the manifest differences need it even without a duplicate


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
    assert log["status"].tolist() == ["created", "pending", "written", "done", "verified"]
    assert log["baseVersion"].tolist() == [1, 1, 1, 1, 1]
    assert log["version"].dropna().astype(int).tolist() == [1, 2, 2]  # created v1, written v2, verified v2
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


def seed_existing(fake, rs_id="synRSX", logged=False, verified=False, log_path=None):
    folder = "synFX"
    fake.folders[("synP1", "publications")] = folder
    fake.rs_folder[folder] = rs_id
    fake.rs[rs_id] = [pd.DataFrame(columns=[KEY]), pd.DataFrame({KEY: ["p1"]})]
    if logged:
        log = rs.CsvWriteLog(log_path)
        log.start(rs_id, 1, "old", "42")
        log.record(rs_id, 2, "old", 1, base=1, principal="42")
        log.finish(rs_id, 1, "old", "42")
    if verified:
        rs.CsvWriteLog(log_path).verified(rs_id, 2, 1, "old", "42")


def test_execute_skips_a_logged_and_verified_existing_recordset(tmp_path):
    fake = FakeSynapse()
    seed_existing(fake, logged=True, verified=True, log_path=tmp_path / "log.csv")
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "already_migrated" and fake.writes == [] and fake.created == []
    assert len(fake.rs["synRSX"]) == 2


def test_execute_verifies_a_logged_but_unverified_load_instead_of_skipping(tmp_path):
    fake = FakeSynapse()
    seed_existing(fake, logged=True, log_path=tmp_path / "log.csv")  # content p1 only: not what converted holds
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "verification_mismatch" and code == 1 and fake.writes == []
    assert (rep / "CA1" / "verification.txt").exists()
    assert rs.CsvWriteLog(tmp_path / "log.csv").verified_versions("synRSX") == set()


def test_a_run_that_failed_after_the_export_is_verified_not_skipped_on_rerun(tmp_path):
    fake = FakeSynapse()
    real_read = fake.read
    state = {"fail": True}

    def flaky(syn, rs_id, version=None, cache_dir=None, list_cols=()):
        if state["fail"]:
            raise RuntimeError("read-back failed")
        return real_read(syn, rs_id, version, cache_dir, list_cols)

    fake.read = flaky
    code, rep = run(tmp_path, fake, execute=True, run_id="r1")
    assert status_of(rep) == "failed" and len(fake.writes) == 1
    rs_id = next(iter(fake.rs))
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    assert log.pipeline_map(rs_id) == {2: 1} and log.verified_versions(rs_id) == set()
    state["fail"] = False
    code, rep = run(tmp_path, fake, execute=True, run_id="r2")
    assert status_of(rep) == "migrated" and "never verified" in (rep / "CA1" / "summary.txt").read_text()
    assert len(fake.writes) == 1 and len(fake.created) == 1  # verified, not rewritten or recreated
    assert log.verified_versions(rs_id) == {2}
    code, rep = run(tmp_path, fake, execute=True, run_id="r3", out="out3")
    assert status_of(rep) == "already_migrated"


def test_verified_and_created_rows_are_neither_versions_nor_pending_closers(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.created("synR", 1, "r1", "42")
    log.start("synR", 1, "r1", "42")
    log.verified("synR", 2, 1, "r1", "42")
    assert log.pipeline_map("synR") == {} and log.last_version("synR") is None
    assert [p["runId"] for p in log.unresolved_pending("synR")] == ["r1"]  # not closed by verified
    assert log.locked_by("synR", "other") == "r1"
    assert log.created_version("synR") == 1 and log.verified_versions("synR") == {2}


def test_an_empty_recordset_left_after_a_failed_load_is_loaded_on_rerun(tmp_path):
    fake = FakeSynapse()
    real_write = fake.write

    def boom(syn, rs_id, csv_path, on_version=None, **kw):
        raise RuntimeError("grid down")

    fake.write = boom
    code, rep = run(tmp_path, fake, execute=True, run_id="r1")
    assert status_of(rep) == "failed"
    fake.write = real_write
    code, rep = run(tmp_path, fake, execute=True, run_id="r2")
    assert status_of(rep) == "migrated" and len(fake.created) == 1 and len(fake.writes) == 1
    assert "left empty at v1" in (rep / "CA1" / "summary.txt").read_text()
    rs_id = next(iter(fake.rs))
    assert rs.CsvWriteLog(tmp_path / "log.csv").verified_versions(rs_id) == {2}


def test_a_newer_unlogged_version_after_creation_is_not_loaded_over(tmp_path):
    fake = FakeSynapse()
    real_write = fake.write

    def boom(syn, rs_id, csv_path, on_version=None, **kw):
        raise RuntimeError("grid down")

    fake.write = boom
    run(tmp_path, fake, execute=True, run_id="r1")
    rs_id = next(iter(fake.rs))
    fake.rs[rs_id].append(pd.DataFrame({KEY: ["someone else"]}))  # a contributor wrote v2
    fake.write = real_write
    code, rep = run(tmp_path, fake, execute=True, run_id="r2")
    assert status_of(rep) == "failed" and fake.writes == []
    assert "exists but not migrated; investigate" in (rep / "CA1" / "summary.txt").read_text()


def test_retry_message_on_the_migration_path_says_rerun_the_migration(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    csv = tmp_path / "c.csv"
    csv.write_text("a\n1\n")
    with mock.patch.object(rs, "recordset_info", lambda syn, rs_id: (3, [KEY])):
        with pytest.raises(rs.WriteRefused) as err:
            rs.write_logged(object(), "synR", csv, log, "r1", "42", 1, 1, retry_note="rerun the migration (the RecordSet stays empty)")
    assert err.value.status == "retry" and "rerun the migration" in err.value.message
    assert "next run replays" not in err.value.message


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
    assert log._read(rs_id)["status"].tolist() == ["created", "pending", "aborted"]
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
        {"grant": "CA1", "key": "p1", "column": "Assay", "value": "Sin(Read more on Pubmed)", "rule": "truncated"}]
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


# --- recovering truncated authors from PubMed ---------------------------------------


def truncated_authors_table():
    headers = HEADERS + ["Pubmed Id", "Publication Authors"]
    rows = [
        ["PublicationView", "p1", "CA123456", "First", "10.1038/abc", "", "", "Open Access", "1", "syn1", "111",
         "Nathan A Charles, Eric C Holland, Gilbe(Read more on Pubmed)"],
        ["PublicationView", "p2", "CA123456", "Second", "", "", "", "Open Access", "2", "syn2", "222",
         "Kwak M, Han(Read more on Pubmed)"],
        ["PublicationView", "p3", "CA123456", "Third", "", "", "", "Open Access", "3", "syn3", "333",
         "Fine A, Fine B"],
    ]
    return pd.DataFrame(rows, columns=headers)


def test_truncated_authors_are_recovered_from_pubmed_by_pubmed_id(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", truncated_authors_table())})
    fake.pubmed_authors = {"111": ["Nathan A  Charles", "eric c holland", "Gilbertson R"]}  # case/space-insensitive guard
    code, rep = run(tmp_path, fake)
    assert fake.pubmed_calls[0][0] == ["111", "222"] and fake.pubmed_calls[0][1] == str(tmp_path / "cache")
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str, keep_default_na=False).set_index(KEY)
    assert converted.loc["p1", "PublicationAuthors"] == '["Nathan A  Charles","eric c holland","Gilbertson R"]'
    assert "Read more" in converted.loc["p2", "PublicationAuthors"]  # PubMed returned nothing: stays
    recovered = pd.read_csv(rep / "CA1" / "recovered.csv", dtype=str, keep_default_na=False)
    assert recovered.columns.tolist() == ["key", "old", "new", "pubmed_id", "source", "reason"]
    ok = recovered[recovered["key"] == "p1"].iloc[0]
    assert (ok["old"], ok["source"], ok["reason"]) == (
        "Nathan A Charles, Eric C Holland, Gilbe(Read more on Pubmed)", "pubmed_efetch", "")
    assert recovered[recovered["key"] == "p2"].iloc[0]["reason"] == "PMID not returned by PubMed"
    changes = pd.read_csv(rep / "CA1" / "changes.csv", dtype=str, keep_default_na=False)
    assert changes[changes["rule"] == "recovered_pubmed"]["key"].tolist() == ["p1"]
    truncated = pd.read_csv(rep / "truncated.csv", dtype=str)
    assert truncated["key"].tolist() == ["p2"]  # p1 is recovered, p2 is still truncated
    summary = (rep / "CA1" / "summary.txt").read_text()
    assert "authors_recovered=1" in summary and "truncated=1" in summary
    assert "authors recovered from PubMed: 1" in (rep / "run_summary.txt").read_text()
    fixes = pd.read_csv(rep / "portal_author_fixes.csv", dtype=str)
    assert fixes.to_dict("records") == [{"pubMedId": "111", "authors": "Nathan A  Charles, eric c holland, Gilbertson R",
                                         "source": "pubmed_efetch"}]


def test_a_recovered_list_that_disagrees_with_the_leading_items_is_not_applied(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", truncated_authors_table())})
    fake.pubmed_authors = {"111": ["Nathan A Charles", "Someone Else", "Gilbertson R"],  # 2nd leading name differs
                           "222": ["Mincheol Kwak"]}                                     # shorter than the cut point
    code, rep = run(tmp_path, fake)
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str, keep_default_na=False).set_index(KEY)
    assert "Read more" in converted.loc["p1", "PublicationAuthors"] and "Read more" in converted.loc["p2", "PublicationAuthors"]
    recovered = pd.read_csv(rep / "CA1" / "recovered.csv", dtype=str, keep_default_na=False).set_index("key")
    assert recovered.loc["p1", "reason"].startswith("recovery_mismatch") and recovered.loc["p1", "new"].endswith("Gilbertson R")
    assert recovered.loc["p2", "reason"].startswith("recovery_mismatch")
    changes = pd.read_csv(rep / "CA1" / "changes.csv", dtype=str, keep_default_na=False)
    assert sorted(changes[changes["rule"] == "recovery_mismatch"]["key"]) == ["p1", "p2"]
    assert "recovered_pubmed" not in changes["rule"].tolist()
    truncated = pd.read_csv(rep / "truncated.csv", dtype=str)
    assert sorted(truncated["key"]) == ["p1", "p2"] and set(truncated["rule"]) == {"recovery_mismatch"}
    assert pd.read_csv(rep / "portal_author_fixes.csv").empty


def test_pubmed_unavailable_leaves_the_cell_truncated_with_that_reason(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", truncated_authors_table())})
    fake.pubmed_failed = {"111", "222"}
    code, rep = run(tmp_path, fake)
    recovered = pd.read_csv(rep / "CA1" / "recovered.csv", dtype=str, keep_default_na=False)
    assert recovered["reason"].tolist() == ["PubMed unavailable", "PubMed unavailable"]
    changes = pd.read_csv(rep / "CA1" / "changes.csv", dtype=str, keep_default_na=False)
    assert changes[changes["rule"] == "truncated"]["key"].tolist() == ["p1", "p2"]
    assert "authors recovered from PubMed: 0" in (rep / "run_summary.txt").read_text()


def test_recovery_can_be_switched_off_and_is_off_for_other_components(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", truncated_authors_table())})
    fake.pubmed_authors = {"111": ["A B"]}
    code, rep = run(tmp_path, fake, "--no-recover-truncated")
    assert fake.pubmed_calls == []
    assert not (rep / "CA1" / "recovered.csv").exists()
    assert mig.get_args(["--component", "DatasetView"]).recover_truncated is False
    assert mig.get_args(["--component", "PublicationView"]).recover_truncated is True


def test_pubmed_id_falls_back_to_the_key_when_blank(tmp_path):
    table = truncated_authors_table()
    table.loc[0, "Pubmed Id"] = ""
    table.loc[0, "PublicationView_id"] = "777"
    fake = FakeSynapse({"synT1": ("CA1", "synP1", table)})
    fake.pubmed_authors = {"777": ["X Y"]}
    code, rep = run(tmp_path, fake)
    recovered = pd.read_csv(rep / "CA1" / "recovered.csv", dtype=str)
    assert recovered["pubmed_id"].tolist() == ["777", "222"]


def test_nothing_is_fetched_when_no_author_is_truncated(tmp_path):
    fake = FakeSynapse()
    run(tmp_path, fake)
    assert fake.pubmed_calls == []


# --- owner choices among duplicate rows ---------------------------------------------


def choice_convert(choices, **kw):
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    rows = [
        ds_row("syn1", "Desc A", "Design", "FASTQ", "10.1000/a", "https://x", "e2fa65c2-22e7"),
        ds_row("syn1", "Desc B", "Design", "FASTQ", "10.1000/a", "https://x", "e9e03dd9-a181"),
    ]
    portal = pd.DataFrame([["syn1", "Name", "alias", "Desc A", "Design", "FASTQ", "https://doi.org/10.1000/a", "", ""]],
                          columns=PORTAL_COLUMNS)
    return mig.convert_table(pd.DataFrame(rows, columns=DS_HEADERS), "DatasetView", norm, lambda: None,
                             lambda: portal, choices)


def test_owner_choice_decides_before_the_portal_and_every_other_rule():
    converted, _, _, _, res = choice_convert({"syn1": "e9e03dd9-a181"})  # the portal would keep the other row
    assert converted["DatasetDescription"].tolist() == ["Desc B"]
    assert res.to_dict("records") == [{"key": "syn1", "kept_row": "row 2 (Id e9e03dd9-a181)",
                                       "dropped_row": "row 1 (Id e2fa65c2-22e7)", "reason": "owner choice"}]


def test_owner_choice_accepts_an_unambiguous_id_prefix():
    converted, _, _, _, res = choice_convert({"syn1": "e9e03dd9"})
    assert converted["DatasetDescription"].tolist() == ["Desc B"] and res.iloc[0]["reason"] == "owner choice"


def test_owner_choice_naming_an_id_that_is_not_a_duplicate_row_fails_clearly():
    with pytest.raises(mig.ConversionError, match=r"duplicate choice for key syn1: legacy Id 'nope' is not among the duplicate rows"):
        choice_convert({"syn1": "nope"})


def test_choices_for_other_keys_are_ignored():
    converted, _, _, _, res = choice_convert({"other": "zzz"})
    assert converted["DatasetDescription"].tolist() == ["Desc A"] and "owner choice" not in res.iloc[0]["reason"]


def test_duplicate_choices_csv_fails_only_that_grant(tmp_path):
    choices = tmp_path / "c.csv"
    pd.DataFrame({"key": ["p1"], "keep_legacy_id": ["10"]}).to_csv(choices, index=False)
    tables = {"synT1": ("CA1", "synP1", dup_table(first_id="10", second_id="11")),
              "synT2": ("CA2", "synP2", dup_table(first_id="20", second_id="21"))}
    code, rep = run(tmp_path, FakeSynapse(tables), "--duplicate-choices", str(choices))
    assert status_of(rep, "CA1") == "dryrun" and status_of(rep, "CA2") == "failed" and code == 1
    assert pd.read_csv(rep / "CA1" / "duplicates_resolved.csv")["reason"].tolist() == ["owner choice"]
    assert "legacy Id '10' is not among the duplicate rows" in (rep / "CA2" / "summary.txt").read_text()


def test_duplicate_choices_csv_needs_its_columns(tmp_path):
    path = tmp_path / "c.csv"
    pd.DataFrame({"key": ["p1"]}).to_csv(path, index=False)
    with pytest.raises(ValueError, match="lacks"):
        mig.load_duplicate_choices(path)


# --- model ref ------------------------------------------------------------------------


def unavailable_tag(monkeypatch):
    real = mig.model_contract.fetch

    def fetch(path, ref=None):
        if ref == "v99.0.0":
            raise mig.urllib.error.HTTPError("u", 404, "Not Found", None, None)
        return real(path, ref=ref)

    monkeypatch.setattr(mig.model_contract, "fetch", fetch)
    mig.load_schema.cache_clear()


def test_unavailable_schema_tag_fails_an_execute_but_only_warns_in_a_dry_run(tmp_path, monkeypatch, caplog):
    unavailable_tag(monkeypatch)
    with pytest.raises(SystemExit, match="v99.0.0 is not available"):
        run(tmp_path, FakeSynapse(), "--schema-version", "99.0.0", execute=True)
    code, rep = run(tmp_path, FakeSynapse(), "--schema-version", "99.0.0", out="out2")
    assert status_of(rep) == "dryrun" and "v99.0.0 is not available" in caplog.text
    mig.load_schema.cache_clear()


def test_enum_maps_and_validation_read_the_model_at_the_schema_version_tag(tmp_path, monkeypatch):
    seen = []
    real = mig.model_contract.fetch
    monkeypatch.setattr(mig.model_contract, "fetch", lambda path, ref=None: (seen.append((path, ref)), real(path, ref=ref))[1])
    monkeypatch.setattr(mig.value_maps.model_contract, "fetch", mig.model_contract.fetch)
    mig.load_schema.cache_clear()
    run(tmp_path, FakeSynapse(), "--schema-version", "14.0.1")
    assert ("all_valid_values.csv", "v14.0.1") in seen and ("json_schemas/PublicationView.json", "v14.0.1") in seen
    mig.load_schema.cache_clear()


def test_rejected_synonyms_report_is_written_once_per_run(tmp_path):
    code, rep = run(tmp_path, FakeSynapse())
    rejected = pd.read_csv(rep / "rejected_synonyms.csv", dtype=str)
    assert rejected.columns.tolist() == ["column", "valid_value", "fragment", "cell", "reason"]
    assert not rejected.duplicated().any()


def test_portal_comparison_uses_raw_cells_and_set_like_lists():
    # raw 13.1.0 cells: the Assay list differs only in order from the portal's (an enum list compares as a set)
    headers = ["DatasetView_id", "GrantView Key", "Dataset Name", "Dataset Assay", "Id"]
    rows = [["k", "CA123456", "Name", "Mass Spectrometry", "1"],
            ["k", "CA123456", "Name", "rna-seq, Mass Spectrometry", "2"]]
    portal = pd.DataFrame([["k", "Name", ["Mass Spectrometry", "rna-seq"]]], columns=["datasetId", "datasetName", "assay"])
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    converted, _, _, _, res = mig.convert_table(pd.DataFrame(rows, columns=headers), "DatasetView", norm,
                                                lambda: None, lambda: portal)
    assert converted["Assay"].tolist() == [["RNA Sequencing", "Mass Spectrometry"]]  # row 2: portal's raw set equal
    assert "matches more portal cells (2 vs 1 of 2)" in res.iloc[0]["reason"]


def test_portal_comparison_is_on_raw_values_not_normalized_ones():
    # the portal holds the raw term; the candidate whose raw cell equals it wins although both normalize alike
    headers = ["DatasetView_id", "GrantView Key", "Dataset Name", "Dataset Doi", "Id"]
    rows = [["k", "CA123456", "Name", "https://doi.org/10.1000/a", "1"], ["k", "CA123456", "Name", "10.1000/a", "2"]]
    portal = pd.DataFrame([["k", "Name", "10.1000/a"]], columns=["datasetId", "datasetName", "doi"])
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    _, _, _, _, res = mig.convert_table(pd.DataFrame(rows, columns=headers), "DatasetView", norm, lambda: None, lambda: portal)
    assert res.iloc[0]["kept_row"] == "row 2 (Id 2)"


# --- review nits --------------------------------------------------------------------


def test_resume_skips_a_done_grant_only_when_the_effective_options_match(tmp_path):
    ov = tmp_path / "o.csv"
    pd.DataFrame({"column": ["Publication Accessibility"], "current_value": ["open access"],
                  "proposed_value": ["Restricted Access"]}).to_csv(ov, index=False)
    fake = FakeSynapse()
    run(tmp_path, fake, "--overrides", str(ov), run_id="r1")
    reads = []
    real = fake.read_source
    fake.read_source = lambda syn, t: (reads.append(t), real(syn, t))[1]
    run(tmp_path, fake, "--overrides", str(ov), run_id="r2")
    assert reads == []  # same options: skipped
    pd.DataFrame({"column": ["Publication Accessibility"], "current_value": ["open access"],
                  "proposed_value": ["Open Access"]}).to_csv(ov, index=False)
    code, rep = run(tmp_path, fake, "--overrides", str(ov), run_id="r3")
    assert reads == ["synT1"]  # overrides content changed: redone
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str, keep_default_na=False)
    assert converted["PublicationAccessibility"].tolist() == ["Open Access", "Open Access"]
    run(tmp_path, fake, "--overrides", str(ov), "--schema-version", "14.0.1", run_id="r4")
    assert reads == ["synT1", "synT1"]  # schema version changed
    run(tmp_path, fake, "--overrides", str(ov), "--schema-version", "14.0.1", "--no-recover-truncated", run_id="r5")
    assert reads == ["synT1"] * 3
    run(tmp_path, fake, "--overrides", str(ov), "--schema-version", "14.0.1", "--no-recover-truncated",
        "--no-include-unlisted", run_id="r6")
    assert reads == ["synT1"] * 4


def test_options_hash_covers_the_duplicate_choices_content(tmp_path):
    choices = tmp_path / "c.csv"
    pd.DataFrame({"key": ["p1"], "keep_legacy_id": ["10"]}).to_csv(choices, index=False)
    a = mig.options_hash(mig.get_args(["--component", "PublicationView", "--duplicate-choices", str(choices)]))
    pd.DataFrame({"key": ["p1"], "keep_legacy_id": ["11"]}).to_csv(choices, index=False)
    b = mig.options_hash(mig.get_args(["--component", "PublicationView", "--duplicate-choices", str(choices)]))
    assert a != b


def test_duplicate_choice_with_a_grant_column_applies_only_to_that_grant(tmp_path):
    choices = tmp_path / "c.csv"
    pd.DataFrame({"grant": ["CA2", ""], "key": ["p1", "zzz"], "keep_legacy_id": ["20", "none"]}).to_csv(choices, index=False)
    tables = {"synT1": ("CA1", "synP1", dup_table(first_id="10", second_id="11")),
              "synT2": ("CA2", "synP2", dup_table(first_id="20", second_id="21"))}
    code, rep = run(tmp_path, FakeSynapse(tables), "--duplicate-choices", str(choices))
    assert status_of(rep, "CA1") == "dryrun" and status_of(rep, "CA2") == "dryrun"  # CA1 ignores CA2's choice
    assert "owner choice" not in (rep / "CA1" / "duplicates_resolved.csv").read_text()
    assert pd.read_csv(rep / "CA2" / "duplicates_resolved.csv")["reason"].tolist() == ["owner choice"]
    assert mig.choices_for({("", "k"): "1", ("CA2", "k"): "2"}, "CA2") == {"k": "2"}
    assert mig.choices_for({("", "k"): "1", ("CA2", "k"): "2"}, "CA1") == {"k": "1"}


def test_compare_keys_compares_multisets():
    ok, lines = mig.compare_keys("x", ["a", "a", "b"], ["a", "b", "b"], "legacy")
    assert not ok and any("'a'" in line and "missing" in line for line in lines) and any("'b'" in line and "extra" in line for line in lines)


def test_a_project_skipped_from_state_still_counts_for_the_one_table_per_project_rule(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", source_table())})
    run(tmp_path, fake, run_id="r1")
    fake.tables["synT9"] = ("CA9", "synP1", source_table())  # a second table in the same project
    fake.grants["synP1"] = "CA1"
    code, rep = run(tmp_path, fake, run_id="r2")
    summary = pd.read_csv(rep / "summary.csv")
    assert summary["status"].tolist().count("failed") == 1 and code == 1


def two_table_project():
    return {"synT1": ("CA1", "synP1", source_table()), "synT2": ("CA2", "synP2", source_table()),
            "synT3": ("CA1", "synP1", source_table())}  # P1's second table is listed last


def test_a_grant_with_two_tables_migrates_neither_and_fails_before_any_read(tmp_path):
    fake = FakeSynapse(two_table_project())
    reads = []
    real = fake.read_source
    fake.read_source = lambda syn, t: (reads.append(t), real(syn, t))[1]
    code, rep = run(tmp_path, fake, "--grants", "CA1")
    assert reads == [] and code == 1  # the scan reached synT3 although CA2 is not wanted
    assert status_of(rep) == "failed"
    message = (rep / "CA1" / "summary.txt").read_text()
    assert "synT1" in message and "synT3" in message and not (rep / "CA1" / "converted.csv").exists()


def test_a_two_table_grant_does_not_stop_the_other_grants(tmp_path):
    fake = FakeSynapse(two_table_project())
    code, rep = run(tmp_path, fake, execute=True)
    assert code == 1 and status_of(rep, "CA1") == "failed" and status_of(rep, "CA2") == "migrated"
    assert len(fake.created) == 1 and fake.created[0]["project_id"] == "synP2"


def test_limit_counts_grants_after_the_full_scan(tmp_path):
    fake = FakeSynapse(two_table_project())
    code, rep = run(tmp_path, fake, "--limit", "1")
    assert status_of(rep, "CA1") == "failed" and not (rep / "CA2").exists()  # synT3 was not cut off by the limit


def recover_one(cell, authors):
    """Run recover_truncated_authors on one row whose author cell is ``cell`` (a list)."""
    converted = pd.DataFrame({KEY: ["p1"], "PubmedId": ["111"], "PublicationAuthors": [cell]})
    changes = pd.DataFrame([{"key": "p1", "column": "PublicationAuthors", "from": cell[-1], "to": cell[-1], "rule": "truncated"}])
    out, changes, recovered = mig.recover_truncated_authors(converted, changes, lambda pmids: ({"111": authors}, set()), KEY)
    return out["PublicationAuthors"].iloc[0], changes["rule"].tolist(), recovered["reason"].iloc[0]


@pytest.mark.parametrize("cell,authors,ok", [
    (["Jo(Read more on Pubmed)"], ["Mary Jones"], False),                           # cut == 0, prefix differs
    (["Jo(Read more on Pubmed)"], ["John Smith"], True),                            # cut == 0, prefix matches
    (["Jo...(Read more on Pubmed)"], ["john smith"], True),                         # trailing dots, case-folded
    (["(Read more on Pubmed)"], ["John Smith"], False),                             # cut == 0, nothing to compare
    (["Ann Lee", "Han(Read more on Pubmed)"], ["Ann Lee", "Bo Chan"], False),       # leading ok, cut prefix not
    (["Ann Lee", "Han(Read more on Pubmed)"], ["Ann Lee", "Han Solo"], True),
    (["Ann Lee", "(Read more on Pubmed)"], ["Ann Lee", "Han Solo"], True),          # empty text, leading names decide
])
def test_recovery_checks_the_cut_off_item_as_a_prefix_of_pubmeds_name(cell, authors, ok):
    cells, rules, reason = recover_one(cell, authors)
    assert (reason == "") is ok
    assert (cells == authors) if ok else (cells == cell and rules == ["recovery_mismatch"])


def test_an_empty_recordset_with_the_wrong_upsert_key_is_not_loaded_on_resume(tmp_path):
    fake = FakeSynapse()
    real_write = fake.write

    def boom(syn, rs_id, csv_path, on_version=None, **kw):
        raise RuntimeError("grid down")

    fake.write = boom
    run(tmp_path, fake, execute=True, run_id="r1")
    fake.write = real_write
    fake.info = lambda syn, rs_id: (len(fake.rs[rs_id]), ["Wrong"])
    code, rep = run(tmp_path, fake, execute=True, run_id="r2")
    assert status_of(rep) == "failed" and code == 1 and fake.writes == []
    assert "upsert key ['Wrong'], expected ['PublicationViewId']" in (rep / "CA1" / "summary.txt").read_text()


def test_an_empty_grant_table_creates_nothing_and_is_not_a_failure(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", source_table(rows=[])), "synT2": ("CA2", "synP2", source_table())})
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep, "CA1") == "empty_source" and status_of(rep, "CA2") == "migrated" and code != 1
    assert [kw["project_id"] for kw in fake.created] == ["synP2"] and len(fake.writes) == 1
    assert "empty source tables (no RecordSet): CA1" in (rep / "run_summary.txt").read_text()
    assert "no rows after conversion" in (rep / "CA1" / "summary.txt").read_text()


def test_a_write_that_exports_no_version_fails_the_grant_instead_of_raising(tmp_path):
    fake = FakeSynapse()
    fake.write = lambda syn, rs_id, csv_path, on_version=None, **kw: []
    code, rep = run(tmp_path, fake, execute=True)
    assert status_of(rep) == "failed" and code == 1
    assert "exported no version" in (rep / "CA1" / "summary.txt").read_text()


def float_key_table():
    table = dup_table()
    table["PublicationView_id"] = [123.0, 123.0]  # a numeric key column: float dtype
    return table


def test_a_float_typed_duplicate_key_is_removed_from_the_source_keys_as_text(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", float_key_table())})
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "dryrun"
    assert mig.load_state(tmp_path / "out" / "state.json")["grants"]["CA1"]["source_keys"] == ["123"]


def test_a_resolved_duplicate_key_missing_from_the_source_keys_is_noted_not_raised(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", dup_table())})
    real = mig.convert_table

    def convert(*a, **kw):
        converted, changes, unmapped, dropped, resolutions = real(*a, **kw)
        a[-1]["dropped_keys"] = ["zzz"]
        return converted, changes, unmapped, dropped, resolutions.assign(key="zzz")

    with mock.patch.object(mig, "convert_table", convert):
        code, rep = run(tmp_path, fake)
    assert status_of(rep) == "dryrun"
    assert "not found among the source keys: ['zzz']" in (rep / "CA1" / "summary.txt").read_text()


def test_options_hash_covers_the_resolved_data_models_ref():
    def h(ref):
        args = mig.get_args(["--component", "PublicationView"])
        args.model_ref = ref
        return mig.options_hash(args)

    assert h("v14.0.1") != h("v14.0.2") and h("v14.0.1") == h("v14.0.1")
    assert h(None) == h(mig.model_contract.MODEL_REF)  # a fallback is the same as naming MODEL_REF


# --- ids from the curated manifest (DatasetView) -------------------------------------

MS_HEADERS = ["DatasetView_id", "GrantView Key", "Dataset Name", "Dataset Alias", "Dataset Description", "Id"]
MS_KEY = "DatasetViewId"


def ms_source(rows):
    return pd.DataFrame(rows, columns=MS_HEADERS)


def ms_reference(rows):
    """A manifest with the dataset manifest's header style (class labels without spaces)."""
    return pd.DataFrame(rows, columns=["DatasetView_id", "GrantViewKey", "DatasetName", "DatasetAlias",
                                       "DatasetDescription"])


def ms_convert(source, ref, grant="CA1", info=None):
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    frame = None if ref is None else mig.reference_frame(ref, "DatasetView", norm)
    return mig.convert_table(source, "DatasetView", norm, lambda: frame, lambda: None, None, grant, info)


def test_the_entity_id_is_taken_from_the_manifest_by_alias():
    info = {}
    source = ms_source([["syn_old", "CA1", "N", "GSE1", "d", "1"], ["syn_same", "CA1", "N", "GSE2", "d", "2"]])
    ref = ms_reference([["syn_new", "CA1", "N", "GSE1", "d"], ["syn_same", "CA1", "N", "GSE2", "d"]])
    converted, changes, _, _, _ = ms_convert(source, ref, info=info)
    assert converted[MS_KEY].tolist() == ["syn_new", "syn_same"]
    manifest = changes[changes["rule"] == "manifest_id"]
    assert manifest.to_dict("records") == [{"key": "syn_new", "column": MS_KEY, "from": "syn_old", "to": "syn_new",
                                            "rule": "manifest_id"}]
    assert info["remapped"] == {"syn_new": "syn_old"}
    assert info["manifest_ids"] == {"remapped": 1, "ambiguous": 0, "unmatched": 0, "collision": 0, "other_grant": 0}


def test_a_comma_repeated_manifest_id_counts_as_one_id():
    source = ms_source([["syn_old", "CA1", "N", "GSE1", "d", "1"]])
    ref = ms_reference([["syn_new,syn_new", "CA1", "N", "GSE1", "d"]])
    converted, *_ = ms_convert(source, ref)
    assert converted[MS_KEY].tolist() == ["syn_new"]


def test_several_manifest_ids_keep_the_grant_value_and_are_reported():
    info = {}
    source = ms_source([["syn_old", "CA1", "N", "GSE1", "d", "1"]])
    ref = ms_reference([["syn_a,syn_b", "CA1", "N", "GSE1", "d"]])
    converted, changes, *_ = ms_convert(source, ref, info=info)
    assert converted[MS_KEY].tolist() == ["syn_old"]
    assert changes["rule"].tolist() == ["manifest_id_ambiguous"]
    assert changes.iloc[0]["to"] == "syn_a, syn_b"
    assert info["manifest_ids"]["ambiguous"] == 1 and info["manifest_ids"]["remapped"] == 0 and info["remapped"] == {}


def test_an_alias_missing_from_the_manifest_keeps_the_grant_value_and_is_counted():
    info = {}
    source = ms_source([["syn_old", "CA1", "N", "GSE9", "d", "1"]])
    converted, changes, *_ = ms_convert(source, ms_reference([["syn_x", "CA1", "N", "GSE1", "d"]]), info=info)
    assert converted[MS_KEY].tolist() == ["syn_old"] and changes.empty
    assert info["manifest_ids"]["unmatched"] == 1


def test_the_grant_picks_among_manifest_rows_of_one_alias():
    source = ms_source([["syn_old", "CA2", "N", "GSE1", "d", "1"]])
    ref = ms_reference([["syn_for1", "CA1", "N", "GSE1", "d"], ["syn_for2", "CA2,CA3", "N", "GSE1", "d"]])
    converted, *_ = ms_convert(source, ref, grant="CA2")
    assert converted[MS_KEY].tolist() == ["syn_for2"]
    info = {}
    ms_convert(source, ref, grant="CA9", info=info)  # no row names this grant: ambiguous would need ids; none -> unmatched
    assert info["manifest_ids"]["unmatched"] == 1 and info["remapped"] == {}


def test_a_remap_that_makes_two_rows_of_one_alias_share_a_key_goes_through_duplicate_handling():
    info = {}
    source = ms_source([["syn_a", "CA1", "N", "GSE1", "d", "1"], ["syn_b", "CA1", "N", "GSE1", "d2", "2"]])
    ref = ms_reference([["syn_new", "CA1", "N", "GSE1", "d2"]])
    converted, changes, _, _, resolutions = ms_convert(source, ref, info=info)
    assert converted[MS_KEY].tolist() == ["syn_new"]
    assert converted["DatasetDescription"].tolist() == ["d2"]  # the row equal to the manifest
    assert resolutions["key"].tolist() == ["syn_new"]
    assert info["dropped_keys"] == ["syn_a"] and info["remapped"] == {"syn_new": "syn_b"}


def test_a_component_without_manifest_ids_is_untouched():
    info = {}
    converted, changes, *_ = mig.convert_table(source_table(), "PublicationView",
                                               mig.value_maps.Normalizer.for_component("PublicationView"), info=info)
    assert info["remapped"] == {} and "manifest_id" not in set(changes["rule"])


def ms_table(old_key="syn_old"):
    return ms_source([[old_key, "CA1", "N", "GSE1", "d", "1"], ["syn_same", "CA1", "N", "GSE2", "d", "2"]])


def test_the_run_compares_with_the_grant_tables_keys_and_reports_the_remaps(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", ms_table())})
    fake.reference_source = ms_reference([["syn_new", "CA1", "N", "GSE1", "d"], ["syn_same", "CA1", "N", "GSE2", "d"]])
    code, rep = run(tmp_path, fake, component="DatasetView")
    comparison = (rep / "comparison.txt").read_text()
    assert "keys remapped to the curated manifest's ids: 1" in comparison
    assert "EQUAL" in comparison and "DIFFERENT" not in comparison  # source keys (syn_old) vs the old keys
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str)
    assert converted[MS_KEY].tolist() == ["syn_new", "syn_same"]
    assert "manifest ids: remapped=1 (of which other grant 0), collisions kept=0, ambiguous=0, no manifest row=0" in (rep / "run_summary.txt").read_text()


def test_the_rules_version_is_part_of_the_resume_hash(tmp_path):
    args = mig.get_args(["--component", "DatasetView"])
    before = mig.options_hash(args)
    with mock.patch.object(mig, "CONVERSION_RULES", "other"):
        assert mig.options_hash(args) != before


# --- worklist of other differences ---------------------------------------------------


def test_differences_list_cells_that_differ_from_the_manifest_row():
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    source = ms_source([["s1", "CA1", "N", "GSE1", "grant text", "1"], ["s2", "CA1", "N", "GSE2", "same", "2"]])
    ref = ms_reference([["s1", "CA1,CA2", "N", "GSE1", "manifest text"], ["s2", "CA1", "N", "GSE2", "  same "]])
    converted, *_ = mig.convert_table(source, "DatasetView", norm, lambda: mig.reference_frame(ref, "DatasetView", norm))
    diffs, missing = mig.manifest_differences(converted, mig.reference_frame(ref, "DatasetView", norm), "DatasetView", "CA1")
    assert diffs.to_dict("records") == [{"grant": "CA1", "key": "s1", "column": "DatasetDescription",
                                         "grant_value": "grant text", "manifest_value": "manifest text"}]
    assert missing == []  # GrantViewKey CA1 is among CA1,CA2: not a difference


def test_differences_skip_columns_the_manifest_lacks_and_report_unknown_keys():
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    converted, *_ = mig.convert_table(ms_source([["s1", "CA1", "N", "GSE1", "d", "1"]]), "DatasetView", norm)
    ref = mig.reference_frame(pd.DataFrame([["s9", "N2"]], columns=["DatasetView_id", "DatasetName"]), "DatasetView", norm)
    diffs, missing = mig.manifest_differences(converted, ref, "DatasetView", "CA1")
    assert diffs.empty and missing == ["s1"]
    assert mig.manifest_differences(converted, None, "DatasetView", "CA1")[0].empty


def test_a_run_writes_manifest_differences_and_an_override_template(tmp_path):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", ms_table("syn_new"))})
    fake.reference_source = ms_reference([["syn_new", "CA1", "N", "GSE1", "manifest text"],
                                          ["syn_same", "CA1", "N", "GSE2", "d"]])
    template = tmp_path / "template.csv"
    code, rep = run(tmp_path, fake, "--write-override-template", str(template), component="DatasetView")
    diffs = pd.read_csv(rep / "manifest_differences.csv", dtype=str, keep_default_na=False)
    assert diffs.to_dict("records") == [{"grant": "CA1", "key": "syn_new", "column": "DatasetDescription",
                                         "grant_value": "d", "manifest_value": "manifest text"}]
    assert (rep / "CA1" / "manifest_differences.csv").exists()
    assert "differences from the curated manifest: 1 (DatasetDescription=1)" in (rep / "run_summary.txt").read_text()
    rows = pd.read_csv(template, dtype=str, keep_default_na=False)
    assert rows.iloc[0]["proposed_value"] == "manifest text" and rows.iloc[0]["accept"] == ""
    assert mig.value_maps.load_overrides(template) == {}
    rows.loc[0, "accept"] = "yes"
    rows.to_csv(template, index=False)
    # the curator's accepted row now applies to its own cell on the next run
    fake2 = FakeSynapse({"synT1": ("CA1", "synP1", ms_table("syn_new"))})
    fake2.reference_source = fake.reference_source
    code, rep2 = run(tmp_path, fake2, "--overrides", str(template), component="DatasetView", out="out2")
    converted = pd.read_csv(rep2 / "CA1" / "converted.csv", dtype=str)
    assert converted["DatasetDescription"].tolist() == ["manifest text", "d"]
    assert (rep2 / "manifest_differences.csv").read_text().strip() == "grant,key,column,grant_value,manifest_value"


def test_reference_frame_reads_class_label_and_spaceless_headers():
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    ref = pd.DataFrame([["syn1", "GSE1", "RNA Sequencing", "Human"]],
                       columns=["DatasetView_id", "DatasetAlias", "DatasetAssay", "DatasetSpecies"])
    frame = mig.reference_frame(ref, "DatasetView", norm)
    assert frame.columns.tolist() == ["DatasetViewId", "DatasetAlias", "Assay", "Species"]
    assert frame.iloc[0]["Assay"] == ["RNA Sequencing"]


# --- placeholders, known differences and the id worklist ----------------------------


def test_a_blank_grant_cell_equals_a_manifest_placeholder():
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    source = ms_source([["s1", "CA1", "N", "GSE1", "", "1"]])
    converted, *_ = mig.convert_table(source, "DatasetView", norm)
    for placeholder in ("Pending Annotation", "Not Applicable"):
        ref = mig.reference_frame(ms_reference([["s1", "CA1", "N", "GSE1", placeholder]]), "DatasetView", norm)
        assert mig.manifest_differences(converted, ref, "DatasetView", "CA1")[0].empty
    ref = mig.reference_frame(ms_reference([["s1", "CA1", "N", "GSE1", "real text"]]), "DatasetView", norm)
    assert len(mig.manifest_differences(converted, ref, "DatasetView", "CA1")[0]) == 1


def test_recovered_authors_and_mangled_pmid_keys_are_counted_not_listed():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    source = pd.DataFrame([["p1", "CA1", "Full A, Full B", "1", "39191725"], ["p2", "CA1", "Full A", "2", "39191725"]],
                          columns=["PublicationView_id", "GrantView Key", "Publication Authors", "Id", "Pubmed Id"])
    converted, *_ = mig.convert_table(source, "PublicationView", norm)
    ref = pd.DataFrame([["p1", "CA1", "Full A(Read more on Pubmed)", "39191725"],
                        ["p2", "CA1", "Full B", "39191725"]],
                       columns=["PublicationView_id", "GrantView Key", "Publication Authors", "Pubmed Id"])
    frame = mig.reference_frame(ref, "PublicationView", norm)
    known = {}
    diffs, _ = mig.manifest_differences(converted, frame, "PublicationView", "CA1", {"p1"}, known)
    assert known == {"authors_recovered": 1}
    assert diffs["key"].tolist() == ["p2"] and diffs["column"].tolist() == ["PublicationAuthors"]
    assert not mig._MANGLED_KEY.match("35121903, 33681822") and not mig._MANGLED_KEY.match("38191555,38191555")


def test_a_mangled_pmid_link_cell_is_counted_not_listed():
    norm = mig.value_maps.Normalizer.for_component("DatasetView")
    source = pd.DataFrame([["s1", "CA1", "39191725", "1"], ["s2", "CA1", "39191725", "2"]],
                          columns=["DatasetView_id", "GrantView Key", "PublicationView Key", "Id"])
    converted, *_ = mig.convert_table(source, "DatasetView", norm)
    ref = pd.DataFrame([["s1", "39,191,725,391,917,250"], ["s2", "11111111"]],
                       columns=["DatasetView_id", "PublicationView Key"])
    known = {}
    diffs, _ = mig.manifest_differences(converted, mig.reference_frame(ref, "DatasetView", norm), "DatasetView", "CA1", (), known)
    assert known == {"mangled_keys": 1} and diffs["key"].tolist() == ["s2"]


def test_ambiguous_and_unmatched_aliases_go_to_a_worklist_file(tmp_path):
    table = ms_source([["syn_a", "CA1", "N", "GSE1", "d", "1"], ["syn_b", "CA1", "N", "GSE2", "d", "2"],
                       ["syn_c", "CA1", "N", "GSE3", "d", "3"]])
    fake = FakeSynapse({"synT1": ("CA1", "synP1", table)})
    fake.reference_source = ms_reference([["syn_x", "CA1", "N", "GSE1", "d"], ["syn_y,syn_z", "CA1", "N", "GSE2", "d"]])
    code, rep = run(tmp_path, fake, component="DatasetView")
    work = pd.read_csv(rep / "manifest_id_worklist.csv", dtype=str, keep_default_na=False)
    assert work.to_dict("records") == [
        {"kind": "ambiguous", "alias": "GSE2", "grant": "CA1", "grant_table": "synT1", "grant_table_id": "syn_b",
         "manifest_ids": "syn_y, syn_z", "note": ""},
        {"kind": "no_manifest_row", "alias": "GSE3", "grant": "CA1", "grant_table": "synT1", "grant_table_id": "syn_c",
         "manifest_ids": "", "note": ""}]
    assert "known differences: authors recovered 0, mangled PMID keys 0" in (rep / "run_summary.txt").read_text()


# --- review fixes: collisions, other grants, one equality rule, overrides, resume --------


def test_an_id_the_manifest_gives_to_two_aliases_is_not_a_remap():
    info = {}
    source = ms_source([["syn_a", "CA1", "N", "GSE1", "d", "1"], ["syn_b", "CA1", "N", "GSE2", "d", "2"]])
    ref = ms_reference([["syn_shared", "CA1", "N", "GSE1", "d"], ["syn_shared", "CA1", "N", "GSE2", "d"]])
    converted, changes, _, _, resolutions = ms_convert(source, ref, info=info)
    assert converted[MS_KEY].tolist() == ["syn_a", "syn_b"] and resolutions.empty  # nothing merged or dropped
    assert changes["rule"].tolist() == ["manifest_id_collision"] * 2
    assert info["manifest_ids"]["collision"] == 2 and info["manifest_ids"]["remapped"] == 0 and info["remapped"] == {}
    assert [(w["kind"], w["alias"]) for w in info["id_worklist"]] == [("collision", "GSE1"), ("collision", "GSE2")]
    assert "GSE2" in info["id_worklist"][0]["note"] and "GSE1" in info["id_worklist"][1]["note"]


def test_a_shared_manifest_id_is_a_collision_even_when_the_other_alias_is_in_another_grant():
    info = {}
    source = ms_source([["syn_a", "CA1", "N", "GSE1", "d", "1"]])  # this grant table has only GSE1
    ref = ms_reference([["syn_shared", "CA1", "N", "GSE1", "d"], ["syn_shared", "CA2", "N", "PRJ9", "d"]])
    converted, *_ = ms_convert(source, ref, info=info)
    assert converted[MS_KEY].tolist() == ["syn_a"] and info["manifest_ids"]["collision"] == 1


def test_a_remap_to_a_manifest_row_of_another_grant_has_its_own_rule_and_a_worklist_entry():
    info = {}
    source = ms_source([["syn_a", "CA1", "N", "GSE1", "d", "1"]])
    ref = ms_reference([["syn_new", "CA9", "N", "GSE1", "d"]])
    converted, changes, *_ = ms_convert(source, ref, grant="CA1", info=info)
    assert converted[MS_KEY].tolist() == ["syn_new"]  # still applied
    assert changes["rule"].tolist() == ["manifest_id_other_grant"]
    assert info["manifest_ids"]["other_grant"] == 1 and info["manifest_ids"]["remapped"] == 1
    assert info["id_worklist"][0]["kind"] == "other_grant" and "CA9" in info["id_worklist"][0]["note"]


def test_duplicate_matching_uses_the_manifest_equality_rule():
    # blank equals a manifest placeholder, and the row's grant is among the manifest row's grants
    source = ms_source([["s1", "CA1", "N", "GSE1", "x", "1"], ["s1", "CA1", "N", "GSE1", "", "2"]])
    ref = ms_reference([["s1", "CA1,CA2", "N", "GSE1", "Pending Annotation"]])
    converted, _, _, _, resolutions = ms_convert(source, ref)
    assert converted["DatasetDescription"].tolist() == [""]
    assert resolutions.iloc[0]["reason"] == "matches the merged manifest"


def test_a_row_override_does_not_change_the_reference_manifest(tmp_path):
    overrides = tmp_path / "o.csv"
    pd.DataFrame([["DatasetDescription", "syn_new", "d", "X", "CA1", "yes"]],
                 columns=["column", "key", "current_value", "proposed_value", "grant", "accept"]).to_csv(overrides, index=False)
    fake = FakeSynapse({"synT1": ("CA1", "synP1", ms_table("syn_new"))})
    fake.reference_source = ms_reference([["syn_new", "CA1", "N", "GSE1", "manifest text"],
                                          ["syn_same", "CA1", "N", "GSE2", "d"]])
    code, rep = run(tmp_path, fake, "--overrides", str(overrides), component="DatasetView")
    converted = pd.read_csv(rep / "CA1" / "converted.csv", dtype=str)
    assert converted["DatasetDescription"].tolist() == ["X", "d"]
    diffs = pd.read_csv(rep / "manifest_differences.csv", dtype=str, keep_default_na=False)
    assert diffs[["key", "grant_value", "manifest_value"]].values.tolist() == [["syn_new", "X", "manifest text"]]
    assert "row overrides that matched no row: 0" in (rep / "run_summary.txt").read_text()


def test_unused_row_overrides_and_duplicate_choices_are_reported(tmp_path):
    overrides, choices = tmp_path / "o.csv", tmp_path / "c.csv"
    pd.DataFrame([["DatasetDescription", "syn_new", "X", "CA1", "yes"], ["DatasetDescription", "nope", "Y", "", "yes"],
                  ["DatasetDescription", "syn_same", "Z", "CA7", "yes"]],
                 columns=["column", "key", "proposed_value", "grant", "accept"]).assign(current_value="").to_csv(overrides, index=False)
    pd.DataFrame([["zzz", "abc"]], columns=["key", "keep_legacy_id"]).to_csv(choices, index=False)
    fake = FakeSynapse({"synT1": ("CA1", "synP1", ms_table("syn_new"))})
    code, rep = run(tmp_path, fake, "--overrides", str(overrides), "--duplicate-choices", str(choices), component="DatasetView")
    text = (rep / "run_summary.txt").read_text()
    assert "row overrides that matched no row: 2" in text and "DatasetDescription / nope" in text
    assert "DatasetDescription / syn_same / CA7" in text  # the grant scopes the override
    assert "duplicate choices that matched no row: 1" in text and "zzz" in text


def test_the_manifest_version_is_part_of_the_resume_hash(tmp_path):
    args = mig.get_args(["--component", "DatasetView"])
    assert mig.options_hash(args, "etag1:3") != mig.options_hash(args, "etag2:3")
    versions = ["v1"]
    fake = FakeSynapse({"synT1": ("CA1", "synP1", ms_table())})
    with mock.patch.object(mig, "reference_version", lambda syn, component: versions[0]):
        run(tmp_path, fake, component="DatasetView", run_id="r1")
        code, rep = run(tmp_path, fake, component="DatasetView", run_id="r2")
        assert "already dryrun in run r1" in (rep / "summary.csv").read_text()
        versions[0] = "v2"
        code, rep = run(tmp_path, fake, component="DatasetView", run_id="r3")
        assert "already" not in (rep / "summary.csv").read_text()
