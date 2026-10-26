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

    # views / tables
    def list_tables(self, syn, component):
        return pd.DataFrame({"id": list(self.tables), "name": ["x"] * len(self.tables)})

    def grants_map(self, syn):
        return dict(self.grants)

    def project_of(self, syn, table_id):
        return self.tables[table_id][1]

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
    converted, changes, unmapped, dropped = mig.convert_table(source_table(), "PublicationView", norm)
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


def test_duplicate_and_blank_keys_fail_the_conversion():
    norm = mig.value_maps.Normalizer.for_component("PublicationView")
    table = source_table()
    table.loc[1, "PublicationView_id"] = "p1"
    with pytest.raises(mig.ConversionError, match=r"duplicate keys \['p1'\]"):
        mig.convert_table(table, "PublicationView", norm)
    table.loc[1, "PublicationView_id"] = " "
    with pytest.raises(mig.ConversionError, match="blank key"):
        mig.convert_table(table, "PublicationView", norm)


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


def test_duplicate_key_fails_that_grant(tmp_path):
    dup = source_table()
    dup.loc[1, "PublicationView_id"] = "p1"
    fake = FakeSynapse({"synT1": ("CA1", "synP1", dup)})
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "failed" and "duplicate keys" in (rep / "CA1" / "summary.txt").read_text()
    assert code == 1


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


def test_tables_of_projects_outside_the_grants_table_are_skipped(tmp_path, capsys):
    fake = FakeSynapse({"synT1": ("CA1", "synP1", source_table()), "synT2": ("CA2", "synP2", source_table())},
                       grants={"synP1": "CA1"})
    code, rep = run(tmp_path, fake)
    assert sorted(p.name for p in rep.iterdir() if p.is_dir()) == ["CA1"]
    assert "synT2: project synP2 is not in the Grants table" in capsys.readouterr().out


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
