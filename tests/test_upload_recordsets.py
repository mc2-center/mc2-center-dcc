"""annotations/upload_recordsets.py with every Synapse call mocked."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("upload_recordsets", ROOT / "annotations" / "upload_recordsets.py")
ur = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ur)

KEY = "PublicationViewId"
HEADERS = ["Component", "PublicationView_id", "Publication Title", "Assay"]


def make_inputs(tmp_path, grants=("syn100",)):
    rows = []
    for g in grants:
        f = tmp_path / f"{g}_publication.csv"
        pd.DataFrame(
            [["PublicationView", "p1", "Pipeline title", "RNA Sequencing, Mass Spectrometry"],
             ["PublicationView", "p2", "Second", ""]],
            columns=HEADERS,
        ).to_csv(f, index=False)
        rows.append([str(f), f"syn{g[3:]}9", g])
    paths = tmp_path / "paths.csv"
    pd.DataFrame(rows, columns=["File Paths", "folderIdPublication", "grantId"]).to_csv(paths, index=False)
    return paths


class FakeSynapse:
    """Versions of one RecordSet as DataFrames; a Grid write appends versions."""

    def __init__(self, versions, find=None, key=(KEY,), drift=None, midwrite=None, invalid=0):
        self.versions = list(versions)
        self.find = find if find is not None else (lambda folder: "synRS")
        self.key = list(key)
        self.drift = drift  # frame appended after our write: a foreign version landing later
        self.midwrite = midwrite  # frame appended before our write: landed between read and write
        self.extra_exports = 0
        self.invalid = invalid
        self.writes = []
        self.calls = []

    def find_recordset(self, syn, folder, component):
        return self.find(folder)

    def info(self, syn, rs_id):
        return len(self.versions), self.key

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        return self.versions[(version or len(self.versions)) - 1].copy()

    def write(self, syn, rs_id, csv_path, on_version=None, **kw):
        self.calls.append("write")
        written = rs._load_csv(csv_path, {"Assay"})
        self.writes.append(written)
        if self.midwrite is not None:
            self.versions.append(self.midwrite)
            self.midwrite = None  # lands once
        created = []
        for _ in range(1 + self.extra_exports):
            self.versions.append(written)
            created.append(len(self.versions))
            if on_version:
                on_version(len(self.versions))
        if self.drift is not None:
            self.versions.append(self.drift)
        return created

    def validation(self, syn, rs_id, version, loc, keys):
        valid = [i >= self.invalid for i in range(len(keys))]
        return pd.DataFrame({"key": keys, "row_index": range(len(keys)), "is_valid": valid,
                             "validation_error_message": [""] * len(keys),
                             "all_validation_messages": [""] * len(keys)})


def run(tmp_path, fake, *extra, execute=True, log=None, grants=("syn100",), run_id="r1", paths=None):
    paths = paths or make_inputs(tmp_path, grants)
    log = log or tmp_path / "log.csv"
    argv = ["--paths-csv", str(paths), "--component", "PublicationView",
            "--report-dir", str(tmp_path / "reports"), "--run-id", run_id,
            "--cache-dir", str(tmp_path / "cache"), *extra]
    argv += ["--write-log-table", "synLOG"] if execute else ["--write-log-csv", str(log)]
    if execute:
        argv.append("--execute")
    with mock.patch.object(ur.recordsets, "find_recordset", fake.find_recordset), \
         mock.patch.object(ur.recordsets, "recordset_info", fake.info), \
         mock.patch.object(ur.recordsets, "read_recordset", fake.read), \
         mock.patch.object(ur.recordsets, "write_via_grid", fake.write), \
         mock.patch.object(ur.recordsets, "get_validation", fake.validation), \
         mock.patch.object(ur.recordsets, "SynapseTableWriteLog", lambda syn, tid: rs.CsvWriteLog(log)), \
         mock.patch.object(ur, "principal_id", lambda syn: "42"):
        code = ur.main(argv, syn=object())
    return code, tmp_path / "reports" / run_id


def stored(*rows):
    return pd.DataFrame(list(rows), columns=[KEY, "PublicationTitle", "Assay"])


def status_of(rep, grant="syn100"):
    return (rep / grant / "summary.txt").read_text().split()[1]


def test_first_write_is_plain_upsert_pending_then_written(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ["RNA Sequencing"]], ["p9", "Other", ""])])
    code, rep = run(tmp_path, fake)
    assert code == 0
    written = fake.writes[0].set_index(KEY)
    assert written.loc["p1", "PublicationTitle"] == "Pipeline title"
    assert written.loc["p1", "Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert "p9" in written.index and "p2" in written.index
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    assert log.pipeline_map("synRS") == {2: 1}
    statuses = log._read("synRS")
    assert statuses["status"].tolist() == ["pending", "written"]
    assert statuses["principalId"].tolist() == ["42", "42"]
    assert (rep / "syn100" / "validation.csv").exists()
    assert status_of(rep) == "written"
    assert pd.read_csv(rep / "syn100" / "decisions.csv").empty


def test_dry_run_writes_nothing_but_reports(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    code, rep = run(tmp_path, fake, execute=False)
    assert code == 0 and fake.writes == []
    assert not (tmp_path / "log.csv").exists()
    assert not (rep / "syn100" / "validation.csv").exists()
    assert (rep / "syn100" / "merged.csv").exists()
    assert status_of(rep) == "dryrun"


def test_execute_with_a_csv_write_log_is_refused(tmp_path):
    paths = make_inputs(tmp_path)
    with pytest.raises(SystemExit):
        ur.main(["--paths-csv", str(paths), "--component", "PublicationView", "--report-dir",
                 str(tmp_path / "r"), "--write-log-csv", str(tmp_path / "l.csv"), "--execute"], syn=object())


def test_missing_recordset_stops_that_grant_only(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])], find=lambda folder: None if folder == "syn1009" else "synRS")
    code, rep = run(tmp_path, fake, grants=("syn100", "syn200"))
    assert code == 1
    assert status_of(rep) == "missing_recordset" and status_of(rep, "syn200") == "written"
    assert len(fake.writes) == 1


def test_exception_in_one_grant_does_not_stop_others(tmp_path):
    calls = []

    def find(folder):
        calls.append(folder)
        if folder == "syn1009":
            raise RuntimeError("synapse down")
        return "synRS"

    fake = FakeSynapse([stored(["p1", "Old", ""])], find=find)
    code, rep = run(tmp_path, fake, grants=("syn100", "syn200"))
    assert code == 1 and calls == ["syn1009", "syn2009"]
    assert status_of(rep) == "failed"


def test_log_ahead_of_recordset_fails(tmp_path):
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 5, "old", 2)
    code, rep = run(tmp_path, FakeSynapse([stored(["p1", "Old", ""])]), log=log)
    assert code == 1


def test_duplicate_and_blank_upload_keys_fail_that_grant(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    paths = make_inputs(tmp_path)
    f = pd.read_csv(tmp_path / "syn100_publication.csv", dtype=str, keep_default_na=False)
    f.loc[1, "PublicationView_id"] = " p1"
    f.to_csv(tmp_path / "syn100_publication.csv", index=False)
    code, rep = run(tmp_path, fake, paths=paths)
    assert code == 1 and status_of(rep) == "failed"
    assert "duplicate keys" in (rep / "syn100" / "summary.txt").read_text()
    assert fake.writes == []


def test_lock_from_another_runs_pending_row_skips_the_grant(tmp_path):
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).start("synRS", 1, "other-run", "7")
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    code, rep = run(tmp_path, fake, log=log)
    assert code == 1 and fake.writes == []  # non-zero: the upload is not done
    assert status_of(rep) == "locked"


def test_crashed_run_is_recovered_before_replay(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.start("synRS", 1, "dead", "42")
    old = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)
    text = (tmp_path / "log.csv").read_text().splitlines()
    # age the pending row beyond the lock window
    text[2] = text[2].replace(text[2].split(",")[-2], old.isoformat())
    (tmp_path / "log.csv").write_text("\n".join(text) + "\n")
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Dead run wrote this", ""], ["p2", "Second", ""])  # dead run's export, unlogged
    fake = FakeSynapse([v1, v2])
    with mock.patch.object(ur.recordsets, "recordset_version_meta",
                           lambda syn, rs_id, v: ("42", pd.Timestamp.now(tz="UTC"))):
        code, rep = run(tmp_path, fake, log=tmp_path / "log.csv")
    assert code == 0
    assert log.pipeline_map("synRS")[2] == 1  # recovered with the pending row's base
    # v2 is a pipeline version, not a contributor edit: the upload overwrites it
    assert fake.writes[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Pipeline title"
    assert "recovered=[2]" in (rep / "syn100" / "summary.txt").read_text()


def test_contributor_value_survives_later_runs(tmp_path):
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Contributor title", ""], ["p2", "Second", ""])
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    fake = FakeSynapse([v1, v2])
    for n in (1, 2):
        code, rep = run(tmp_path, fake, log=log, run_id=f"r{n}")
        assert code == 0
        assert fake.writes[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Contributor title", n
        conflicts = pd.read_csv(rep / "syn100" / "conflicts.csv", dtype=str, keep_default_na=False)
        assert list(conflicts["column"]) == ["PublicationTitle"], n


def test_pipeline_originated_value_is_overwritten(tmp_path):
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    fake = FakeSynapse([stored(["p1", "Old pipeline", ""], ["p2", "Second", ""])])
    code, rep = run(tmp_path, fake, log=log)
    assert fake.writes[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Pipeline title"
    assert pd.read_csv(rep / "syn100" / "conflicts.csv").empty


def test_stale_session_scenario_second_run_keeps_both_edits(tmp_path):
    v5 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v6 = stored(["p1", "A edit", ""], ["p2", "Second", ""])
    v7 = stored(["p1", "Pipeline title", ""], ["p2", "B edit", ""])
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    fake = FakeSynapse([v5, v6, v7])
    for n in (1, 2):
        run(tmp_path, fake, log=log, run_id=f"r{n}")
        written = fake.writes[-1].set_index(KEY)
        assert written.loc["p1", "PublicationTitle"] == "A edit"
        assert written.loc["p2", "PublicationTitle"] == "B edit"


def test_unchanged_merge_skips_the_write(tmp_path):
    log = tmp_path / "log.csv"
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    run(tmp_path, fake, log=log, run_id="r1")
    n = len(fake.versions)
    code, rep = run(tmp_path, fake, log=log, run_id="r2")
    assert status_of(rep) == "unchanged" and len(fake.versions) == n


def test_every_export_is_logged_with_the_base_version(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    fake.extra_exports = 1
    run(tmp_path, fake)
    assert rs.CsvWriteLog(tmp_path / "log.csv").pipeline_map("synRS") == {2: 1, 3: 1}


def test_contributor_export_mid_write_triggers_one_immediate_rerun(tmp_path):
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    mid = stored(["p1", "Contributor title", ""], ["p2", "Second", ""])
    fake = FakeSynapse([v1], midwrite=mid)
    code, rep = run(tmp_path, fake, log=log, run_id="r1")
    text = (rep / "syn100" / "summary.txt").read_text()
    assert "rerun after foreign versions [2] landed mid-write" in text
    # v3 (base 1) overwrote the edit; the rerun's replay restored it as v4
    assert rs.CsvWriteLog(log).pipeline_map("synRS") == {1: 0, 3: 1, 4: 3}
    assert fake.versions[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Contributor title"
    assert (rep / "syn100" / "rerun" / "merged.csv").exists()
    assert len(fake.writes) == 2  # at most one rerun


def test_dropped_schema_property_column_does_not_force_a_write(tmp_path):
    log = tmp_path / "log.csv"
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    run(tmp_path, fake, log=log, run_id="r1")
    stale = fake.versions[-1].copy()
    stale["DroppedProperty"] = "legacy"  # an old column Synapse no longer keeps
    fake.versions[-1] = stale
    n = len(fake.versions)
    code, rep = run(tmp_path, fake, log=log, run_id="r2")
    assert status_of(rep) == "unchanged" and len(fake.versions) == n
    assert "DroppedProperty" not in (rep / "syn100" / "merged.csv").read_text()


def test_duplicate_keys_in_a_stored_version_warn(tmp_path, caplog):
    fake = FakeSynapse([stored(["p1", "A", ""], ["p1", "B", ""])])
    with caplog.at_level("WARNING"):
        run(tmp_path, fake, execute=False)
    assert "duplicate keys" in caplog.text


def test_retry_and_verification_mismatch_exit_non_zero(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    real_write = fake.write

    def corrupting(*a, **kw):
        out = real_write(*a, **kw)
        fake.versions[out[-1] - 1] = stored(["p1", "Altered by Grid", ""])
        return out

    fake.write = corrupting
    code, _ = run(tmp_path, fake)
    assert code == 1
    fake2 = FakeSynapse([stored(["p1", "Old", ""])])
    calls = {"n": 0}
    real_info = fake2.info

    def moving(syn, rs_id):
        calls["n"] += 1
        if calls["n"] == 2:
            fake2.versions.append(stored(["p1", "Someone", ""]))
        return real_info(syn, rs_id)

    fake2.info = moving
    (tmp_path / "second").mkdir()
    code, _ = run(tmp_path / "second", fake2)
    assert code == 1


def test_foreign_version_after_write_is_changed_after_write_not_mismatch(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])], drift=stored(["p1", "Someone else", ""]))
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "changed_after_write" and code == 0
    assert not (rep / "syn100" / "verification.txt").exists()


def test_invalid_rows_status_is_written_with_invalid_and_exit_zero(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])], invalid=1)
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "written_with_invalid" and code == 0
    assert "invalid_rows=1" in (rep / "syn100" / "summary.txt").read_text()


def test_verification_reads_the_exported_version_and_flags_a_real_mismatch(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    real_write = fake.write

    def corrupting_write(*a, **kw):
        out = real_write(*a, **kw)
        fake.versions[out[-1] - 1] = stored(["p1", "Altered by Grid", ""])
        return out

    fake.write = corrupting_write
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "verification_mismatch"
    assert (rep / "syn100" / "verification.txt").exists()


def test_failed_write_resolves_the_pending_row(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])

    def boom(*a, **kw):
        raise RuntimeError("grid failed")

    fake.write = boom
    meta = lambda syn, rs_id, v: ("99", pd.Timestamp.now(tz="UTC"))
    with mock.patch.object(ur.recordsets, "recordset_version_meta", meta):
        code, rep = run(tmp_path, fake)
    assert code == 1 and status_of(rep) == "failed"
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]
    assert log.unresolved_pending("synRS") == []


def test_lock_race_loser_aborts_and_reports_locked(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    real_start = rs.WriteLog.start

    def start_after_rival(self, rs_id, base, run_id, principal):
        # a rival's pending row, written a moment earlier, appears before ours
        real_start(self, rs_id, base, "rival", "7")
        real_start(self, rs_id, base, run_id, principal)

    with mock.patch.object(rs.WriteLog, "start", start_after_rival):
        code, rep = run(tmp_path, fake, log=tmp_path / "log.csv")
    assert status_of(rep) == "locked" and fake.writes == []
    assert log._read("synRS")["status"].tolist() == ["pending", "pending", "aborted"]


def test_recordset_moving_before_the_write_aborts_with_retry(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    calls = {"n": 0}
    real_info = fake.info

    def moving_info(syn, rs_id):
        calls["n"] += 1
        if calls["n"] == 2:  # the re-check right before the write
            fake.versions.append(stored(["p1", "Someone", ""]))
        return real_info(syn, rs_id)

    fake.info = moving_info
    code, rep = run(tmp_path, fake)
    assert status_of(rep) == "retry" and fake.writes == []
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]
