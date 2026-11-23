"""annotations/upload_recordsets.py with every Synapse call mocked."""
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs
from conftest import load_script
from fakes import FakeRS

ur = load_script("annotations/upload_recordsets.py", "upload_recordsets")
pytestmark = pytest.mark.usefixtures("synapse_tables")

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


class FakeSynapse(FakeRS):
    """A RecordSet whose versions are DataFrames, where a Grid write appends versions and can be disturbed.

    Kept as a subclass: the write is the upload's own (it appends the export versions, and a foreign version
    can land before or after it).
    """

    def __init__(self, versions, find=None, key=(KEY,), drift=None, midwrite=None, invalid=0):
        super().__init__(versions, key, invalid=invalid, list_cols={"Assay", "PublicationAuthors"})
        self.find = find if find is not None else (lambda folder: "synRS")
        self.drift = drift  # frame appended after our write: a foreign version landing later
        self.midwrite = midwrite  # frame appended before our write: landed between read and write
        self.extra_exports = 0

    def find_recordset(self, syn, folder, component):
        return self.find(folder)

    def write(self, syn, rs_id, csv_path, on_version=None, **kw):
        written = rs._load_csv(csv_path, self.list_cols)
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


def run(tmp_path, fake, *extra, execute=True, grants=("syn100",), run_id="r1", paths=None):
    paths = paths or make_inputs(tmp_path, grants)
    argv = ["--paths-csv", str(paths), "--component", "PublicationView",
            "--report-dir", str(tmp_path / "reports"), "--run-id", run_id,
            "--cache-dir", str(tmp_path / "cache"), "--write-log-table", "synLOG", *extra]
    if execute:
        argv += ["--execute", "--removals-table", "synREM"]
    with mock.patch.object(ur.recordsets, "find_recordset", fake.find_recordset), \
         mock.patch.object(ur.recordsets, "recordset_info", fake.info), \
         mock.patch.object(ur.recordsets, "latest_version",
                           lambda syn, rs_id: (*fake.info(syn, rs_id), getattr(fake, "md5", None))), \
         mock.patch.object(ur.recordsets, "read_recordset", fake.read), \
         mock.patch.object(ur.recordsets, "write_via_grid", fake.write), \
         mock.patch.object(ur.recordsets, "get_validation", fake.validation), \
         mock.patch.object(ur.recordsets, "principal_id", lambda syn: "42"):
        code = ur.main(argv, syn=object())
    return code, tmp_path / "reports" / run_id


def stored(*rows):
    return pd.DataFrame(list(rows), columns=[KEY, "PublicationTitle", "Assay"])


def status_of(rep, grant="syn100"):
    return (rep / grant / "summary.txt").read_text().split()[1]


def test_first_write_is_plain_upsert_pending_then_written(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ["RNA Sequencing"]], ["p9", "Other", ""])])
    code, rep = run(tmp_path, fake)
    assert code == 0
    written = fake.writes[0].set_index(KEY)
    assert written.loc["p1", "PublicationTitle"] == "Pipeline title"
    assert written.loc["p1", "Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert "p9" in written.index and "p2" in written.index
    assert log.pipeline_map("synRS") == {2: 1}
    statuses = log._read("synRS")
    assert statuses["status"].tolist() == ["pending", "written", "done"]
    assert statuses["principalId"].tolist() == ["42", "42", "42"]
    assert (rep / "syn100" / "validation.csv").exists()
    assert status_of(rep) == "written"
    assert pd.read_csv(rep / "syn100" / "decisions.csv").empty


def test_dry_run_does_not_recover_an_aged_pending_row(tmp_path, log):
    log.start("synRS", 1, "dead", "42")
    log.rows[0]["writtenOn"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)
    before = list(log.rows)
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    code, rep = run(tmp_path, fake, execute=False)
    assert code == 0 and status_of(rep) == "dryrun" and fake.writes == []
    assert log.rows == before and len(fake.versions) == 1  # an execute run would log the row ``aborted``


def test_dry_run_writes_nothing_but_reports(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    code, rep = run(tmp_path, fake, execute=False)
    assert code == 0 and fake.writes == []
    assert log.rows == []
    assert not (rep / "syn100" / "validation.csv").exists()
    assert (rep / "syn100" / "merged.csv").exists()
    assert status_of(rep) == "dryrun"


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


def test_log_ahead_of_recordset_fails(tmp_path, log):
    log.record("synRS", 5, "old", 2)
    code, rep = run(tmp_path, FakeSynapse([stored(["p1", "Old", ""])]))
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


def test_lock_from_another_runs_pending_row_skips_the_grant(tmp_path, log):
    log.start("synRS", 1, "other-run", "7")
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    code, rep = run(tmp_path, fake)
    assert code == 1 and fake.writes == []  # non-zero: the upload is not done
    assert status_of(rep) == "locked"
    assert ("run other-run has an unfinished write (pending row under 2 h old); skipped"
            in (rep / "syn100" / "summary.txt").read_text())


def test_crashed_run_is_recovered_before_replay(tmp_path, log):
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.start("synRS", 1, "dead", "42")
    log.rows[1]["writtenOn"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)  # age the pending row beyond the lock window
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Dead run wrote this", ""], ["p2", "Second", ""])  # dead run's export, unlogged
    fake = FakeSynapse([v1, v2])
    with mock.patch.object(ur.recordsets, "recordset_version_meta",
                           lambda syn, rs_id, v: ("42", pd.Timestamp.now(tz="UTC"))):
        code, rep = run(tmp_path, fake)
    assert code == 0
    assert log.pipeline_map("synRS")[2] == 1  # recovered with the pending row's base
    # v2 is a pipeline version, not a contributor edit: the upload overwrites it
    assert fake.writes[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Pipeline title"
    assert "recovered=[2]" in (rep / "syn100" / "summary.txt").read_text()


def test_contributor_value_survives_later_runs(tmp_path, log):
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Contributor title", ""], ["p2", "Second", ""])
    log.record("synRS", 1, "old", 2)
    fake = FakeSynapse([v1, v2])
    for n in (1, 2):
        code, rep = run(tmp_path, fake, run_id=f"r{n}")
        assert code == 0
        assert fake.writes[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Contributor title", n
        conflicts = pd.read_csv(rep / "syn100" / "conflicts.csv", dtype=str, keep_default_na=False)
        assert list(conflicts["column"]) == ["PublicationTitle"], n


def curator_edits(log):
    v1 = stored(["p1", "Pipeline title", ["RNA Sequencing"]], ["p2", "Second", ""])
    v2 = stored(["p1", "Contributor title", ["Imaging"]], ["p2", "Second", ""])
    log.record("synRS", 1, "old", 2)
    return FakeSynapse([v1, v2])


def test_overwrite_csv_replaces_only_the_listed_curator_cell(tmp_path, log):
    fake = curator_edits(log)
    allow = tmp_path / "overwrite.csv"
    pd.DataFrame([["syn100", "p1", "PublicationTitle"]], columns=["grantId", "key", "column"]).to_csv(allow, index=False)
    code, rep = run(tmp_path, fake, "--overwrite-csv", str(allow))
    assert code == 0
    row = fake.writes[-1].set_index(KEY).loc["p1"]
    assert row["PublicationTitle"] == "Pipeline title"  # listed: overwritten
    assert row["Assay"] == ["Imaging"]  # not listed: the curator still wins
    ow = pd.read_csv(rep / "syn100" / "overwritten.csv", dtype=str, keep_default_na=False)
    assert ow.to_dict("records") == [{"key": "p1", "column": "PublicationTitle",
                                      "contributor_value": "Contributor title", "upload_value": "Pipeline title"}]
    conflicts = pd.read_csv(rep / "syn100" / "conflicts.csv", dtype=str, keep_default_na=False)
    assert list(conflicts["column"]) == ["Assay"]


def test_overwrite_csv_of_another_grant_changes_nothing(tmp_path, log):
    fake = curator_edits(log)
    allow = tmp_path / "overwrite.csv"
    pd.DataFrame([["syn555", "p1", "PublicationTitle"]], columns=["grantId", "key", "column"]).to_csv(allow, index=False)
    code, rep = run(tmp_path, fake, "--overwrite-csv", str(allow))
    assert fake.writes == []  # the curator wins everywhere: nothing to write
    assert not (rep / "syn100" / "overwritten.csv").exists()


def test_pipeline_originated_value_is_overwritten(tmp_path, log):
    log.record("synRS", 1, "old", 2)
    fake = FakeSynapse([stored(["p1", "Old pipeline", ""], ["p2", "Second", ""])])
    code, rep = run(tmp_path, fake)
    assert fake.writes[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Pipeline title"
    assert pd.read_csv(rep / "syn100" / "conflicts.csv").empty


def test_stale_session_scenario_second_run_keeps_both_edits(tmp_path, log):
    v5 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v6 = stored(["p1", "A edit", ""], ["p2", "Second", ""])
    v7 = stored(["p1", "Pipeline title", ""], ["p2", "B edit", ""])
    log.record("synRS", 1, "old", 2)
    fake = FakeSynapse([v5, v6, v7])
    for n in (1, 2):
        run(tmp_path, fake, run_id=f"r{n}")
        written = fake.writes[-1].set_index(KEY)
        assert written.loc["p1", "PublicationTitle"] == "A edit"
        assert written.loc["p2", "PublicationTitle"] == "B edit"


def test_unchanged_merge_skips_the_write(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    run(tmp_path, fake, run_id="r1")
    n = len(fake.versions)
    code, rep = run(tmp_path, fake, run_id="r2")
    assert status_of(rep) == "unchanged" and len(fake.versions) == n


def test_every_export_is_logged_with_the_base_version(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    fake.extra_exports = 1
    run(tmp_path, fake)
    assert log.pipeline_map("synRS") == {2: 1, 3: 1}


def test_contributor_export_mid_write_triggers_one_immediate_rerun(tmp_path, log):
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    log.record("synRS", 1, "old", 2)
    mid = stored(["p1", "Contributor title", ""], ["p2", "Second", ""])
    fake = FakeSynapse([v1], midwrite=mid)
    code, rep = run(tmp_path, fake, run_id="r1")
    text = (rep / "syn100" / "summary.txt").read_text()
    assert "rerun after foreign versions [2] landed mid-write" in text
    # v3 (base 1) overwrote the edit; the rerun's replay restored it as v4
    assert log.pipeline_map("synRS") == {1: 0, 3: 1, 4: 3}
    assert fake.versions[-1].set_index(KEY).loc["p1", "PublicationTitle"] == "Contributor title"
    assert (rep / "syn100" / "rerun" / "merged.csv").exists()
    assert len(fake.writes) == 2  # at most one rerun


def test_dropped_schema_property_column_does_not_force_a_write(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    run(tmp_path, fake, run_id="r1")
    stale = fake.versions[-1].copy()
    stale["DroppedProperty"] = "legacy"  # an old column Synapse no longer keeps
    fake.versions[-1] = stale
    n = len(fake.versions)
    code, rep = run(tmp_path, fake, run_id="r2")
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


def test_failed_write_resolves_the_pending_row(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""])])

    def boom(*a, **kw):
        raise RuntimeError("grid failed")

    fake.write = boom
    meta = lambda syn, rs_id, v: ("99", pd.Timestamp.now(tz="UTC"))
    with mock.patch.object(ur.recordsets, "recordset_version_meta", meta):
        code, rep = run(tmp_path, fake)
    assert code == 1 and status_of(rep) == "failed"
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]
    assert log.unresolved_pending("synRS") == []


def test_lock_race_loser_aborts_and_reports_locked(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    real_start = rs.WriteLog.start

    def start_after_rival(self, rs_id, base, run_id, principal):
        # a rival's pending row, written a moment earlier, appears before ours
        real_start(self, rs_id, base, "rival", "7")
        real_start(self, rs_id, base, run_id, principal)

    with mock.patch.object(rs.WriteLog, "start", start_after_rival):
        code, rep = run(tmp_path, fake)
    assert status_of(rep) == "locked" and fake.writes == []
    assert log._read("synRS")["status"].tolist() == ["pending", "pending", "aborted"]


def test_recordset_moving_before_the_write_aborts_with_retry(tmp_path, log):
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
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]


def authors_inputs(tmp_path, authors):
    f = tmp_path / "syn100_publication.csv"
    pd.DataFrame([["PublicationView", f"p{i}", "T", a] for i, a in enumerate(authors)],
                 columns=["Component", "PublicationView_id", "Publication Title", "Publication Authors"]).to_csv(f, index=False)
    paths = tmp_path / "paths.csv"
    pd.DataFrame([[str(f), "syn1009", "syn100"]], columns=["File Paths", "folderIdPublication", "grantId"]).to_csv(paths, index=False)
    return paths


def test_rows_with_a_quote_or_backslash_in_a_list_item_are_dropped_and_reported(tmp_path, caplog):
    fake = FakeSynapse([stored(["p9", "Other", ""])])
    paths = authors_inputs(tmp_path, ['Lo"ic B, Kuo JC', "Back\\slash A", "Kuo JC, Lee K"])
    code, rep = run(tmp_path, fake, paths=paths)
    assert code == 0 and status_of(rep) == "written"
    written = fake.writes[0].set_index(KEY)
    assert sorted(written.index) == ["p2", "p9"]  # p0 (quote) and p1 (backslash) are not written
    unsafe = pd.read_csv(rep / "syn100" / "unsafe_list_items.csv", dtype=str)
    assert sorted(unsafe[KEY]) == ["p0", "p1"]
    assert "unsafe_rows=2" in (rep / "syn100" / "summary.txt").read_text()
    assert "unsafe_list_items.csv" in caplog.text


def test_comma_terms_are_learned_only_after_an_executed_write(tmp_path):
    with mock.patch.object(ur.recordsets, "learn_comma_terms", return_value=["a, b"]) as learn:
        run(tmp_path, FakeSynapse([stored(["p1", "Old", ""])]), execute=False)
        learn.assert_not_called()
        code, rep = run(tmp_path, FakeSynapse([stored(["p1", "Old", ""])]), run_id="r2")
        learn.assert_called_once()
    assert code == 0 and "new_comma_terms" in pd.read_csv(rep.parent / "r2" / "summary.csv").columns


def test_summary_line_text():
    s = {"grant": "g1", "status": "written", "message": "wrote v2", "recordset": "synRS", "version": 2, "rows": 3,
         "decisions": 4, "conflicts": 5, "overwritten": 6, "invalid_rows": 7, "recovered": [8], "foreign_midwrite": [9],
         "removal_skipped": 10, "unsafe_rows": 11, "file": "ignored.csv"}
    assert rs.summary_line(s, "grant", ur.SUMMARY_FIELDS) == (
        "g1: written recordset=synRS version=2 rows=3 decisions=4 conflicts=5 overwritten=6 invalid_rows=7 "
        "recovered=[8] foreign_midwrite=[9] removal_skipped=10 unsafe_rows=11 wrote v2")


def test_only_the_latest_version_is_read_with_its_md5(tmp_path, log):
    fake = FakeSynapse([stored(["p1", "Old", ""]), stored(["p1", "Older", ""])])
    fake.md5, seen, read = "md5-latest", [], fake.read
    fake.read = lambda syn, rs_id, version=None, md5=None, **kw: seen.append((version, md5)) or read(
        syn, rs_id, version=version, md5=md5, **kw)
    run(tmp_path, fake, execute=False)
    assert (1, None) in seen and (2, "md5-latest") in seen  # an older version is immutable: no md5 check
