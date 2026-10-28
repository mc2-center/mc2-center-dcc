"""annotations/remove_recordset_rows.py with every Synapse call stubbed (D-Q49)."""
import argparse
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("remove_recordset_rows", ROOT / "annotations" / "remove_recordset_rows.py")
cmd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cmd)

K = "PublicationViewId"
COLS = [K, "Title", "Assay"]


def frame(*rows):
    return pd.DataFrame(list(rows), columns=COLS)


class FakeRS:
    """One RecordSet's versions; store and export append versions, calls are ordered in ``events``."""

    def __init__(self, versions, sessions=(), log=None):
        self.versions = list(versions)
        self.sessions = list(sessions)
        self.events = []
        self.log = log
        self.crash_after_store = False

    def info(self, syn, rs_id):
        return len(self.versions), [K]

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        return self.versions[(version or len(self.versions)) - 1].copy()

    def store(self, syn, rs_id, path):
        self.events.append(("store", list(self.log._read(rs_id)["status"]) if self.log else None))
        self.versions.append(rs._load_csv(path))
        return len(self.versions)

    def export(self, syn, rs_id, on_version=None, **kw):
        self.events.append(("export", None))
        self.versions.append(self.versions[-1].copy())
        if on_version:
            on_version(len(self.versions))
        return [len(self.versions)]

    def open_sessions(self, syn, rs_id):
        return list(self.sessions)

    def meta(self, syn, rs_id, v):
        return "42", pd.Timestamp.now(tz="UTC")


def setup(tmp_path, versions, requested=("b",), **kw):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    removals = rs.CsvRemovals(tmp_path / "removals.csv")
    for k in requested:
        removals.append("synRS", k, "requested", reason="dup", requested_by="u1")
    fake = FakeRS(versions, log=log, **kw)
    return fake, log, removals


def process(tmp_path, fake, log, removals, execute=True, run_id="dcc-removal-1"):
    args = argparse.Namespace(execute=execute, run_id=run_id, cache_dir=str(tmp_path / "cache"))
    with mock.patch.object(cmd.recordsets, "recordset_info", fake.info), \
         mock.patch.object(cmd.recordsets, "read_recordset", fake.read), \
         mock.patch.object(cmd.recordsets, "store_new_version", fake.store), \
         mock.patch.object(cmd.recordsets, "export_via_grid", fake.export), \
         mock.patch.object(cmd.recordsets, "open_grid_sessions", fake.open_sessions), \
         mock.patch.object(cmd.recordsets, "recordset_version_meta", fake.meta):
        return cmd.process_recordset(object(), "synRS", removals, log, args, tmp_path / "rep" / "synRS",
                                     "42" if execute else "")


V1 = frame(["a", "T", '["x","y"]'], ["b", "T", ""], ["c", "T", ""])


def test_dry_run_reports_keys_and_writes_nothing(tmp_path, capsys):
    fake, log, removals = setup(tmp_path, [V1], requested=("b", "zzz"))
    s = process(tmp_path, fake, log, removals, execute=False)
    assert s["status"] == "dryrun" and s["remove"] == 1 and s["not_found"] == 1
    assert "would remove ['b']; not found ['zzz']" in s["message"]
    assert len(fake.versions) == 1 and fake.events == []
    assert not (tmp_path / "log.csv").exists()
    assert set(removals.requested("synRS")) == {"b", "zzz"}  # unchanged


def test_dry_run_reports_a_blocking_session(tmp_path):
    fake, log, removals = setup(tmp_path, [V1], sessions=[SimpleNamespace(session_id="g1")])
    s = process(tmp_path, fake, log, removals, execute=False)
    assert s["status"] == "blocked" and "g1" in s["message"]


def test_main_dry_run_exit_code_and_output(tmp_path, capsys):
    fake, log, removals = setup(tmp_path, [V1])
    with mock.patch.object(cmd.recordsets, "recordset_info", fake.info), \
         mock.patch.object(cmd.recordsets, "read_recordset", fake.read), \
         mock.patch.object(cmd.recordsets, "open_grid_sessions", fake.open_sessions), \
         mock.patch.object(cmd.recordsets, "recordset_version_meta", fake.meta):
        code = cmd.main(["--removals-csv", str(tmp_path / "removals.csv"), "--write-log-csv", str(tmp_path / "log.csv"),
                         "--report-dir", str(tmp_path / "rep"), "--run-id", "dcc-removal-1"], syn=object())
    out = capsys.readouterr().out
    assert code == 0 and "dry run" in out and "would remove ['b']" in out
    assert len(fake.versions) == 1


def test_execute_stores_then_exports_then_logs_in_order(tmp_path):
    fake, log, removals = setup(tmp_path, [V1], requested=("b", "zzz"))
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "removed" and s["version"] == 2
    # the lock (pending) is taken before the store; the export comes after
    assert fake.events == [("store", ["pending"]), ("export", None)]
    statuses = log._read("synRS")
    assert statuses["status"].tolist() == ["pending", "removed", "written", "done"]
    assert statuses["version"].dropna().tolist() == [2, 3]
    assert statuses["baseVersion"].tolist() == [1, 1, 2, 1]  # removed: base = read; written: base = removal version
    assert log.removal_versions("synRS") == {2} and log.pipeline_map("synRS") == {2: 1, 3: 2}
    assert log.unresolved_pending("synRS") == []
    # stored version lacks b, keeps the JSON list cell as written
    assert list(fake.versions[1][K]) == ["a", "c"] and fake.versions[1].loc[0, "Assay"] == '["x","y"]'
    assert "\nb,T," not in (tmp_path / "rep" / "synRS" / "removal.csv").read_text()
    # the removals table
    assert removals.requested("synRS") == {}
    assert removals.removed("synRS") == {"b": 2}
    assert removals.removal_events("synRS") == {2: {"b"}}
    latest = removals._latest("synRS").set_index("key")
    assert latest.loc["zzz", "status"] == "not_found" and latest.loc["b", "reason"] == "dup"
    # replay now drops it for good
    state, _, _ = rs.replay(list(enumerate(fake.versions, start=1)), log.pipeline_map("synRS"), K, (),
                            removals.removal_events("synRS"))
    assert set(state[K]) == {"a", "c"}


def test_open_grid_session_refuses_and_writes_nothing(tmp_path):
    fake, log, removals = setup(tmp_path, [V1], sessions=[SimpleNamespace(session_id="g1")])
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "blocked" and fake.events == [] and len(fake.versions) == 1
    assert set(removals.requested("synRS")) == {"b"}


def test_session_opened_after_the_lock_aborts_the_pending_row(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])
    calls = iter([[], [SimpleNamespace(session_id="g2")]])
    fake.open_sessions = lambda syn, rs_id: next(calls)
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "blocked" and fake.events == []
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]


def test_another_runs_pending_row_locks_the_recordset(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])
    log.start("synRS", 1, "other", "7")
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "locked" and fake.events == []


def test_lock_race_loser_aborts(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])
    with mock.patch.object(rs.CsvWriteLog, "earlier_pending", lambda self, rs_id, run_id, now=None: "other"):
        s = process(tmp_path, fake, log, removals)
    assert s["status"] == "locked" and fake.events == []
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]


def test_recordset_moving_after_the_read_retries(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])
    answers = iter([(1, [K]), (2, [K])])
    fake.info = lambda syn, rs_id: next(answers)
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "retry" and fake.events == []


def test_keys_not_found_are_logged_and_the_pending_row_is_resolved(tmp_path):
    fake, log, removals = setup(tmp_path, [V1], requested=("nope",))
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "nothing_to_remove" and fake.events == []
    assert removals._latest("synRS").set_index("key").loc["nope", "status"] == "not_found"
    assert removals.requested("synRS") == {}
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]
    assert log.unresolved_pending("synRS") == []


def test_crash_between_the_store_and_the_log_is_recovered_and_claimed(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])

    class Dying(rs.CsvWriteLog):
        calls = 0

        def removed(self, *a, **k):
            Dying.calls += 1
            if Dying.calls == 1:  # the run's own log call dies; recovery's goes through
                raise RuntimeError("died after the store")
            super().removed(*a, **k)

    dying = Dying(tmp_path / "log.csv")
    with pytest.raises(RuntimeError):
        process(tmp_path, fake, dying, removals)
    assert [e[0] for e in fake.events] == ["store"] and len(fake.versions) == 2
    # the failure handler already recovered v2 through the pending row (same principal, after writtenOn)
    statuses = log._read("synRS")
    assert statuses["status"].tolist() == ["pending", "removed", "done"]  # the prefix marks it a removal store
    assert log.pipeline_map("synRS") == {2: 1} and log.removal_versions("synRS") == {2}
    assert removals.removed("synRS") == {"b": 2}
    assert removals.requested("synRS") == {}
    assert log.unresolved_pending("synRS") == []
    # and the replay of the history drops the key
    state, _, _ = rs.replay(list(enumerate(fake.versions, start=1)), log.pipeline_map("synRS"), K, (),
                            removals.removal_events("synRS"))
    assert set(state[K]) == {"a", "c"}


def test_crash_recovery_by_the_next_run_when_the_handler_could_not_run(tmp_path):
    # the process died outright: a pending row and a stored version, nothing else
    fake, log, removals = setup(tmp_path, [V1])
    log.start("synRS", 1, "dcc-removal-dead", "42")
    fake.versions.append(frame(["a", "T", '["x","y"]'], ["c", "T", ""]))
    old = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)
    text = (tmp_path / "log.csv").read_text().splitlines()
    text[1] = text[1].replace(text[1].split(",")[-2], old.isoformat())
    (tmp_path / "log.csv").write_text("\n".join(text) + "\n")
    s = process(tmp_path, fake, log, removals, run_id="dcc-removal-2")
    assert s["status"] == "revalidated" and s["claimed"] == {"b": 2} and s["owed_export"] == [2]
    assert log.pipeline_map("synRS") == {2: 1, 3: 2}
    assert removals.removed("synRS") == {"b": 2}
    assert [e[0] for e in fake.events] == ["export"]  # nothing stored again; the owed export is made


def test_removed_version_without_removals_rows_is_claimed(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])
    fake.versions.append(frame(["a", "T", '["x","y"]'], ["c", "T", ""]))
    log.removed("synRS", 2, 1, "dcc-removal-dead", "42", 2)  # logged, but the removals rows were never written
    s = process(tmp_path, fake, log, removals)
    assert s["claimed"] == {"b": 2} and removals.removed("synRS") == {"b": 2}


def test_main_skips_recordsets_without_requested_keys(tmp_path, capsys):
    fake, log, removals = setup(tmp_path, [V1])
    removals.append("synDONE", "x", "removed", removed_in_version=3)
    assert removals.recordset_ids() == ["synDONE", "synRS"]
    with mock.patch.object(cmd.recordsets, "recordset_info", fake.info), \
         mock.patch.object(cmd.recordsets, "read_recordset", fake.read), \
         mock.patch.object(cmd.recordsets, "open_grid_sessions", fake.open_sessions):
        cmd.main(["--removals-csv", str(tmp_path / "removals.csv"), "--write-log-csv", str(tmp_path / "log.csv"),
                  "--report-dir", str(tmp_path / "rep"), "--recordset", "synRS"], syn=object())
    out = capsys.readouterr().out
    assert "synRS: dryrun" in out and "synDONE" not in out


def test_run_id_without_the_removal_prefix_is_rejected(tmp_path):
    with pytest.raises(SystemExit):
        cmd.get_args(["--removals-csv", "r.csv", "--write-log-csv", "l.csv", "--report-dir", "x", "--run-id", "mine"])
    assert cmd.get_args(["--removals-csv", "r.csv", "--write-log-csv", "l.csv",
                         "--report-dir", "x"]).run_id.startswith("dcc-removal-")


def test_recover_pending_logs_a_removal_runs_store_as_removed_and_its_export_as_recovered(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("synRS", 1, "dcc-removal-x", "42")
    log.start("synRS2", 1, "pipeline-run", "42")
    meta = lambda v: ("42", pd.Timestamp.now(tz="UTC"))
    found = rs.recover_pending("syn", "synRS", log, 3, meta=meta, skip_younger_than=None)
    assert dict(found) == {2: 1, 3: 1} and found.removal == {2}
    assert log._read("synRS")["status"].tolist() == ["pending", "removed", "recovered", "done"]
    found = rs.recover_pending("syn", "synRS2", log, 3, meta=meta)
    assert found.removal == set() and log._read("synRS2")["status"].tolist() == ["pending", "recovered", "recovered"]
    # a run that already logged its removed version: what is left is its export
    log.start("synRS3", 1, "dcc-removal-y", "42")
    log.removed("synRS3", 2, 1, "dcc-removal-y", "42", 5)
    found = rs.recover_pending("syn", "synRS3", log, 3, meta=meta)
    assert dict(found) == {3: 1} and found.removal == set()
    assert log.removal_versions("synRS3") == {2}


def test_removals_rows_are_appended_in_one_write_each(tmp_path):
    fake, log, removals = setup(tmp_path, [V1], requested=("a", "b", "zzz"))
    writes = []
    orig = rs.CsvRemovals._append_rows
    with mock.patch.object(rs.CsvRemovals, "_append_rows", lambda self, rows: (writes.append(len(rows)), orig(self, rows))):
        process(tmp_path, fake, log, removals)
    assert writes == [1, 2]  # not_found (zzz), then both removed keys together


def test_crash_after_the_removals_rows_but_before_the_export_owes_a_revalidation(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])

    def dying_export(syn, rs_id, on_version=None, **kw):
        fake.events.append(("export-died", None))
        raise RuntimeError("died before the export created a version")

    fake.export = dying_export
    with pytest.raises(RuntimeError):
        process(tmp_path, fake, log, removals, run_id="dcc-removal-1")
    assert log.removal_versions("synRS") == {2} and removals.removed("synRS") == {"b": 2}
    assert log.unexported_removals("synRS") == [2]
    # a later run with nothing requested makes the export and logs it written, base = the removal version
    fake2 = FakeRS(fake.versions, log=log)
    s = process(tmp_path, fake2, log, removals, run_id="dcc-removal-2")
    assert s["status"] == "revalidated" and [e[0] for e in fake2.events] == ["export"]
    assert log.unexported_removals("synRS") == [] and log.pipeline_map("synRS") == {2: 1, 3: 2}
    assert log.unresolved_pending("synRS") == []
    # and the next run finds nothing to do
    assert process(tmp_path, FakeRS(fake2.versions, log=log), log, removals, run_id="dcc-removal-3")["message"] == "no requested keys"


def test_dry_run_reports_an_owed_export_and_writes_nothing(tmp_path):
    fake, log, removals = setup(tmp_path, [V1, frame(["a", "T", '["x","y"]'], ["c", "T", ""])], requested=())
    log.removed("synRS", 2, 1, "dcc-removal-dead", "42", 2)
    s = process(tmp_path, fake, log, removals, execute=False)
    assert s["owed_export"] == [2] and "would re-validate" in s["message"] and fake.events == []
    assert log.pipeline_map("synRS") == {2: 1}


def test_open_session_blocks_the_owed_export_too(tmp_path):
    fake, log, removals = setup(tmp_path, [V1, V1], requested=(), sessions=[SimpleNamespace(session_id="g1")])
    log.removed("synRS", 2, 1, "dcc-removal-dead", "42", 2)
    s = process(tmp_path, fake, log, removals)
    assert s["status"] == "blocked" and "best effort" in s["message"] and fake.events == []


def request(tmp_path, *argv, key_cols=(K,), execute=False):
    base = ["request", "--removals-csv", str(tmp_path / "removals.csv"), "--recordset", "synRS", "--reason", "dup"]
    with mock.patch.object(cmd.recordsets, "recordset_info", lambda syn, rs_id: (3, list(key_cols))), \
         mock.patch.object(cmd.recordsets, "principal_id", lambda syn: "42"):
        return cmd.main(base + list(argv), syn=object())


def test_request_dry_run_writes_nothing_and_execute_appends_requested_rows(tmp_path, capsys):
    assert request(tmp_path, "--key", "a", "--key", "b") == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "removals.csv").exists()
    # --execute needs the table; the CSV is test-only, so write through the backend directly
    removals = rs.CsvRemovals(tmp_path / "removals.csv")
    with mock.patch.object(cmd.recordsets, "SynapseTableRemovals", lambda syn, tid: removals), \
         mock.patch.object(cmd.recordsets, "recordset_info", lambda syn, rs_id: (3, [K])), \
         mock.patch.object(cmd.recordsets, "principal_id", lambda syn: "42"):
        code = cmd.main(["request", "--removals-table", "synREM", "--recordset", "synRS", "--key", "a", "--key", " b ",
                         "--key", "a", "--reason", "dup", "--execute"], syn=object())
        again = cmd.main(["request", "--removals-table", "synREM", "--recordset", "synRS", "--key", "a",
                          "--reason", "dup", "--execute"], syn=object())
    assert code == 0 and again == 0
    assert removals.requested("synRS") == {"a": {"reason": "dup", "requestedBy": "42"},
                                           "b": {"reason": "dup", "requestedBy": "42"}}
    assert len(pd.read_csv(tmp_path / "removals.csv")) == 2  # the repeat of a was skipped both times


def test_request_composite_key_values_are_joined_per_key_column(tmp_path):
    removals = rs.CsvRemovals(tmp_path / "removals.csv")
    with mock.patch.object(cmd.recordsets, "SynapseTableRemovals", lambda syn, tid: removals), \
         mock.patch.object(cmd.recordsets, "recordset_info", lambda syn, rs_id: (3, ["g", "t"])), \
         mock.patch.object(cmd.recordsets, "principal_id", lambda syn: "42"):
        assert cmd.main(["request", "--removals-table", "synREM", "--recordset", "synRS", "--key", "G1", "T1",
                         "--reason", "dup", "--requested-by", "me", "--execute"], syn=object()) == 0
        assert cmd.main(["request", "--removals-table", "synREM", "--recordset", "synRS", "--key", "G1",
                         "--reason", "dup", "--execute"], syn=object()) == 1  # one value for two key columns
    assert list(removals.requested("synRS")) == [rs.KEY_SEP.join(["G1", "T1"])]
    assert removals.requested("synRS")[rs.KEY_SEP.join(["G1", "T1"])]["requestedBy"] == "me"


def test_request_skips_a_key_that_is_already_removed(tmp_path, capsys):
    removals = rs.CsvRemovals(tmp_path / "removals.csv")
    removals.append("synRS", "a", "requested")
    removals.append("synRS", "a", "removed", removed_in_version=2)
    assert request(tmp_path, "--key", "a", "--key", "b") == 0  # dry run
    out = capsys.readouterr().out
    assert "already removed (v2)" in out and "request 1 key(s) ['b']" in out
    with mock.patch.object(cmd.recordsets, "SynapseTableRemovals", lambda syn, tid: removals), \
         mock.patch.object(cmd.recordsets, "recordset_info", lambda syn, rs_id: (3, [K])), \
         mock.patch.object(cmd.recordsets, "principal_id", lambda syn: "42"):
        cmd.main(["request", "--removals-table", "synREM", "--recordset", "synRS", "--key", "a",
                  "--reason", "dup", "--execute"], syn=object())
    assert removals.removed("synRS") == {"a": 2} and removals.requested("synRS") == {}
    assert len(pd.read_csv(tmp_path / "removals.csv")) == 2  # nothing appended
