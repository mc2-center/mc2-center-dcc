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


def process(tmp_path, fake, log, removals, execute=True, run_id="r1"):
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
                         "--report-dir", str(tmp_path / "rep"), "--run-id", "r1"], syn=object())
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
        def removed(self, *a, **k):
            raise RuntimeError("died after the store")

    dying = Dying(tmp_path / "log.csv")
    with pytest.raises(RuntimeError):
        process(tmp_path, fake, dying, removals)
    assert [e[0] for e in fake.events] == ["store"] and len(fake.versions) == 2
    # the failure handler already recovered v2 through the pending row (same principal, after writtenOn)
    statuses = log._read("synRS")
    assert statuses["status"].tolist() == ["pending", "recovered"]
    assert log.pipeline_map("synRS") == {2: 1}
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
    log.start("synRS", 1, "dead", "42")
    fake.versions.append(frame(["a", "T", '["x","y"]'], ["c", "T", ""]))
    old = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)
    text = (tmp_path / "log.csv").read_text().splitlines()
    text[1] = text[1].replace(text[1].split(",")[-2], old.isoformat())
    (tmp_path / "log.csv").write_text("\n".join(text) + "\n")
    s = process(tmp_path, fake, log, removals, run_id="r2")
    assert s["status"] == "nothing_to_remove" and s["claimed"] == {"b": 2}
    assert log.pipeline_map("synRS") == {2: 1}
    assert removals.removed("synRS") == {"b": 2}
    assert fake.events == []  # nothing stored or exported again


def test_removed_version_without_removals_rows_is_claimed(tmp_path):
    fake, log, removals = setup(tmp_path, [V1])
    fake.versions.append(frame(["a", "T", '["x","y"]'], ["c", "T", ""]))
    log.removed("synRS", 2, 1, "dead", "42", 2)  # logged, but the removals rows were never written
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
