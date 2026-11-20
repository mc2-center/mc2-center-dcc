"""annotations/rekey_recordset_rows.py with every Synapse call stubbed."""
import csv
import io
from unittest import mock

import pytest

import recordsets as rs
from conftest import load_script
from fakes import FakeRS, FakeSynapse, assert_nothing_written

cmd = load_script("annotations/rekey_recordset_rows.py", "rekey_recordset_rows")
pytestmark = pytest.mark.usefixtures("synapse_tables")

K = "DatasetViewId"
V1 = (
    f"{K},DatasetTitle,DatasetAlias\n"
    "syn1,First,a\n"
    "syn2,Second,b\n"
    "syn3,Third,\n"
)


def fake_rs(key_cols=(K,), **kw):
    return FakeRS([V1], key_cols, **kw)


def run(tmp_path, fake, changes, execute=False, log=None):
    (tmp_path / "changes.csv").write_text(changes, encoding="utf-8")
    argv = ["--changes", str(tmp_path / "changes.csv"), "--component", "DatasetView",
            "--write-log-table", "synLOG", "--removals-table", "synREM",
            "--report-dir", str(tmp_path / "rep"), "--run-id", "dcc-rekey-1"]
    if execute:
        argv.append("--execute")
    with mock.patch.object(cmd.recordsets, "recordset_info", fake.info), \
         mock.patch.object(cmd.recordsets, "_download_version_file", fake.download), \
         mock.patch.object(cmd.recordsets, "read_recordset", fake.read), \
         mock.patch.object(cmd.recordsets, "store_new_version", fake.store), \
         mock.patch.object(cmd.recordsets, "export_via_grid", fake.export), \
         mock.patch.object(cmd.recordsets, "recordset_version_meta", fake.meta), \
         mock.patch.object(cmd.recordsets, "principal_id", lambda syn: "42"):
        return cmd.main(argv, syn=object())


def read_csv(text):
    return list(csv.DictReader(io.StringIO(text, newline="")))


def summary(tmp_path):
    return {r["recordset"]: r for r in read_csv((tmp_path / "rep" / "dcc-rekey-1" / "summary.csv").read_text())}


def test_dry_run_changes_only_key_cells_and_stores_nothing(tmp_path, log, removals, capsys):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\nsynA,syn3,syn30\n", log=log) == 0
    assert "synA: dryrun" in capsys.readouterr().out
    assert_nothing_written(f, log, removals)
    rep = tmp_path / "rep" / "dcc-rekey-1" / "synA"
    rows = read_csv((rep / "rekeyed.csv").read_text(encoding="utf-8"))
    assert rows == [
        {K: "syn10", "DatasetTitle": "First", "DatasetAlias": "a"},
        {K: "syn2", "DatasetTitle": "Second", "DatasetAlias": "b"},
        {K: "syn30", "DatasetTitle": "Third", "DatasetAlias": ""},
    ]
    assert (rep / "changes.csv").exists()
    assert summary(tmp_path)["synA"]["status"] == "dryrun"


def test_dry_run_does_not_recover_an_aged_pending_row(tmp_path, log, removals):
    log.start("synA", 1, "dcc-rekey-dead", "42")
    log.rows[0]["writtenOn"] = rs.pd.Timestamp.now(tz="UTC") - rs.pd.Timedelta(hours=3)
    before = list(log.rows)
    f = fake_rs(log=log)
    assert run(tmp_path, FakeSynapse(log, synA=f), "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log) == 0
    assert summary(tmp_path)["synA"]["status"] == "dryrun"
    assert_nothing_written(f, removals=removals)
    assert log.rows == before  # an execute run would log the row ``aborted``


def test_new_key_already_present_refuses_that_recordset_only(tmp_path, log, capsys):
    a, b = fake_rs(log=log), fake_rs(log=log)
    fake = FakeSynapse(log, synA=a, synB=b)
    changes = "recordset,old_key,new_key\nsynA,syn1,syn2\nsynA,syn3,syn30\nsynB,syn1,syn10\n"
    assert run(tmp_path, fake, changes, log=log) == 1
    s = summary(tmp_path)
    assert s["synA"]["status"] == "refused" and s["synB"]["status"] == "dryrun"
    refused = read_csv((tmp_path / "rep" / "dcc-rekey-1" / "synA" / "refused.csv").read_text())
    assert [(r["old_key"], r["reason"]) for r in refused] == [("syn1", "new_key is already a row")]
    assert not (tmp_path / "rep" / "dcc-rekey-1" / "synA" / "rekeyed.csv").exists()  # all or nothing
    assert (tmp_path / "rep" / "dcc-rekey-1" / "synB" / "rekeyed.csv").exists()


def test_two_changes_to_the_same_new_key_are_refused(tmp_path, log):
    fake = FakeSynapse(log, synA=fake_rs(log=log))
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn9\nsynA,syn2,syn9\n", log=log) == 1
    refused = read_csv((tmp_path / "rep" / "dcc-rekey-1" / "synA" / "refused.csv").read_text())
    assert {r["reason"] for r in refused} == {"new_key is the target of two changes"}


def test_other_invalid_changes_are_refused(tmp_path, log):
    fake = FakeSynapse(log, synA=fake_rs(log=log))
    changes = "recordset,old_key,new_key\nsynA,syn9,syn10\nsynA,syn1,\nsynA,syn2,syn2\n"
    assert run(tmp_path, fake, changes, log=log) == 1
    refused = read_csv((tmp_path / "rep" / "dcc-rekey-1" / "synA" / "refused.csv").read_text())
    assert [r["reason"] for r in refused] == ["old_key is not a row", "new_key is empty", "new_key equals old_key"]


def test_multi_column_upsert_key_is_refused(tmp_path, log, capsys):
    f = fake_rs(key_cols=(K, "DatasetTitle"), log=log)
    fake = FakeSynapse(log, synA=f)
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log) == 1
    assert "need exactly one" in capsys.readouterr().out
    assert summary(tmp_path)["synA"]["status"] == "refused"


def test_swap_within_a_run_is_allowed(tmp_path, log):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn2\nsynA,syn2,syn1\n", log=log) == 0
    rows = read_csv((tmp_path / "rep" / "dcc-rekey-1" / "synA" / "rekeyed.csv").read_text(encoding="utf-8"))
    assert [(r[K], r["DatasetTitle"]) for r in rows] == [("syn2", "First"), ("syn1", "Second"), ("syn3", "Third")]


def test_locked_recordset_is_reported_and_not_written(tmp_path, log):
    log.start("synA", 1, "dcc-removal-other", "7")
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log, execute=True) == 1
    assert summary(tmp_path)["synA"]["status"] == "locked"
    assert_nothing_written(f)


def test_execute_logs_start_store_record_export_finish_and_writes_removals(tmp_path, log, removals, capsys):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    changes = "recordset,old_key,new_key,reason\nsynA,syn1,syn10,moved\nsynA,syn3,syn30,\n"
    assert run(tmp_path, fake, changes, log=log, execute=True) == 0
    assert "rekeyed 2 rows in v2 (re-validated in v3); matches rekeyed.csv" in capsys.readouterr().out
    assert f.events == [("store", ["pending"]), ("export", None)]
    rows = log._read("synA")
    assert rows["status"].tolist() == ["pending", "removed", "written", "done"]  # the store is a removal version
    assert rows["baseVersion"].tolist() == [1, 1, 2, 1]  # store: base = version read; export: base = store
    assert log.pipeline_map("synA") == {2: 1, 3: 2} and log.unresolved_pending("synA") == []
    assert log.removal_versions("synA") == {2} and log.unexported_removals("synA") == []
    assert "syn10,First,a" in f.versions[1] and "syn2,Second,b" in f.versions[1] and "syn1," not in f.versions[1]
    assert removals.removed("synA") == {"syn1": 2, "syn3": 2}
    reasons = {r["key"]: r["reason"] for r in removals.rows}
    assert reasons == {"syn1": "rekeyed to syn10 (dcc-rekey-1); moved", "syn3": "rekeyed to syn30 (dcc-rekey-1)"}
    s = summary(tmp_path)["synA"]
    assert s["status"] == "rekeyed" and s["version"] == "2" and s["changes"] == "2"


def test_execute_aborts_the_pending_row_when_the_store_fails(tmp_path, log, removals):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)

    def boom(syn, rs_id, path):
        raise RuntimeError("store failed")
    fake.store = boom
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log, execute=True) == 1
    assert log._read("synA")["status"].tolist() == ["pending", "aborted"]
    assert summary(tmp_path)["synA"]["status"] == "failed" and removals.rows == []


def test_run_id_without_the_prefix_is_rejected():
    with pytest.raises(SystemExit):
        cmd.get_args(["--changes", "c.csv", "--component", "DatasetView", "--write-log-table", "synLOG",
                      "--report-dir", "r", "--run-id", "dcc-repair-1"])


def test_replay_drops_the_old_key_and_adds_the_new_one(tmp_path, log):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log, execute=True) == 0
    versions = [(i, rs.pd.read_csv(io.StringIO(t), dtype=str, keep_default_na=False))
                for i, t in enumerate(f.versions, start=1)]
    log_map = {1: 0, **log.pipeline_map("synA")}
    state, _, _ = rs.replay(versions, log_map, K, removal_versions=log.removal_versions("synA"))
    assert sorted(state[K]) == ["syn10", "syn2", "syn3"]


def test_removals_are_written_before_the_export_so_a_failed_export_keeps_them(tmp_path, log, removals):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)

    def boom(syn, rs_id, on_version=None, **kw):
        raise RuntimeError("export failed")
    fake.export = boom
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log, execute=True) == 1
    assert removals.removed("synA") == {"syn1": 2}
    s = summary(tmp_path)["synA"]
    assert s["status"] == "failed" and "v2 was stored" in s["message"]


def test_a_failed_removals_append_is_reported_with_the_stored_version(tmp_path, log, removals):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    with mock.patch.object(type(removals), "append_many", side_effect=RuntimeError("table down")):
        assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn10\n", log=log, execute=True) == 1
    s = summary(tmp_path)["synA"]
    assert s["status"] == "failed" and "v2 was stored" in s["message"] and "removals rows" in s["message"]


def test_swap_and_chain_write_no_removals_row_for_a_reused_key(tmp_path, log, removals):
    f = fake_rs(log=log)
    fake = FakeSynapse(log, synA=f)
    changes = "recordset,old_key,new_key\nsynA,syn1,syn2\nsynA,syn2,syn20\n"  # syn2 is live again
    assert run(tmp_path, fake, changes, log=log, execute=True) == 0
    assert removals.removed("synA") == {"syn1": 2}  # none for syn2


def test_a_new_key_that_is_a_removed_key_is_refused(tmp_path, log, removals):
    removals.append("synA", "syn9", "removed", removed_in_version=1)
    fake = FakeSynapse(log, synA=fake_rs(log=log))
    assert run(tmp_path, fake, "recordset,old_key,new_key\nsynA,syn1,syn9\n", log=log) == 1
    refused = read_csv((tmp_path / "rep" / "dcc-rekey-1" / "synA" / "refused.csv").read_text())
    assert [r["reason"] for r in refused] == ["new_key is a removed key"]
