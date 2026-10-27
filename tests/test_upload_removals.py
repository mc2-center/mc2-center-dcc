"""upload_recordsets.py honours DCC row removals (D-Q49); Synapse stubbed."""
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs
from test_upload_recordsets import KEY, FakeSynapse, make_inputs, run, stored, ur


def setup(tmp_path):
    """v1 p1+p2, v2 removal of p2 (direct store), v3 the export after it; the upload has p1 and p2."""
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.removed("synRS", 2, 1, "dcc-removal-1", "42", 1)
    log.record("synRS", 3, "dcc-removal-1", 1, base=2, principal="42")
    removals = rs.CsvRemovals(tmp_path / "removals.csv")
    removals.append("synRS", "p2", "requested", reason="dup")
    removals.append("synRS", "p2", "removed", removed_in_version=2, run_id="dcc-removal-1")
    v1 = stored(["p1", "Old", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Old", ""])
    return log, removals, FakeSynapse([v1, v2, v2])


def merged_keys(rep):
    return set(pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)[KEY])


def test_upload_skips_removed_keys_and_reports_them(tmp_path, caplog):
    log, removals, fake = setup(tmp_path)
    with caplog.at_level("INFO"):
        code, rep = run(tmp_path, fake, "--removals-csv", str(tmp_path / "removals.csv"), execute=False,
                        log=tmp_path / "log.csv")
    assert code == 0
    skipped = pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)
    assert list(skipped[KEY]) == ["p2"] and list(skipped["removedInVersion"]) == ["2"]
    assert merged_keys(rep) == {"p1"}
    assert "removal_skipped=1" in (rep / "syn100" / "summary.txt").read_text()
    assert "1 upload row(s) skipped" in caplog.text


def test_removal_the_table_does_not_list_is_skipped_from_the_log_and_history(tmp_path):
    log, removals, fake = setup(tmp_path)
    code, rep = run(tmp_path, fake, execute=False, log=tmp_path / "log.csv")  # no removals source at all
    assert merged_keys(rep) == {"p1"}
    assert list(pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)[KEY]) == ["p2"]


def test_a_dead_removal_runs_store_is_recovered_as_removed_and_not_written_back(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.start("synRS", 1, "dcc-removal-dead", "42")
    old = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)
    text = (tmp_path / "log.csv").read_text().splitlines()
    text[2] = text[2].replace(text[2].split(",")[-2], old.isoformat())
    (tmp_path / "log.csv").write_text("\n".join(text) + "\n")
    v1 = stored(["p1", "Old", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Old", ""])  # the dead run's store: nothing logged, nothing in the removals table
    fake = FakeSynapse([v1, v2])
    with mock.patch.object(ur.recordsets, "recordset_version_meta",
                           lambda syn, rs_id, v: ("42", pd.Timestamp.now(tz="UTC"))):
        code, rep = run(tmp_path, fake, log=tmp_path / "log.csv")
    assert code == 0
    assert log.removal_versions("synRS") == {2}  # logged removed, not recovered
    assert set(fake.writes[-1][KEY]) == {"p1"}  # p2 was neither kept in the state nor uploaded
    assert list(pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)[KEY]) == ["p2"]


def test_execute_without_a_removals_source_is_refused(tmp_path):
    paths = make_inputs(tmp_path)
    argv = ["--paths-csv", str(paths), "--component", "PublicationView", "--report-dir", str(tmp_path / "r"),
            "--write-log-table", "synLOG", "--execute"]
    with pytest.raises(SystemExit):
        ur.get_args(argv)
    assert ur.get_args(argv + ["--removals-table", "synREM"]).removals_table == "synREM"


def test_restored_key_is_uploaded_normally(tmp_path):
    log, removals, fake = setup(tmp_path)
    removals.append("synRS", "p2", "restored")
    code, rep = run(tmp_path, fake, "--removals-csv", str(tmp_path / "removals.csv"), execute=False,
                    log=tmp_path / "log.csv")
    assert merged_keys(rep) == {"p1", "p2"}
    assert pd.read_csv(rep / "syn100" / "removal_skipped.csv").empty
    assert "removal_skipped" not in (rep / "syn100" / "summary.txt").read_text()
