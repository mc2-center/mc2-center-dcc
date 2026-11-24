"""upload_recordsets.py honours DCC row removals (D-Q49); Synapse stubbed."""
from unittest import mock

import pandas as pd
import pytest

from test_upload_recordsets import KEY, FakeSynapse, run, stored, ur

pytestmark = pytest.mark.usefixtures("synapse_tables")


def setup(log, removals):
    """v1 p1+p2, v2 removal of p2 (direct store), v3 the export after it; the upload has p1 and p2."""
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.removed("synRS", 2, 1, "dcc-removal-1", "42", 1)
    log.record("synRS", 3, "dcc-removal-1", 1, base=2, principal="42")
    removals.append("synRS", "p2", "requested", reason="dup")
    removals.append("synRS", "p2", "removed", removed_in_version=2, run_id="dcc-removal-1")
    v1 = stored(["p1", "Old", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Old", ""])
    return FakeSynapse([v1, v2, v2])


def merged_keys(rep):
    return set(pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)[KEY])


def test_upload_skips_removed_keys_and_reports_them(tmp_path, log, removals, caplog):
    fake = setup(log, removals)
    log_rows, removals_rows = len(log.rows), len(removals.rows)
    with caplog.at_level("INFO"):
        code, rep = run(tmp_path, fake, "--removals-table", "synREM", execute=False)
    assert code == 0
    assert len(log.rows) == log_rows and len(removals.rows) == removals_rows  # a dry run reads both, writes neither
    skipped = pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)
    assert list(skipped[KEY]) == ["p2"] and list(skipped["removedInVersion"]) == ["2"]
    assert merged_keys(rep) == {"p1"}
    assert "removal_skipped=1" in (rep / "syn100" / "summary.txt").read_text()
    assert "1 upload row(s) skipped" in caplog.text


def test_removal_the_table_does_not_list_is_skipped_from_the_log_and_history(tmp_path, log, removals):
    fake = setup(log, removals)
    code, rep = run(tmp_path, fake, execute=False)  # the removals table is empty
    assert merged_keys(rep) == {"p1"}
    assert list(pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)[KEY]) == ["p2"]


def test_a_dead_removal_runs_store_is_recovered_as_removed_and_not_written_back(tmp_path, log):
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.start("synRS", 1, "dcc-removal-dead", "42")
    log.rows[1]["writtenOn"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=3)
    v1 = stored(["p1", "Old", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Old", ""])  # the dead run's store: nothing logged, nothing in the removals table
    fake = FakeSynapse([v1, v2])
    with mock.patch.object(ur.recordsets, "recordset_version_meta",
                           lambda syn, rs_id, v: ("42", pd.Timestamp.now(tz="UTC"))):
        code, rep = run(tmp_path, fake)
    assert code == 0
    assert log.removal_versions("synRS") == {2}  # logged removed, not recovered
    assert set(fake.writes[-1][KEY]) == {"p1"}  # p2 was neither kept in the state nor uploaded
    assert list(pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)[KEY]) == ["p2"]


def test_the_tables_default_to_the_production_ones(tmp_path):
    argv = ["--paths-csv", "p.csv", "--component", "PublicationView", "--report-dir", str(tmp_path / "r")]
    args = ur.get_args(argv)
    assert (args.write_log_table, args.removals_table) == ("syn78404341", "syn78404342")
    assert ur.get_args(argv + ["--removals-table", "synREM"]).removals_table == "synREM"


def test_restored_key_is_uploaded_normally(tmp_path, log, removals):
    fake = setup(log, removals)
    removals.append("synRS", "p2", "restored")
    code, rep = run(tmp_path, fake, "--removals-table", "synREM", execute=False)
    assert merged_keys(rep) == {"p1", "p2"}
    assert pd.read_csv(rep / "syn100" / "removal_skipped.csv").empty
    assert "removal_skipped" not in (rep / "syn100" / "summary.txt").read_text()


def test_second_removal_with_missing_rows_is_not_lifted_by_the_older_restore(tmp_path, log, removals):
    """removed (v2) -> restored -> re-added (v4) -> removed again (v5), crash before the removals rows:
    the table's newest status is still ``restored``, but it predates the v5 removal."""
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.removed("synRS", 2, 1, "dcc-removal-1", "42", 1)
    log.record("synRS", 3, "dcc-removal-1", 1, base=2, principal="42")
    log.record("synRS", 4, "r1", 2, base=3, principal="42")
    log.removed("synRS", 5, 4, "dcc-removal-2", "42", 1)
    removals.append("synRS", "p2", "requested", reason="dup")
    removals.append("synRS", "p2", "removed", removed_in_version=2, run_id="dcc-removal-1")
    removals.append("synRS", "p2", "restored")
    v1 = stored(["p1", "Old", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Old", ""])
    fake = FakeSynapse([v1, v2, v2, v1, v2])
    code, rep = run(tmp_path, fake, "--removals-table", "synREM", execute=False)
    assert merged_keys(rep) == {"p1"}
    skipped = pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)
    assert list(skipped[KEY]) == ["p2"] and list(skipped["removedInVersion"]) == ["5"]
