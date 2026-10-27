"""upload_recordsets.py honours the removals table (D-Q49); Synapse stubbed."""
import pandas as pd

import recordsets as rs
from test_upload_recordsets import KEY, FakeSynapse, run, status_of, stored


def setup(tmp_path):
    """v1 p1+p2, v2 removal of p2 (direct store), v3 the export after it; the upload has p1 and p2."""
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.record("synRS", 1, "r0", 2, base=0, principal="42")
    log.removed("synRS", 2, 1, "rm", "42", 1)
    log.record("synRS", 3, "rm", 1, base=2, principal="42")
    removals = rs.CsvRemovals(tmp_path / "removals.csv")
    removals.append("synRS", "p2", "requested", reason="dup")
    removals.append("synRS", "p2", "removed", removed_in_version=2, run_id="rm")
    v1 = stored(["p1", "Old", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Old", ""])
    return log, removals, FakeSynapse([v1, v2, v2])


def test_upload_skips_removed_keys_and_reports_them(tmp_path, caplog):
    log, removals, fake = setup(tmp_path)
    with caplog.at_level("INFO"):
        code, rep = run(tmp_path, fake, "--removals-csv", str(tmp_path / "removals.csv"), execute=False,
                        log=tmp_path / "log.csv")
    assert code == 0
    skipped = pd.read_csv(rep / "syn100" / "removal_skipped.csv", dtype=str)
    assert list(skipped[KEY]) == ["p2"] and list(skipped["removedInVersion"]) == ["2"]
    assert list(pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)[KEY]) == ["p1"]
    assert "removal_skipped=1" in (rep / "syn100" / "summary.txt").read_text()
    assert "1 upload row(s) skipped" in caplog.text


def test_removed_row_is_not_restored_by_replay_when_nothing_is_uploaded_for_it(tmp_path):
    log, removals, fake = setup(tmp_path)
    code, rep = run(tmp_path, fake, "--removals-csv", str(tmp_path / "removals.csv"), execute=False,
                    log=tmp_path / "log.csv")
    assert "p2" not in set(pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)[KEY])


def test_without_the_removals_option_the_row_is_uploaded_again(tmp_path):
    log, removals, fake = setup(tmp_path)
    code, rep = run(tmp_path, fake, execute=False, log=tmp_path / "log.csv")
    assert set(pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)[KEY]) == {"p1", "p2"}
    assert not (rep / "syn100" / "removal_skipped.csv").exists()


def test_restored_key_is_uploaded_normally(tmp_path):
    log, removals, fake = setup(tmp_path)
    removals.append("synRS", "p2", "restored")
    code, rep = run(tmp_path, fake, "--removals-csv", str(tmp_path / "removals.csv"), execute=False,
                    log=tmp_path / "log.csv")
    assert set(pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)[KEY]) == {"p1", "p2"}
    assert pd.read_csv(rep / "syn100" / "removal_skipped.csv").empty
    assert "removal_skipped" not in (rep / "syn100" / "summary.txt").read_text()
