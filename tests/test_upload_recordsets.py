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
    """Versions of one RecordSet as DataFrames; Grid writes append a version."""

    def __init__(self, versions, find=None, key=(KEY,), drift=None):
        self.versions = list(versions)
        self.find = find if find is not None else (lambda folder: "synRS")
        self.key = list(key)
        self.extra_exports = 0
        self.drift = drift  # frame appended after our write, simulating a foreign edit
        self.writes = []

    def find_recordset(self, syn, folder, component):
        return self.find(folder)

    def info(self, syn, rs_id):
        return len(self.versions), self.key

    def read(self, syn, rs_id, version=None, cache_dir=None):
        return self.versions[(version or len(self.versions)) - 1].copy()

    def write(self, syn, rs_id, csv_path, **kw):
        written = pd.read_csv(csv_path, dtype=str, keep_default_na=False).map(rs.parse_cell)
        self.writes.append(written)
        self.versions.append(written)
        ours = [len(self.versions)]
        for _ in range(self.extra_exports):
            self.versions.append(written)
            ours.append(len(self.versions))
        if self.drift is not None:
            self.versions.append(self.drift)
        return ours

    def validation(self, syn, rs_id, version, loc, keys):
        return pd.DataFrame({"key": keys, "row_index": range(len(keys)), "is_valid": [True] * len(keys),
                             "validation_error_message": [""] * len(keys),
                             "all_validation_messages": [""] * len(keys)})


def run(tmp_path, fake, *extra, execute=True, log=None, grants=("syn100",)):
    paths = make_inputs(tmp_path, grants)
    log = log or tmp_path / "log.csv"
    argv = ["--paths-csv", str(paths), "--component", "PublicationView", "--write-log-csv", str(log),
            "--report-dir", str(tmp_path / "reports"), "--run-id", "r1", *extra]
    if execute:
        argv.append("--execute")
    with mock.patch.object(ur.recordsets, "find_recordset", fake.find_recordset), \
         mock.patch.object(ur.recordsets, "recordset_info", fake.info), \
         mock.patch.object(ur.recordsets, "read_recordset", fake.read), \
         mock.patch.object(ur.recordsets, "write_via_grid", fake.write), \
         mock.patch.object(ur.recordsets, "get_validation", fake.validation):
        code = ur.main(argv, syn=object())
    return code, tmp_path / "reports" / "r1"


def stored(*rows):
    return pd.DataFrame(list(rows), columns=[KEY, "PublicationTitle", "Assay"])


def test_first_write_is_plain_upsert_and_logged(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ["RNA Sequencing"]], ["p9", "Other", ""])])
    code, rep = run(tmp_path, fake)
    assert code == 0
    written = fake.writes[0].set_index(KEY)
    assert written.loc["p1", "PublicationTitle"] == "Pipeline title"
    assert written.loc["p1", "Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert "p9" in written.index and "p2" in written.index
    assert rs.CsvWriteLog(tmp_path / "log.csv").pipeline_versions("synRS") == [2]
    assert (rep / "syn100" / "validation.csv").exists()
    assert "written" in (rep / "syn100" / "summary.txt").read_text()
    assert pd.read_csv(rep / "syn100" / "decisions.csv").empty


def test_reconcile_and_write_keeps_contributor_edit_and_reports_conflict(tmp_path):
    v1 = stored(["p1", "Pipeline title", ["Mass Spectrometry", "RNA Sequencing"]], ["p2", "Second", ""])
    v2 = stored(["p1", "Contributor title", ["Mass Spectrometry", "RNA Sequencing"]], ["p2", "Second", ""],
                ["p3", "Added by contributor", ""])
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    fake = FakeSynapse([v1, v2])
    code, rep = run(tmp_path, fake, log=log)
    assert code == 0
    written = fake.writes[0].set_index(KEY)
    assert written.loc["p1", "PublicationTitle"] == "Contributor title"
    assert "p3" in written.index
    decisions = pd.read_csv(rep / "syn100" / "decisions.csv", dtype=str, keep_default_na=False)
    assert set(decisions["decision"]) == {"kept_edit", "kept_contributor_row"}
    conflicts = pd.read_csv(rep / "syn100" / "conflicts.csv", dtype=str, keep_default_na=False)
    assert conflicts.to_dict("records") == [
        {"key": "p1", "column": "PublicationTitle", "contributor_value": "Contributor title",
         "upload_value": "Pipeline title"}]
    assert rs.CsvWriteLog(log).pipeline_versions("synRS") == [1, 3]


def test_dry_run_writes_nothing_but_reports(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    code, rep = run(tmp_path, fake, execute=False)
    assert code == 0 and fake.writes == []
    assert not (tmp_path / "log.csv").exists()
    assert not (rep / "syn100" / "validation.csv").exists()
    assert (rep / "syn100" / "merged.csv").exists()
    assert "dryrun" in (rep / "syn100" / "summary.txt").read_text()


def test_missing_recordset_stops_that_grant_only(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])], find=lambda folder: None if folder == "syn1009" else "synRS")
    code, rep = run(tmp_path, fake, grants=("syn100", "syn200"))
    assert code == 1
    assert "missing_recordset" in (rep / "syn100" / "summary.txt").read_text()
    assert "written" in (rep / "syn200" / "summary.txt").read_text()
    assert len(fake.writes) == 1


def test_verification_mismatch_reported(tmp_path):
    drift = stored(["p1", "Someone else", ""])
    fake = FakeSynapse([stored(["p1", "Old", ""])], drift=drift)
    code, rep = run(tmp_path, fake)
    text = (rep / "syn100" / "summary.txt").read_text()
    assert "verification_mismatch" in text and ur.CHANGED_DURING_WRITE in text
    assert (rep / "syn100" / "verification.txt").exists()
    assert code == 0  # self-heals on the next run; not a failed grant


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
    assert "failed" in (rep / "syn100" / "summary.txt").read_text()


def test_log_ahead_of_recordset_fails(tmp_path):
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 5, "old", 2)
    code, rep = run(tmp_path, FakeSynapse([stored(["p1", "Old", ""])]), log=log)
    assert code == 1


def test_every_export_is_logged(tmp_path):
    fake = FakeSynapse([stored(["p1", "Old", ""])])
    fake.extra_exports = 1
    code, _ = run(tmp_path, fake)
    assert code == 0
    assert rs.CsvWriteLog(tmp_path / "log.csv").pipeline_versions("synRS") == [2, 3]


def test_stale_session_scenario_keeps_wiped_edit(tmp_path):
    v5 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v6 = stored(["p1", "A edit", ""], ["p2", "Second", ""])
    v7 = stored(["p1", "Pipeline title", ""], ["p2", "B edit", ""])  # B's stale export wiped A's edit
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)  # the pipeline's last write is v1 (= v5 above)
    fake = FakeSynapse([v5, v6, v7])
    code, rep = run(tmp_path, fake, log=log)
    assert code == 0
    written = fake.writes[0].set_index(KEY)
    assert written.loc["p1", "PublicationTitle"] == "A edit"
    assert written.loc["p2", "PublicationTitle"] == "B edit"
    decisions = pd.read_csv(rep / "syn100" / "decisions.csv", dtype=str, keep_default_na=False)
    assert set(decisions["version"]) == {"2", "3"}


def test_non_schema_columns_are_not_uploaded(tmp_path):
    # A Grid import drops columns that aren't schema properties (Component),
    # so they must not reach the merge or the verification.
    assert "Component" not in rs.schema_columns("PublicationView")
    assert "PublicationTitle" in rs.schema_columns("PublicationView")
    code, rep = run(tmp_path, FakeSynapse([stored(["p1", "Old", ""])]), execute=False)
    merged = pd.read_csv(rep / "syn100" / "merged.csv", dtype=str)
    assert "Component" not in merged.columns


def test_contributor_value_survives_later_runs(tmp_path):
    """Run N keeps the edit and reports a conflict; run N+1 with the same upload does too."""
    v1 = stored(["p1", "Pipeline title", ""], ["p2", "Second", ""])
    v2 = stored(["p1", "Contributor title", ""], ["p2", "Second", ""])
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    fake = FakeSynapse([v1, v2])
    for n in (1, 2):
        code, rep = run(tmp_path, fake, log=log)
        assert code == 0
        written = fake.writes[-1].set_index(KEY)
        assert written.loc["p1", "PublicationTitle"] == "Contributor title", f"run {n}"
        conflicts = pd.read_csv(rep / "syn100" / "conflicts.csv", dtype=str, keep_default_na=False)
        assert list(conflicts["column"]) == ["PublicationTitle"], f"run {n}"
    assert rs.CsvWriteLog(log).pipeline_versions("synRS") == [1, 3, 4]


def test_pipeline_originated_value_is_overwritten(tmp_path):
    v1 = stored(["p1", "Old pipeline", ""], ["p2", "Second", ""])
    log = tmp_path / "log.csv"
    rs.CsvWriteLog(log).record("synRS", 1, "old", 2)
    fake = FakeSynapse([v1])
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
    for _ in range(2):
        run(tmp_path, fake, log=log)
        written = fake.writes[-1].set_index(KEY)
        assert written.loc["p1", "PublicationTitle"] == "A edit"
        assert written.loc["p2", "PublicationTitle"] == "B edit"
