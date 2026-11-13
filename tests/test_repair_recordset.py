"""annotations/repair_recordset.py with every Synapse call stubbed."""
import importlib.util
from pathlib import Path
from unittest import mock

import pytest

import recordsets as rs

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("repair_recordset", ROOT / "annotations" / "repair_recordset.py")
cmd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cmd)

K, LIST = "PublicationViewId", "PublicationAuthors"
# v1: one doubled-quote cell (Grid's export of M"ockl L), one fine list cell, one empty
V1 = (
    f"{K},PublicationTitle,{LIST}\n"
    '1,First,"[""M""""ockl L"",""Lee K""]"\n'
    '2,Second,"[""Ng P""]"\n'
    "3,Third,\n"
)
FIX_OK = ("key,column,value\n" f'1,{LIST},"[""Möckl L"",""Lee K""]"\n')


class FakeRS:
    """Versions as stored CSV text; store and export append versions, calls are ordered in ``events``."""

    def __init__(self, v1=V1, log=None):
        self.versions = [v1]
        self.events = []
        self.log = log

    def info(self, syn, rs_id):
        return len(self.versions), [K]

    def download(self, syn, rs_id, version, dest):
        dest.write_text(self.versions[version - 1], encoding="utf-8")
        return dest

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        text = self.versions[(version or len(self.versions)) - 1]
        path = Path(self.log.path).with_name(f"read{len(self.versions)}.csv")
        path.write_text(text, encoding="utf-8")
        return rs._load_csv(path, list_cols)

    def store(self, syn, rs_id, path):
        self.events.append(("store", list(self.log._read(rs_id)["status"])))
        self.versions.append(Path(path).read_text(encoding="utf-8"))
        return len(self.versions)

    def export(self, syn, rs_id, on_version=None, **kw):
        self.events.append(("export", None))
        self.versions.append(self.versions[-1])
        if on_version:
            on_version(len(self.versions))
        return [len(self.versions)]

    def meta(self, syn, rs_id, v):
        return "42", rs.pd.Timestamp.now(tz="UTC")


def run(tmp_path, fake, fixes=FIX_OK, execute=False, log=None):
    (tmp_path / "fixes.csv").write_text(fixes, encoding="utf-8")
    argv = ["--recordset", "synRS", "--component", "PublicationView", "--fixes", str(tmp_path / "fixes.csv"),
            "--write-log-csv", str(log.path), "--report-dir", str(tmp_path / "rep"), "--run-id", "dcc-repair-1"]
    real_get_args = cmd.get_args  # --execute with a CSV write log is refused by the parser; tests need it

    def get_args(a):
        ns = real_get_args([x for x in a if x != "--execute"])
        ns.execute = "--execute" in a
        return ns
    if execute:
        argv.append("--execute")
    with mock.patch.object(cmd.recordsets, "recordset_info", fake.info), \
         mock.patch.object(cmd.recordsets, "_download_version_file", fake.download), \
         mock.patch.object(cmd.recordsets, "read_recordset", fake.read), \
         mock.patch.object(cmd.recordsets, "store_new_version", fake.store), \
         mock.patch.object(cmd.recordsets, "export_via_grid", fake.export), \
         mock.patch.object(cmd.recordsets, "recordset_version_meta", fake.meta), \
         mock.patch.object(cmd.recordsets, "principal_id", lambda syn: "42"), \
         mock.patch.object(cmd, "get_args", get_args):
        return cmd.main(argv, syn=object())


@pytest.fixture
def log(tmp_path):
    l = rs.CsvWriteLog(tmp_path / "log.csv")
    l.path = tmp_path / "log.csv"
    return l


def test_find_broken_detects_the_doubled_quote_cell():
    import csv
    import io

    rows = list(csv.DictReader(io.StringIO(V1, newline="")))
    broken = cmd.find_broken(rows, [K], {LIST})
    assert [(b["key"], b["column"]) for b in broken] == [("1", LIST)]
    assert broken[0]["value"] == '["M""ockl L","Lee K"]'


def test_dry_run_writes_nothing_and_reports(tmp_path, log, capsys):
    fake = FakeRS(log=log)
    assert run(tmp_path, fake, log=log) == 0
    assert "dry run: 1 broken cells, 1 fixes; nothing written" in capsys.readouterr().out
    assert len(fake.versions) == 1 and fake.events == [] and not log.path.exists()
    rep = tmp_path / "rep"
    assert (rep / "broken_cells.csv").read_text().splitlines()[0] == "key,column,raw_value"
    assert "Möckl L" in (rep / "corrected.csv").read_text(encoding="utf-8")
    assert (rep / "changes.csv").exists()


def test_refuses_a_broken_cell_without_a_fix(tmp_path, log, capsys):
    fake = FakeRS(log=log)
    assert run(tmp_path, fake, fixes=f"key,column,value\n2,{LIST},\"[\"\"Ng Q\"\"]\"\n", log=log) == 1
    assert "has no fix" in capsys.readouterr().out and len(fake.versions) == 1


def test_refuses_a_fix_value_containing_a_quote_or_backslash(tmp_path, log, capsys):
    fake = FakeRS(log=log)
    bad = 'key,column,value\n1,%s,"[""M\\""ockl L""]"\n' % LIST  # JSON-escaped quote inside the item
    assert run(tmp_path, fake, fixes=bad, log=log) == 1
    assert "contains" in capsys.readouterr().out and len(fake.versions) == 1


def test_refuses_a_fix_for_a_missing_row(tmp_path, log, capsys):
    fake = FakeRS(log=log)
    fixes = FIX_OK + f'9,{LIST},"[""A""]"\n'
    assert run(tmp_path, fake, fixes=fixes, log=log) == 1
    assert "no such row" in capsys.readouterr().out


def test_a_fix_for_a_healthy_cell_is_allowed(tmp_path, log, capsys):
    fake = FakeRS(log=log)
    fixes = FIX_OK + f'2,{LIST},"[""Ng Q""]"\n'
    assert run(tmp_path, fake, fixes=fixes, log=log) == 0
    assert "1 broken cells, 2 fixes" in capsys.readouterr().out


def test_locked_recordset_is_refused(tmp_path, log, capsys):
    log.start("synRS", 1, "dcc-removal-other", "7")
    fake = FakeRS(log=log)
    assert run(tmp_path, fake, log=log, execute=True) == 1
    assert "dcc-removal-other" in capsys.readouterr().out
    assert fake.events == [] and len(fake.versions) == 1


def test_execute_logs_start_store_record_export_finish_and_matches(tmp_path, log, capsys):
    fake = FakeRS(log=log)
    assert run(tmp_path, fake, log=log, execute=True) == 0
    assert "repaired 1 cells in v2 (re-validated in v3); matches the corrected file" in capsys.readouterr().out
    assert fake.events == [("store", ["pending"]), ("export", None)]
    rows = log._read("synRS")
    assert rows["status"].tolist() == ["pending", "written", "written", "done"]
    assert rows["version"].dropna().tolist() == [2, 3]
    assert rows["baseVersion"].tolist() == [1, 1, 2, 1]  # store: base = version read; export: base = store
    assert log.pipeline_map("synRS") == {2: 1, 3: 2} and log.unresolved_pending("synRS") == []
    stored = fake.versions[1]
    assert '"Möckl L"' in stored and "Ng P" in stored and "\n3,Third,\n" in stored.replace("\r\n", "\n")


def test_execute_aborts_the_pending_row_when_the_store_fails(tmp_path, log):
    fake = FakeRS(log=log)

    def boom(syn, rs_id, path):
        raise RuntimeError("store failed")
    fake.store = boom
    assert run(tmp_path, fake, log=log, execute=True) == 1
    assert log._read("synRS")["status"].tolist() == ["pending", "aborted"]
