"""annotations/fix_recordset_cells.py with every Synapse call stubbed."""
import importlib.util
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("fix_recordset_cells", ROOT / "annotations" / "fix_recordset_cells.py")
fx = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fx)

K = "DatasetViewId"
URL = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE1"


def current(species_a=("Mouse", "Rat")):
    return pd.DataFrame(
        [["syn1", list(species_a), "http://old", "alias"], ["syn2", ["Human"], "http://two", ""]],
        columns=[K, "Species", "DatasetUrl", "DatasetAlias"]).astype(object)


def changes(tmp_path, *rows):
    p = tmp_path / "changes.csv"
    pd.DataFrame(rows, columns=fx.CHANGE_COLUMNS).to_csv(p, index=False)
    return p


def run(tmp_path, *rows, extra=(), cur=None, projects=None):
    calls = []
    argv = ["--changes", str(changes(tmp_path, *rows)), "--report-dir", str(tmp_path / "rep"),
            "--run-id", "dcc-fixcells-t", *extra]
    cur = current() if cur is None else cur
    projects = {"CA1": "syn100", "CA2": "syn200"} if projects is None else projects
    with mock.patch.object(fx, "grant_projects", lambda syn: projects), \
         mock.patch.object(fx, "find_folder", lambda syn, project, component: "syn9" + project[3:]), \
         mock.patch.object(fx.recordsets, "find_recordset", lambda syn, folder, component: "synRS" + folder[-3:]), \
         mock.patch.object(fx.recordsets, "recordset_info", lambda syn, rs_id: (3, [K])), \
         mock.patch.object(fx.recordsets, "read_recordset", lambda syn, rs_id, list_cols=(): cur.copy()), \
         mock.patch.object(fx, "run_upload", lambda a, syn: calls.append(a) or 0):
        code = fx.main(argv, syn=object())
    return code, calls, tmp_path / "rep" / "dcc-fixcells-t" / "fix_cells"


def row(grant="CA1", key="syn1", column="Dataset Url", value=URL):
    return ["DatasetView", grant, key, column, value]


def test_upload_file_has_key_and_changed_columns_only(tmp_path):
    code, calls, out = run(tmp_path, row(column="Dataset Url"), row(column="Species", value="Mouse"))
    assert code == 0
    up = pd.read_csv(out / "files" / "CA1.csv", dtype=str, keep_default_na=False)
    assert up.to_dict("records") == [{"DatasetView_id": "syn1", "Dataset Url": URL, "Species": "Mouse"}]


def test_unset_cells_keep_current_values_and_lists_are_joined(tmp_path):
    # syn1 gets a new Species; syn2 is in the file for Dataset Url only, so its Species must stay
    code, calls, out = run(tmp_path, row(column="Species", value="Cat"), row(key="syn2", column="Dataset Url"))
    up = pd.read_csv(out / "files" / "CA1.csv", dtype=str, keep_default_na=False).set_index("DatasetView_id")
    assert up.loc["syn2", "Species"] == "Human" and up.loc["syn1", "Dataset Url"] == "http://old"
    cells = pd.read_csv(out / "cells.csv", dtype=str, keep_default_na=False)
    assert list(cells["current"]) == ["Mouse, Rat", "http://two"]
    assert list(cells.columns) == ["grant", "recordset", "key", "column", "current", "new"]


def test_key_name_and_display_name_columns_are_equal(tmp_path):
    _, _, a = run(tmp_path, row(column="Dataset Url"))
    ua = (a / "files" / "CA1.csv").read_text()
    _, _, b = run(tmp_path, row(column="DatasetUrl"))
    assert (b / "files" / "CA1.csv").read_text() == ua


@pytest.mark.parametrize("bad, why", [
    (row(key="syn404"), "not a row"),
    (row(column="No Such Column"), "unknown column"),
    (row(column="DatasetView_id"), "rekey_recordset_rows.py"),
    (row(column="DatasetViewId"), "rekey_recordset_rows.py"),
    (row(grant="CA9"), "Grants table"),
    (row(column="Species", value="Zzz"), "schema"),
])
def test_refusals_write_nothing(tmp_path, bad, why):
    code, calls, out = run(tmp_path, row(key="syn2"), bad)
    assert code == 1 and calls == []
    refused = pd.read_csv(out / "refused.csv", dtype=str, keep_default_na=False)
    assert len(refused) == 1 and why in refused.loc[0, "reason"]
    assert not (out / "paths.csv").exists() and not (out / "files").exists()


def test_a_cell_set_twice_differently_is_refused(tmp_path):
    code, calls, out = run(tmp_path, row(), row(value=URL + "2"))
    assert code == 1 and calls == []


def test_multi_grant_row_needs_one_line_per_grant(tmp_path):
    code, calls, out = run(tmp_path, row(grant="CA1"), row(grant="CA2"))
    assert code == 0
    paths = pd.read_csv(out / "paths.csv", dtype=str)
    assert list(paths.columns) == ["File Paths", "folderIdDatasets", "grantId"]
    assert list(paths["grantId"]) == ["syn100", "syn200"] and list(paths["folderIdDatasets"]) == ["syn9100", "syn9200"]
    assert (out / "files" / "CA1.csv").exists() and (out / "files" / "CA2.csv").exists()


def test_dry_run_by_default_and_execute_passed_through(tmp_path):
    _, calls, out = run(tmp_path, row())
    argv = calls[0]
    assert "--execute" not in argv
    assert argv[argv.index("--component") + 1] == "DatasetView"
    assert argv[argv.index("--write-log-table") + 1] == "syn78404341"
    assert argv[argv.index("--removals-table") + 1] == "syn78404342"
    assert argv[argv.index("--run-id") + 1] == "dcc-fixcells-t"
    _, calls, _ = run(tmp_path, row(), extra=["--execute"])
    assert "--execute" in calls[0]


def test_only_the_changed_cells_are_allowed_to_overwrite(tmp_path):
    _, calls, out = run(tmp_path, row(), row(key="syn2", column="Species", value="Cat"))
    argv = calls[0]
    assert argv[argv.index("--overwrite-csv") + 1] == str(out / "overwrite.csv")
    allow = pd.read_csv(out / "overwrite.csv", dtype=str)
    assert allow.to_dict("records") == [
        {"grantId": "syn100", "key": "syn1", "column": "DatasetUrl"},
        {"grantId": "syn100", "key": "syn2", "column": "Species"}]
