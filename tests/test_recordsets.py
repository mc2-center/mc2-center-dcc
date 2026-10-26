"""portal_tables/recordsets.py, offline."""
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs

K = "PublicationViewId"


def df(*rows, cols=(K, "Title", "Assay")):
    return pd.DataFrame(list(rows), columns=list(cols))


def decisions_of(d):
    return {(r.key, r.column): r.decision for r in d.itertuples()}


def run(current, pipe, history=None, set_like=()):
    return rs.reconcile(current, pipe, history or [pipe], K, set_like)


def test_equal_cells_record_nothing():
    base = df(["a", "T", ["x"]])
    out, d = run(base, base)
    assert d.empty and out.equals(base)


def test_kept_edit():
    pipe = df(["a", "T", ""])
    cur = df(["a", "Edited", ""])
    out, d = run(cur, pipe)
    assert decisions_of(d) == {("a", "Title"): "kept_edit"}
    assert out.loc[0, "Title"] == "Edited"
    assert d.loc[0, "current_value"] == "Edited" and d.loc[0, "pipeline_value"] == "T"


def test_restored_revert_to_old_value():
    v1 = df(["a", "Old", ""])
    v2 = df(["a", "New", ""])  # pipeline's last write
    cur = df(["a", "Old", ""])  # contributor reverted
    out, d = run(cur, v2, [v1, v2])
    assert decisions_of(d) == {("a", "Title"): "restored_revert"}
    assert out.loc[0, "Title"] == "New"


def test_multiple_foreign_versions_edit_then_new_value_is_kept():
    v1 = df(["a", "Old", ""])
    v2 = df(["a", "New", ""])
    # foreign v3 and v4 collapse into the current content: a value never seen in history
    cur = df(["a", "Third", ""])
    out, d = run(cur, v2, [v1, v2])
    assert decisions_of(d) == {("a", "Title"): "kept_edit"}
    assert out.loc[0, "Title"] == "Third"


def test_contributor_added_row_kept():
    pipe = df(["a", "T", ""])
    cur = df(["a", "T", ""], ["b", "Mine", ""])
    out, d = run(cur, pipe)
    assert decisions_of(d) == {("b", ""): "kept_contributor_row"}
    assert set(out[K]) == {"a", "b"}


def test_missing_row_restored():
    pipe = df(["a", "T", ""], ["b", "U", ""])
    cur = df(["a", "T", ""])
    out, d = run(cur, pipe)
    assert decisions_of(d) == {("b", ""): "restored_missing"}
    assert out.set_index(K).loc["b", "Title"] == "U"


def test_list_cells_order_matters_unless_set_like():
    pipe = df(["a", "T", ["x", "y"]])
    cur = df(["a", "T", ["y", "x"]])
    _, d = run(cur, pipe)
    assert decisions_of(d) == {("a", "Assay"): "kept_edit"}
    out, d = run(cur, pipe, set_like={"Assay"})
    assert d.empty


def test_list_edit_and_empty_list_equals_blank():
    pipe = df(["a", "T", ["x"]], ["b", "T", ""])
    cur = df(["a", "T", ["x", "z"]], ["b", "T", []])
    out, d = run(cur, pipe, set_like={"Assay"})
    assert decisions_of(d) == {("a", "Assay"): "kept_edit"}
    assert out.set_index(K).loc["a", "Assay"] == ["x", "z"]


def test_column_missing_from_current_counts_as_blank():
    pipe = df(["a", "T", ["x"]])
    cur = pd.DataFrame({K: ["a"], "Title": ["T"]})
    out, d = run(cur, pipe)
    assert decisions_of(d) == {("a", "Assay"): "kept_edit"}


def test_merge_upload_updates_appends_and_reports_conflicts():
    reconciled = df(["a", "Edited", ["x"]], ["b", "B", ""], ["c", "C", ""])
    upload = df(["a", "Pipeline", ["x"]], ["b", "B2", ["q"]], ["d", "D", ""])
    merged, conflicts = rs.merge_upload(reconciled, upload, K, {("a", "Title")})
    m = merged.set_index(K)
    assert m.loc["a", "Title"] == "Edited"
    assert m.loc["b", "Title"] == "B2" and m.loc["b", "Assay"] == ["q"]
    assert m.loc["c", "Title"] == "C"  # absent from upload: untouched
    assert m.loc["d", "Title"] == "D"
    assert conflicts.to_dict("records") == [
        {"key": "a", "column": "Title", "contributor_value": "Edited", "upload_value": "Pipeline"}
    ]


def test_merge_upload_edit_equal_to_upload_is_no_conflict():
    reconciled = df(["a", "Same", ""])
    merged, conflicts = rs.merge_upload(reconciled, df(["a", "Same", ""]), K, {("a", "Title")})
    assert conflicts.empty and merged.loc[0, "Title"] == "Same"


def test_merge_upload_adds_new_columns():
    merged, _ = rs.merge_upload(
        pd.DataFrame({K: ["a"], "Title": ["T"]}), pd.DataFrame({K: ["a"], "Doi": ["10.1"]}), K, set()
    )
    assert merged.loc[0, "Doi"] == "10.1"


def test_composite_key():
    cols = ("A", "B", "V")
    cur = pd.DataFrame([["1", "x", "new"]], columns=cols)
    pipe = pd.DataFrame([["1", "x", "old"]], columns=cols)
    out, d = rs.reconcile(cur, pipe, [pipe], ["A", "B"])
    assert d.loc[0, "key"] == "1|x" and d.loc[0, "decision"] == "kept_edit"


def test_differences():
    a = df(["a", "T", ["x", "y"]])
    assert rs.differences(a, df(["a", "T", ["y", "x"]]), K, {"Assay"}) == []
    assert rs.differences(a, df(["a", "T", ["y", "x"]]), K) != []
    assert rs.differences(a, df(["b", "T", ""]), K) == ["missing key a", "unexpected key b"]


def test_parse_cell_and_csv_round_trip(tmp_path):
    assert rs.parse_cell('["A","B"]') == ["A", "B"]
    assert rs.parse_cell("A, B") == "A, B"
    assert rs.parse_cell("[not json]") == "[not json]"
    assert rs.parse_cell("") == ""
    assert rs.parse_cell(float("nan")) == ""
    frame = df(["a", "T, with comma", ["x", "y"]], ["b", "", []])
    path = tmp_path / "x.csv"
    rs.to_csv(frame, path)
    assert '"[""x"",""y""]"' in path.read_text()
    back = pd.read_csv(path, dtype=str, keep_default_na=False).map(rs.parse_cell)
    assert back.loc[0, "Assay"] == ["x", "y"] and back.loc[1, "Assay"] == ""


def test_read_recordset_parses_json_arrays(tmp_path):
    path = tmp_path / "rs.csv"
    path.write_text(f'{K},Title,Assay\na,"Hi, there","[""RNA Sequencing"",""Whole Genome Sequencing""]"\nb,,\n')
    with mock.patch.object(rs, "RecordSet") as cls:
        cls.return_value.get.return_value = SimpleNamespace(path=str(path))
        out = rs.read_recordset("syn", "syn1", version=3)
    assert cls.call_args.kwargs["version_number"] == 3
    assert out.loc[0, "Assay"] == ["RNA Sequencing", "Whole Genome Sequencing"]
    assert out.loc[0, "Title"] == "Hi, there"
    assert out.loc[1, "Title"] == "" and out.loc[1, "Assay"] == ""


def test_parse_upload_splits_list_columns():
    up = pd.DataFrame({K: ["a"], "Assay": ["RNA Sequencing, Mass Spectrometry"], "Title": ["x, y"]})
    out = rs.parse_upload(up, {"Assay"})
    assert out.loc[0, "Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert out.loc[0, "Title"] == "x, y"


def test_set_like_columns_from_schema():
    assert {"Assay", "Tissue", "TumorType"} <= rs.set_like_columns("PublicationView")
    assert "PublicationAuthors" not in rs.set_like_columns("PublicationView")
    assert "PublicationAuthors" in rs.list_columns("PublicationView")


def test_csv_write_log(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "sub" / "log.csv")
    assert log.pipeline_versions("syn1") == [] and log.last_version("syn1") is None
    log.record("syn1", 2, "run1", 10)
    log.record("syn1", 5, "run2", 12)
    log.record("syn2", 9, "run2", 1)
    assert log.pipeline_versions("syn1") == [2, 5]
    assert log.last_version("syn1") == 5
    header = (tmp_path / "sub" / "log.csv").read_text().splitlines()[0]
    assert header == "recordSetId,version,runId,writtenOn,rowsWritten"


def test_synapse_write_log_queries_and_stores():
    with mock.patch.object(rs, "Table") as table:
        table.return_value.query.return_value = pd.DataFrame({"version": [3, 1]})
        log = rs.SynapseTableWriteLog("syn", "syn99")
        assert log.pipeline_versions("syn1") == [1, 3]
        assert "recordSetId = 'syn1'" in table.return_value.query.call_args.kwargs["query"]
        log.record("syn1", 4, "r", 7)
        row = table.return_value.store_rows.call_args.kwargs["values"].iloc[0]
        assert (row.recordSetId, row.version, row.runId, row.rowsWritten) == ("syn1", 4, "r", 7)


def _grid(stats_sequence, versions):
    grid = mock.MagicMock()
    exports = iter(zip(stats_sequence, versions))

    def export(**kw):
        s, v = next(exports)
        grid.validation_summary_statistics = SimpleNamespace(number_of_unknown_children=s)
        grid.record_set_version_number = v

    grid.export_to_record_set.side_effect = export
    return grid


def test_write_via_grid_order_wait_and_cleanup():
    grid = _grid([0], [7])
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep") as sleep:
        cls.return_value.create.return_value = grid
        assert rs.write_via_grid("syn", "syn1", "x.csv", timeout=5, validation_wait=45) == [7]
    assert cls.call_args.kwargs == {"record_set_id": "syn1"}
    assert cls.return_value.create.call_args.kwargs["attach_to_previous_session"] is False
    assert grid.import_csv.call_args.kwargs["path"] == "x.csv"
    sleep.assert_called_once_with(45)
    grid.delete.assert_called_once()


def test_write_via_grid_re_exports_while_rows_unknown():
    grid = _grid([3, 0], [7, 8])
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep") as sleep:
        cls.return_value.create.return_value = grid
        assert rs.write_via_grid("syn", "syn1", "x.csv", validation_wait=45, retry_wait=10) == [7, 8]
    assert [c.args[0] for c in sleep.call_args_list] == [45, 10]


def test_write_via_grid_deletes_session_on_error():
    grid = mock.MagicMock()
    grid.import_csv.side_effect = RuntimeError("boom")
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep"):
        cls.return_value.create.return_value = grid
        with pytest.raises(RuntimeError):
            rs.write_via_grid("syn", "syn1", "x.csv")
    grid.delete.assert_called_once()
    grid.export_to_record_set.assert_not_called()


def test_get_validation_maps_row_index_to_key(tmp_path):
    results = pd.DataFrame({"row_index": [0, 1], "is_valid": [True, False],
                            "validation_error_message": ["", "bad"], "all_validation_messages": ["", "bad"]})
    with mock.patch.object(rs, "RecordSet") as cls:
        cls.return_value.get.return_value.get_detailed_validation_results.return_value = results
        out = rs.get_validation("syn", "syn1", 4, tmp_path, ["k0", "k1"])
    assert list(out["key"]) == ["k0", "k1"]


def test_reconcile_versions_stale_session_keeps_wiped_edit():
    cols = (K, "Title", "Assay")
    v5 = pd.DataFrame([["a", "Pipeline", ""], ["b", "Pipeline", ""]], columns=cols)  # pipeline's write
    v6 = pd.DataFrame([["a", "A edit", ""], ["b", "Pipeline", ""]], columns=cols)  # A's edit
    # B's stale session was opened at v5: A's edit is gone, B edited b's title and added row c
    v7 = pd.DataFrame([["a", "Pipeline", ""], ["b", "B edit", ""], ["c", "B row", ""]], columns=cols)
    state, d, edited = rs.reconcile_versions([(6, v6), (7, v7)], v5, [v5], K)
    s = state.set_index(K)
    assert s.loc["a", "Title"] == "A edit"
    assert s.loc["b", "Title"] == "B edit"
    assert s.loc["c", "Title"] == "B row"
    got = {(r.version, r.key, r.column, r.decision) for r in d.itertuples()}
    assert got == {
        (6, "a", "Title", "kept_edit"),
        (7, "a", "Title", "restored_revert"),
        (7, "b", "Title", "kept_edit"),
        (7, "c", "", "kept_contributor_row"),
    }
    assert edited == {("a", "Title"), ("b", "Title")}
    # the pipeline's own upload does not overwrite either edit
    _, conflicts = rs.merge_upload(state, v5, K, edited)
    assert {(c.key, c.column) for c in conflicts.itertuples()} == {("a", "Title"), ("b", "Title")}


def test_reconcile_versions_without_foreign_is_identity():
    v5 = df(["a", "T", ""])
    state, d, edited = rs.reconcile_versions([], v5, [v5], K)
    assert state.equals(v5) and d.empty and edited == set()
