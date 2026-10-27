"""portal_tables/recordsets.py, offline."""
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import recordsets as rs

K = "PublicationViewId"


def df(*rows, cols=(K, "Title", "Assay")):
    return pd.DataFrame(list(rows), columns=list(cols))





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


def test_parse_upload_splits_list_columns():
    up = pd.DataFrame({K: ["a"], "Assay": ["RNA Sequencing, Mass Spectrometry"], "Title": ["x, y"]})
    out = rs.parse_upload(up, {"Assay"})
    assert out.loc[0, "Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert out.loc[0, "Title"] == "x, y"


def test_parse_upload_keeps_commas_inside_json_array_items():
    up = pd.DataFrame({K: ["a", "b"], "Assay": ['["Mice, Inbred C57BL", "Humans"]', "A, B"]})
    out = rs.parse_upload(up, {"Assay"})
    assert out.loc[0, "Assay"] == ["Mice, Inbred C57BL", "Humans"]
    assert out.loc[1, "Assay"] == ["A", "B"]


def test_parse_upload_rejoins_known_terms_in_keyword_comma_text_only():
    up = pd.DataFrame({K: ["a"], "PublicationKeywords": ["Aged, 80 and over, Humans, Foo, bar"],
                       "Assay": ["Aged, 80 and over"]})
    out = rs.parse_upload(up, {"PublicationKeywords", "Assay"})
    assert out.loc[0, "PublicationKeywords"] == ["Aged, 80 and over", "Humans", "Foo", "bar"]
    assert out.loc[0, "Assay"] == ["Aged", "80 and over"]


def test_set_like_columns_from_schema():
    assert {"Assay", "Tissue", "TumorType"} <= rs.set_like_columns("PublicationView")
    assert "PublicationAuthors" not in rs.set_like_columns("PublicationView")
    assert "PublicationAuthors" in rs.list_columns("PublicationView")


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


def _grid(stats_sequence, versions):
    grid = mock.MagicMock()
    exports = iter(zip(stats_sequence, versions))

    def export(**kw):
        s, v = next(exports)
        grid.validation_summary_statistics = SimpleNamespace(number_of_unknown_children=s)
        grid.record_set_version_number = v

    grid.export_to_record_set.side_effect = export
    return grid




def rep(*versions, log=None, set_like=()):
    """replay() over frames v1..vN; ``log`` is {pipeline version: base}."""
    return rs.replay(list(enumerate(versions, start=1)), log or {}, K, set_like)


def dec(d):
    return {(r.version, r.key, r.column, r.decision) for r in d.itertuples()}


def cell(state, key, column):
    return state.set_index(K).loc[key, column]


# --- replay ---


def test_first_write_baseline_is_latest_all_pipeline_owned():
    state, owned, d = rep(df(["a", "Old", ""]), df(["a", "Newer", ""]))
    assert cell(state, "a", "Title") == "Newer" and owned == set() and d.empty


def test_pre_log_data_is_pipeline_owned_and_upload_applies_without_conflict():
    v1 = df(["a", "Migrated", ""])  # created by the migration, never logged
    v2 = df(["a", "Migrated", ["x"]])  # pipeline's first write, base 1
    state, owned, d = rep(v1, v2, log={2: 1})
    assert owned == set()
    merged, conflicts = rs.merge_upload(state, df(["a", "Upload", ["x"]]), K, owned)
    assert cell(merged, "a", "Title") == "Upload" and conflicts.empty


def test_contributor_edit_kept_and_reported_then_wins_over_upload():
    v1 = df(["a", "Pipeline", ""])
    v2 = df(["a", "Mine", ""])
    state, owned, d = rep(v1, v2, log={1: 0})
    assert dec(d) == {(2, "a", "Title", "kept_edit")}
    assert owned == {("a", "Title")}
    merged, conflicts = rs.merge_upload(state, df(["a", "Pipeline", ""]), K, owned)
    assert cell(merged, "a", "Title") == "Mine"
    assert conflicts.to_dict("records") == [
        {"key": "a", "column": "Title", "contributor_value": "Mine", "upload_value": "Pipeline"}]


def test_contributor_edit_still_wins_on_a_later_run_after_the_pipeline_carried_it():
    v1 = df(["a", "Pipeline", ""])
    v2 = df(["a", "Mine", ""])
    v3 = df(["a", "Mine", ""])  # pipeline wrote the merged frame, base 2
    state, owned, d = rep(v1, v2, v3, log={1: 0, 3: 2})
    assert owned == {("a", "Title")} and d.empty


def test_contributor_export_between_read_and_write_is_not_overwritten():
    v1 = df(["a", "Pipeline", ""])
    v2 = df(["a", "Mine", ""])  # landed after the pipeline read v1
    v3 = df(["a", "Pipeline 2", ""])  # pipeline's write, base 1: never saw v2
    state, owned, d = rep(v1, v2, v3, log={1: 0, 3: 1})
    assert cell(state, "a", "Title") == "Mine" and owned == {("a", "Title")}
    # nothing is reported for versions at or before the newest logged one
    assert d.empty
    merged, conflicts = rs.merge_upload(state, df(["a", "Pipeline 2", ""]), K, owned)
    assert cell(merged, "a", "Title") == "Mine" and len(conflicts) == 1


def test_unseen_edit_decision_reported_when_newer_than_log():
    v1 = df(["a", "Pipeline", ""])
    v2 = df(["a", "Mine", ""])
    v3 = df(["a", "Pipeline 2", ""])
    _, _, d = rep(v1, v2, v3, log={1: 0})  # v3 not (yet) logged: a foreign version
    assert dec(d) == {(2, "a", "Title", "kept_edit"), (3, "a", "Title", "kept_edit")}
    _, _, d = rep(v1, v2, v3, log={3: 1})
    assert dec(d) == set()


def test_v5_v6_v7_stale_session_keeps_a_edit_and_b_edit():
    v5 = df(["a", "Pipeline", ""], ["b", "Pipeline", ""])
    v6 = df(["a", "A edit", ""], ["b", "Pipeline", ""])
    v7 = df(["a", "Pipeline", ""], ["b", "B edit", ""], ["c", "B row", ""])
    state, owned, d = rep(v5, v6, v7, log={1: 0})
    assert cell(state, "a", "Title") == "A edit"
    assert cell(state, "b", "Title") == "B edit" and cell(state, "c", "Title") == "B row"
    assert dec(d) == {
        (2, "a", "Title", "kept_edit"), (3, "a", "Title", "restored_revert"),
        (3, "b", "Title", "kept_edit"), (3, "c", "", "kept_contributor_row"),
    }
    assert owned == {("a", "Title"), ("b", "Title"), ("c", "Title")}


def test_exact_copy_of_an_older_version_is_a_contributor_undo():
    # A pipeline (v1), B edit (v2), then v3 is an exact copy of v1. Stale export
    # or deliberate undo can't be told apart; owner decision: a contributor undo.
    a = df(["k", "A", ""])
    b = df(["k", "B", ""])
    state, owned, d = rep(a, b, a, a, a, log={1: 0, 5: 4})
    assert cell(state, "k", "Title") == "A" and owned == {("k", "Title")}
    merged, conflicts = rs.merge_upload(state, df(["k", "C", ""]), K, owned)
    assert cell(merged, "k", "Title") == "A" and len(conflicts) == 1


def test_stale_export_with_an_edit_restores_untouched_cells():
    # Not an exact copy (the session also edited another row), so the snapshot
    # is the older version and its untouched cells are stale.
    v1 = df(["k", "A", ""], ["m", "M", ""])
    v2 = df(["k", "B", ""], ["m", "M", ""])
    v3 = df(["k", "A", ""], ["m", "M2", ""])
    state, owned, d = rep(v1, v2, v3, log={1: 0, 2: 1})
    assert cell(state, "k", "Title") == "B" and cell(state, "m", "Title") == "M2"
    assert (3, "k", "Title", "restored_revert") in dec(d)


def test_contributor_row_blank_cells_stay_pipeline_owned():
    v1 = df(["a", "T", ""])
    v2 = df(["a", "T", ""], ["r", "Mine", ""])
    state, owned, d = rep(v1, v2, log={1: 0})
    assert dec(d) == {(2, "r", "", "kept_contributor_row")}
    assert owned == {("r", "Title")}
    merged, conflicts = rs.merge_upload(state, df(["r", "Upload", ["x"]]), K, owned)
    assert cell(merged, "r", "Assay") == ["x"]  # filled: blank cells are the pipeline's
    assert cell(merged, "r", "Title") == "Mine" and len(conflicts) == 1


def test_deleted_contributor_row_is_restored():
    v1 = df(["a", "T", ""])
    v2 = df(["a", "T", ""], ["r", "Mine", ""])
    v3 = df(["a", "T", ""])
    state, owned, d = rep(v1, v2, v3, log={1: 0})
    assert set(state[K]) == {"a", "r"} and cell(state, "r", "Title") == "Mine"
    assert (3, "r", "", "restored_missing") in dec(d)


def test_pipeline_version_keeps_state_rows_it_lacks():
    v1 = df(["a", "T", ""], ["b", "T", ""])
    v2 = df(["a", "T", ""])  # a pipeline version missing b
    state, _, d = rep(v1, v2, log={1: 0, 2: 1})
    assert set(state[K]) == {"a", "b"}


def test_clearing_a_cell_is_an_edit():
    v1 = df(["a", "T", ""])
    v2 = df(["a", "", ""])
    state, owned, d = rep(v1, v2, log={1: 0})
    assert dec(d) == {(2, "a", "Title", "kept_edit")}
    # going back to blank by an exact copy of an older version is a contributor undo
    w1 = df(["a", "", ""])
    w2 = df(["a", "T", ""])
    w3 = df(["a", "", ""])
    state, _, d = rep(w1, w2, w3, log={2: 1})
    assert cell(state, "a", "Title") == ""
    assert (3, "a", "Title", "kept_edit") in dec(d)


def test_set_like_list_order_is_not_an_edit():
    v1 = df(["a", "T", ["x", "y"]])
    v2 = df(["a", "T", ["y", "x"]])
    assert rep(v1, v2, log={1: 0}, set_like={"Assay"})[2].empty
    assert dec(rep(v1, v2, log={1: 0})[2]) == {(2, "a", "Assay", "kept_edit")}


def test_composite_key_uses_unit_separator():
    cols = ("A", "B", "V")
    v1 = pd.DataFrame([["1", "x", "old"]], columns=cols)
    v2 = pd.DataFrame([["1", "x", "new"]], columns=cols)
    _, _, d = rs.replay([(1, v1), (2, v2)], {1: 0}, ["A", "B"])
    assert d.loc[0, "key"] == "1\x1fx"


# --- cells and keys ---


def test_numeric_strings_compare_equal_but_not_leading_zeros():
    assert rs._cells_equal("2020", "2020.0", False)
    assert rs._cells_equal("-1.50", "-1.5", False)
    assert not rs._cells_equal("007", "7", False)
    assert not rs._cells_equal("2020", "2021", False)
    assert not rs._cells_equal("abc", "ABC", False)


def test_check_upload_keys_rejects_blank_and_duplicate_and_strips():
    ok = rs.check_upload_keys(pd.DataFrame({K: [" a ", "b"], "T": ["1", "2"]}), K)
    assert list(ok[K]) == ["a", "b"]
    with pytest.raises(ValueError, match="duplicate keys.*a"):
        rs.check_upload_keys(pd.DataFrame({K: ["a", " a"], "T": ["1", "2"]}), K)
    with pytest.raises(ValueError, match="blank key"):
        rs.check_upload_keys(pd.DataFrame({K: ["a", " "], "T": ["1", "2"]}), K)


def test_read_recordset_parses_json_only_in_list_columns(tmp_path):
    path = tmp_path / "rs.csv"
    path.write_text(f'{K},Title,Assay\na,"[1]","[""RNA Sequencing"",""Whole Genome Sequencing""]"\nb,,\n')
    with mock.patch.object(rs, "RecordSet") as cls:
        cls.return_value.get.return_value = SimpleNamespace(path=str(path))
        out = rs.read_recordset("syn", "syn1", version=3, list_cols={"Assay"})
        plain = rs.read_recordset("syn", "syn1", version=3)
    assert cls.call_args.kwargs["version_number"] == 3
    assert out.loc[0, "Title"] == "[1]"
    assert out.loc[0, "Assay"] == ["RNA Sequencing", "Whole Genome Sequencing"]
    assert out.loc[1, "Title"] == "" and out.loc[1, "Assay"] == ""
    assert plain.loc[0, "Assay"].startswith("[")


def test_read_recordset_cache_miss_then_hit(tmp_path):
    cache = tmp_path / "cache"
    dl = tmp_path / "dl.csv"
    dl.write_text(f"{K},Title\na,x\n")
    with mock.patch.object(rs, "RecordSet") as cls:
        cls.return_value.get.return_value = SimpleNamespace(path=str(dl))
        first = rs.read_recordset("syn", "syn1", version=4, cache_dir=cache)
        assert (cache / "syn1" / "v4.csv").exists() and cls.return_value.get.call_count == 1
        second = rs.read_recordset("syn", "syn1", version=4, cache_dir=cache)
        assert cls.return_value.get.call_count == 1
        assert first.equals(second)
        rs.read_recordset("syn", "syn1", version=None, cache_dir=cache)
        rs.read_recordset("syn", "syn1", version=None, cache_dir=cache)
        assert cls.return_value.get.call_count == 3


# --- write log v2 ---


def test_csv_write_log_v2(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "sub" / "log.csv")
    assert log.pipeline_map("syn1") == {} and log.last_version("syn1") is None
    log.start("syn1", 4, "run1", "42")
    assert log.pipeline_map("syn1") == {}  # pending has no version
    log.record("syn1", 5, "run1", 10, base=4, principal="42")
    log.record("syn1", 7, "run2", 12, base=6, principal="42", status="recovered")
    log.record("syn2", 9, "run2", 1)
    assert log.pipeline_map("syn1") == {5: 4, 7: 6}
    assert log.pipeline_versions("syn1") == [5, 7] and log.last_version("syn1") == 7
    header = (tmp_path / "sub" / "log.csv").read_text().splitlines()[0]
    assert header == ",".join(rs.WRITE_LOG_COLUMNS)
    # a written row doesn't close the write (more exports may follow); done does
    assert [r["runId"] for r in log.unresolved_pending("syn1")] == ["run1"]
    log.finish("syn1", 4, "run1", "42")
    assert log.unresolved_pending("syn1") == []


def test_lock_from_unresolved_pending_row_younger_than_two_hours(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 3, "other", "7")
    assert log.locked_by("syn1", "me") == "other"
    assert log.locked_by("syn1", "other") is None  # a run is not locked by itself
    assert log.locked_by("syn2", "me") is None
    later = pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=2, minutes=1)
    assert log.locked_by("syn1", "me", now=later) is None
    log.record("syn1", 4, "other", 5, base=3, principal="7")  # an export: still writing
    assert log.locked_by("syn1", "me") == "other"
    log.finish("syn1", 3, "other", "7")  # resolved
    assert log.locked_by("syn1", "me") is None


def test_crash_recovery_by_principal_and_time_from_the_pending_row(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.record("syn1", 1, "r0", 3, base=0, principal="42")
    log.start("syn1", 1, "dead", "42")
    pending_on = log.unresolved_pending("syn1")[0]["writtenOn"]
    meta = {
        2: ("42", pending_on - pd.Timedelta(minutes=15)),  # beyond the clock tolerance: not ours
        3: ("99", pending_on + pd.Timedelta(minutes=1)),  # a contributor
        4: ("42", pending_on + pd.Timedelta(minutes=2)),  # the dead run's export, never logged
        5: ("42", pending_on + pd.Timedelta(minutes=3)),  # its second export
    }
    found = rs.recover_pending("syn", "syn1", log, 5, meta=lambda v: meta[v])
    assert found == {4: 1, 5: 1}
    assert log.pipeline_map("syn1") == {1: 0, 4: 1, 5: 1}
    assert log.unresolved_pending("syn1") == []
    df_ = log._read("syn1")
    assert set(df_.loc[df_["version"].isin([4, 5]), "status"]) == {"recovered"}
    # replay then treats v4/v5 as pipeline versions with base 1 and v2/v3 as foreign
    assert rs.recover_pending("syn", "syn1", log, 5, meta=lambda v: meta[v]) == {}


def test_recovery_dry_run_records_nothing_and_empty_recovery_aborts(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "dead", "42")
    on = log.unresolved_pending("syn1")[0]["writtenOn"]
    meta = lambda v: ("42", on + pd.Timedelta(minutes=1))
    assert rs.recover_pending("syn", "syn1", log, 2, meta=meta, record=False) == {2: 1}
    assert log.pipeline_map("syn1") == {} and len(log.unresolved_pending("syn1")) == 1
    assert rs.recover_pending("syn", "syn1", log, 1, meta=meta) == {}
    assert log.unresolved_pending("syn1") == []
    assert log._read("syn1")["status"].tolist() == ["pending", "aborted"]


def test_recovery_accepts_clock_skew_within_the_tolerance(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "dead", "42")
    on = log.unresolved_pending("syn1")[0]["writtenOn"]
    skewed = lambda v: ("42", on - pd.Timedelta(minutes=9))  # server clock 9 min behind
    assert rs.recover_pending("syn", "syn1", log, 2, meta=skewed) == {2: 1}
    log2 = rs.CsvWriteLog(tmp_path / "log2.csv")
    log2.start("syn1", 1, "dead", "42")
    on = log2.unresolved_pending("syn1")[0]["writtenOn"]
    assert rs.recover_pending("syn", "syn1", log2, 2, meta=lambda v: ("42", on - pd.Timedelta(minutes=11))) == {}


def test_recovery_covers_a_pending_row_of_the_same_run_id(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "same", "42")
    on = log.unresolved_pending("syn1")[0]["writtenOn"]
    found = rs.recover_pending("syn", "syn1", log, 2, meta=lambda v: ("42", on + pd.Timedelta(seconds=5)))
    assert found == {2: 1} and log.pipeline_map("syn1") == {2: 1}


def test_lock_counts_a_same_run_id_row_older_than_this_process(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "same", "42")
    started_before = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=1)
    started_after = pd.Timestamp.now(tz="UTC") + pd.Timedelta(minutes=1)
    assert log.locked_by("syn1", "same", since=started_after) == "same"  # row predates this process
    assert log.locked_by("syn1", "same", since=started_before) is None  # our own row
    assert log.locked_by("syn1", "same") is None


def test_earlier_pending_row_wins_the_lock_race(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "first", "7")
    log.start("syn1", 1, "second", "8")
    assert log.earlier_pending("syn1", "second") == "first"
    assert log.earlier_pending("syn1", "first") is None
    assert log.earlier_pending("syn1", "nobody") is None
    # an aborted (resolved) earlier row no longer beats us
    log.abort("syn1", 1, "first", "7")
    assert log.earlier_pending("syn1", "second") is None


def test_no_recovered_warning_for_an_aborted_row(tmp_path, caplog):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "dead", "42")
    with caplog.at_level("WARNING"):
        rs.recover_pending("syn", "syn1", log, 1, meta=lambda v: 1 / 0)
    assert "recovered versions" not in caplog.text


def test_cache_temp_name_is_unique_per_process(tmp_path):
    dl = tmp_path / "dl.csv"
    dl.write_text(f"{K},Title\na,x\n")
    seen = []
    real = rs.shutil.copyfile
    with mock.patch.object(rs, "RecordSet") as cls, \
         mock.patch.object(rs.shutil, "copyfile", side_effect=lambda a, b: (seen.append(str(b)), real(a, b))[1]):
        cls.return_value.get.return_value = SimpleNamespace(path=str(dl))
        rs.read_recordset("syn", "syn1", version=1, cache_dir=tmp_path / "c")
        rs.read_recordset("syn", "syn1", version=2, cache_dir=tmp_path / "c")
    assert all(".part" in n and str(rs.os.getpid()) in n for n in seen)
    assert seen[0].split("v1.csv")[1] != seen[1].split("v2.csv")[1]


def test_synapse_write_log_queries_and_stores():
    with mock.patch.object(rs, "Table") as table:
        table.return_value.query.return_value = pd.DataFrame({
            "recordSetId": ["syn1", "syn1"], "version": [3, 1], "baseVersion": [2, 0],
            "runId": ["a", "b"], "principalId": ["1", "1"], "status": ["written", "recovered"],
            "writtenOn": [0, 0], "rowsWritten": [1, 1]})
        log = rs.SynapseTableWriteLog("syn", "syn99")
        assert log.pipeline_map("syn1") == {3: 2, 1: 0}
        assert "recordSetId = 'syn1'" in table.return_value.query.call_args.kwargs["query"]
        log.start("syn1", 4, "r", "42")
        row = table.return_value.store_rows.call_args.kwargs["values"].iloc[0]
        assert (row.recordSetId, row.baseVersion, row.status, row.principalId) == ("syn1", 4, "pending", "42")
        assert pd.isna(row.version)
        log.record("syn1", 5, "r", 7, base=4, principal="42")
        row = table.return_value.store_rows.call_args.kwargs["values"].iloc[0]
        assert (row.version, row.baseVersion, row.rowsWritten, row.status) == (5, 4, 7, "written")


def test_create_write_log_table_has_the_v2_columns():
    with mock.patch.object(rs, "Table") as table:
        table.return_value.store.return_value.id = "syn5"
        assert rs.create_write_log_table("syn", "synParent") == "syn5"
    names = [c.name for c in table.call_args.kwargs["columns"]]
    assert names == rs.WRITE_LOG_COLUMNS


# --- write_via_grid callbacks ---


def test_write_via_grid_calls_on_version_after_each_export():
    grid = _grid([3, 0], [7, 8])
    seen = []
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep"):
        cls.return_value.create.return_value = grid
        assert rs.write_via_grid("syn", "syn1", "x.csv", on_version=seen.append) == [7, 8]
    assert seen == [7, 8]


def test_grid_delete_failure_is_logged_and_does_not_mask_the_result(caplog):
    grid = _grid([0], [7])
    grid.delete.side_effect = RuntimeError("not the creator")
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep"):
        cls.return_value.create.return_value = grid
        assert rs.write_via_grid("syn", "syn1", "x.csv") == [7]
    assert "could not delete grid session" in caplog.text


def test_grid_delete_failure_does_not_mask_the_original_exception():
    grid = mock.MagicMock()
    grid.import_csv.side_effect = ValueError("original")
    grid.delete.side_effect = RuntimeError("delete failed")
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep"):
        cls.return_value.create.return_value = grid
        with pytest.raises(ValueError, match="original"):
            rs.write_via_grid("syn", "syn1", "x.csv")


def test_fresh_session_undoing_a_pipeline_change_is_an_edit():
    # v1 baseline "Orig"; pipeline v2 writes "Pipe"; a contributor (fresh session
    # on v2) sets it back to "Orig". That is a deliberate edit, not a stale revert.
    v1 = df(["p1", "Orig", ""])
    v2 = df(["p1", "Pipe", ""])
    v3 = df(["p1", "Orig", ""])
    state, owned, d = rep(v1, v2, v3, log={2: 1})
    assert state.set_index(K).loc["p1", "Title"] == "Orig"
    assert ("p1", "Title") in owned
    assert (3, "p1", "Title", "kept_edit") in dec(d)


def test_stale_session_untouched_cell_is_restored_but_its_edit_kept():
    # pipeline v1 (A, X); pipeline v2 (B, X); stale session opened on v1 exports
    # v3 = v1 + an edit to p2: p1 "A" is stale, p2 "Y" is the user's edit.
    v1 = df(["p1", "A", ""], ["p2", "X", ""])
    v2 = df(["p1", "B", ""], ["p2", "X", ""])
    v3 = df(["p1", "A", ""], ["p2", "Y", ""])
    state, owned, d = rep(v1, v2, v3, log={1: 0, 2: 1})
    s = state.set_index(K)
    assert s.loc["p1", "Title"] == "B" and s.loc["p2", "Title"] == "Y"
    assert (3, "p1", "Title", "restored_revert") in dec(d)
    assert (3, "p2", "Title", "kept_edit") in dec(d)
    assert ("p2", "Title") in owned and ("p1", "Title") not in owned


def test_stale_session_edit_to_an_old_value_is_kept():
    # The stale session's snapshot (v1) had "A"; the user typed "Z", a value the
    # cell held long ago (v0 baseline). Differs from the snapshot, so it's an edit.
    v0 = df(["p1", "Z", ""])
    v1 = df(["p1", "A", ""])
    v2 = df(["p1", "B", ""])
    v3 = df(["p1", "Z", ""])
    # rep() numbers them 1..4 (v0=1 baseline, v1=2 and v2=3 pipeline, v3=4)
    state, owned, d = rep(v0, v1, v2, v3, log={2: 1, 3: 2})
    assert state.set_index(K).loc["p1", "Title"] == "Z"
    assert (4, "p1", "Title", "kept_edit") in dec(d)


# --- complete-reversion snapshot rule ---

W = ("T1", "T2", "T3", "T4", "T5")


def wide(*rows):
    return pd.DataFrame(list(rows), columns=[K, *W])


def test_partial_reversion_is_a_contributor_undo_and_the_fix_is_kept():
    v1 = wide(["a", "x1", "x2", "x3", "u", "u"])
    v2 = wide(["a", "p1", "p2", "p3", "u", "u"])  # pipeline changed three cells
    # a fresh session on v2 reverts two of them and fixes an unrelated cell
    v3 = wide(["a", "x1", "x2", "p3", "fixed", "u"])
    state, owned, d = rep(v1, v2, v3, log={1: 0, 2: 1})
    s = state.set_index(K).loc["a"]
    assert (s["T1"], s["T2"], s["T3"], s["T4"]) == ("x1", "x2", "p3", "fixed")
    assert {x.decision for x in d.itertuples()} == {"kept_edit"}
    assert owned == {("a", "T1"), ("a", "T2"), ("a", "T4")}


def test_true_stale_session_restores_untouched_cells_and_keeps_its_edit():
    v1 = wide(["a", "x1", "x2", "x3", "u", "u"])
    v2 = wide(["a", "p1", "p2", "p3", "u", "u"])  # pipeline write the session never saw
    v3 = wide(["a", "x1", "x2", "x3", "fixed", "u"])  # opened on v1: all reverted, one edit
    state, owned, d = rep(v1, v2, v3, log={1: 0, 2: 1})
    s = state.set_index(K).loc["a"]
    assert (s["T1"], s["T2"], s["T3"], s["T4"]) == ("p1", "p2", "p3", "fixed")
    assert dec(d) == {(3, "a", c, "restored_revert") for c in ("T1", "T2", "T3")} | {(3, "a", "T4", "kept_edit")}
    assert owned == {("a", "T4")}


def test_exact_copy_of_an_older_version_is_a_contributor_undo():
    v1 = wide(["a", "x1", "x2", "x3", "u", "u"])
    v2 = wide(["a", "p1", "p2", "p3", "u", "u"])
    state, owned, d = rep(v1, v2, v1, log={1: 0, 2: 1})
    assert state.set_index(K).loc["a", "T1"] == "x1"
    assert owned == {("a", c) for c in ("T1", "T2", "T3")}


def test_rows_added_after_the_stale_base_count_as_reverted_when_missing():
    v1 = wide(["a", "x1", "x2", "x3", "u", "u"])
    v2 = wide(["a", "p1", "x2", "x3", "u", "u"], ["n", "n1", "n2", "n3", "n4", "n5"])  # pipeline adds row n
    v3 = wide(["a", "x1", "x2", "x3", "fixed", "u"])  # stale: no n, a reverted
    state, owned, d = rep(v1, v2, v3, log={1: 0, 2: 1})
    s = state.set_index(K)
    assert s.loc["a", "T1"] == "p1" and s.loc["a", "T4"] == "fixed" and "n" in s.index
    assert (3, "n", "", "restored_missing") in dec(d)


def _reference_base(frames, v, non_key):
    """Brute force of the rule: newest u <= v-2 with a complete reversion, not exact, else v-1."""
    from_v = frames[v]

    def val(frame, k, c):
        return frame[k][c] if k in frame else rs._ABSENT

    if any(frames[u] == from_v for u in range(1, v - 1)):
        return v - 1
    for u in range(v - 2, 0, -1):
        cells = {(k, c) for k in set(frames[u]) | set(frames[v - 1]) for c in non_key
                 if val(frames[u], k, c) != val(frames[v - 1], k, c)}
        if cells and all(val(frames[u], k, c) == val(from_v, k, c) for k, c in cells):
            return u
    return v - 1


def test_incremental_session_base_matches_the_brute_force_rule():
    import random

    rng = random.Random(7)
    for trial in range(60):
        n_versions = rng.randint(3, 9)
        keys = [f"k{i}" for i in range(rng.randint(1, 5))]
        frames = {1: {k: {c: rng.choice("abc") for c in W} for k in keys[: rng.randint(1, len(keys))]}}
        for v in range(2, n_versions + 1):
            f = {k: dict(r) for k, r in frames[v - 1].items()}
            for _ in range(rng.randint(0, 4)):
                op = rng.random()
                k = rng.choice(keys)
                if op < 0.7 and k in f:
                    f[k][rng.choice(W)] = rng.choice("abcd")
                elif op < 0.85 and k not in f:
                    f[k] = {c: rng.choice("abc") for c in W}
                elif k in f and len(f) > 1:
                    del f[k]
            frames[v] = f
        cf = {v: {k: tuple(r[c] for c in W) for k, r in f.items()} for v, f in frames.items()}
        diffs = rs._version_diffs(cf, n_versions)
        fps = {v: hash(frozenset(cf[v].items())) for v in cf}
        for v in range(2, n_versions + 1):
            assert rs._session_base(cf, diffs, fps, v) == _reference_base(frames, v, list(W)), (trial, v)


def test_numbers_compare_exactly():
    a, b = "12345678901234567890", "12345678901234567891"
    assert not rs._cells_equal(a, b, False)
    assert rs._cells_equal(a, a, False)
    assert rs._cells_equal("2020", "2020.0", False)
    assert rs._cells_equal("0.10", "0.1", False)
    assert not rs._cells_equal("0.1", "0.10000000000000000001", False)
    assert not rs._cells_equal("1e3", "1000", False)  # not a plain number


@pytest.mark.slow
def test_replay_benchmark_200_versions_500_rows_18_columns():
    import random
    import time as _time

    rng = random.Random(1)
    cols = [f"C{i}" for i in range(17)]
    keys = [f"k{i}" for i in range(500)]
    rows = {k: {c: str(rng.randint(0, 9)) for c in cols} for k in keys}
    versions, log = [], {}
    for v in range(1, 201):
        for _ in range(30):
            rows[rng.choice(keys)][rng.choice(cols)] = str(rng.randint(0, 99))
        frame = pd.DataFrame([{K: k, **r} for k, r in rows.items()])
        versions.append((v, frame.copy()))
        if v % 3 == 0:
            log[v] = v - 1
    start = _time.perf_counter()
    state, owned, d = rs.replay(versions, log, K)
    elapsed = _time.perf_counter() - start
    print(f"replay of 200 versions x 500 rows x 18 columns: {elapsed:.2f}s")
    assert len(state) == 500 and elapsed < 5


def test_rerun_and_reused_run_id_keep_their_lock(tmp_path):
    # Same runId writes v6 (base 4) and finishes, then a rerun starts at base 6:
    # its pending row must not be resolved by the first write's rows.
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 4, "RUN", "42")
    log.record("syn1", 6, "RUN", 3, base=4, principal="42")
    log.finish("syn1", 4, "RUN", "42")
    log.start("syn1", 6, "RUN", "42")
    assert [r["baseVersion"] for r in log.unresolved_pending("syn1")] == [6]
    assert log.locked_by("syn1", "OTHER") == "RUN"
    log.start("syn1", 6, "OTHER", "42")
    assert log.earlier_pending("syn1", "OTHER") == "RUN"


def test_second_export_crash_is_recovered(tmp_path):
    # A write exported v2 (logged) and v3, then died before logging v3 or done.
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 1, "R", "42")
    log.record("syn1", 2, "R", 3, base=1, principal="42")
    on = log.unresolved_pending("syn1")[0]["writtenOn"] + pd.Timedelta(minutes=1)
    found = rs.recover_pending(None, "syn1", log, 3, meta=lambda v: ("42", on), run_id="R")
    assert found == {3: 1}
    assert log.unresolved_pending("syn1") == []


def test_pre_start_recovery_leaves_a_young_live_row_alone(tmp_path):
    log = rs.CsvWriteLog(tmp_path / "log.csv")
    log.start("syn1", 5, "B", "42")  # run B just started; nothing exported yet
    found = rs.recover_pending(None, "syn1", log, 5, meta=lambda v: ("42", pd.NaT),
                               skip_younger_than=rs.LOCK_HOURS)
    assert found == {} and [r["runId"] for r in log.unresolved_pending("syn1")] == ["B"]


def test_leading_space_number_compares_as_number():
    assert rs._cells_equal(" 2020", "2020.0", False)
