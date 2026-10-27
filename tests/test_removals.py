"""D-Q49 row removal: replay, the Removals table, store/export helpers (Synapse stubbed)."""
from unittest import mock

import pandas as pd

import recordsets as rs

K = "PublicationViewId"


def df(*rows, cols=(K, "Title", "Assay")):
    return pd.DataFrame(list(rows), columns=list(cols))


def rep(*versions, log, removals):
    return rs.replay(list(enumerate(versions, start=1)), log, K, (), removals)


def dec(d):
    return {(r.version, r.key, r.column, r.decision) for r in d.itertuples()}


def cell(state, key, column):
    return state.set_index(K).loc[key, column]


# --- replay ---


def test_removal_version_drops_the_key_and_a_later_foreign_version_lacking_it_does_not_restore_it():
    v1 = df(["a", "T", ""], ["b", "T", ""])
    v2 = df(["a", "T", ""])  # removal (direct store)
    v3 = df(["a", "T", ""])  # the export after it
    v4 = df(["a", "Edited", ""])  # foreign
    log = {1: 0, 2: 1, 3: 2}
    state, _, d = rep(v1, v2, v3, v4, log=log, removals={2: {"b"}})
    assert set(state[K]) == {"a"} and cell(state, "a", "Title") == "Edited"
    assert not {x for x in dec(d) if x[1] == "b"}
    # without the removal record the row is kept, as before
    state, _, _ = rs.replay(list(enumerate([v1, v2, v3, v4], start=1)), log, K)
    assert set(state[K]) == {"a", "b"}


def test_removal_is_reported_when_newer_than_the_newest_logged_version():
    v1 = df(["a", "T", ""], ["b", "T", ""])
    v2 = df(["a", "T", ""])
    state, _, d = rep(v1, v2, log={1: 0}, removals={2: {"b"}})  # v2 not logged: ignored
    assert set(state[K]) == {"a", "b"}
    state, _, d = rep(v1, v2, v2, log={1: 0, 2: 1}, removals={2: {"b"}})
    assert set(state[K]) == {"a"}


def test_foreign_version_readding_a_removed_key_is_kept_and_reported():
    v1 = df(["a", "T", ""], ["b", "T", ""])
    v2 = df(["a", "T", ""])
    v3 = df(["a", "T", ""])
    v4 = df(["a", "T", ""], ["b", "T", "X"])  # a stale Grid session writes b back
    state, owned, d = rep(v1, v2, v3, v4, log={1: 0, 2: 1, 3: 2}, removals={2: {"b"}})
    assert set(state[K]) == {"a", "b"} and cell(state, "b", "Assay") == "X"
    assert (4, "b", "", "readded_removed_key") in dec(d)
    assert (4, "b", "", "kept_contributor_row") not in dec(d)
    assert ("b", "Assay") in owned
    # reported once: a later version that still has it is an ordinary row
    v5 = df(["a", "T", ""], ["b", "T", "X"])
    _, _, d = rep(v1, v2, v3, v4, v5, log={1: 0, 2: 1, 3: 2}, removals={2: {"b"}})
    assert [x for x in dec(d) if x[1] == "b"] == [(4, "b", "", "readded_removed_key")]


def test_pipeline_version_readding_a_removed_key_is_reported():
    v1 = df(["a", "T", ""], ["b", "T", ""])
    v2 = df(["a", "T", ""])
    v3 = df(["a", "T", ""], ["b", "New", ""])
    state, _, d = rep(v1, v2, v3, v3, log={1: 0, 2: 1, 3: 2}, removals={2: {"b"}})
    assert set(state[K]) == {"a", "b"} and cell(state, "b", "Title") == "New"
    # reported only past the newest logged version, so log up to v2
    _, _, d = rep(v1, v2, v3, log={1: 0, 2: 1}, removals={2: {"b"}})
    assert (3, "b", "", "readded_removed_key") in dec(d)  # v3 is foreign there


# --- Removals ---


def test_csv_removals_latest_row_per_key_is_its_status(tmp_path):
    r = rs.CsvRemovals(tmp_path / "sub" / "removals.csv")
    assert r.requested("syn1") == {} and r.removed("syn1") == {}
    r.append("syn1", "a", "requested", reason="dup", requested_by="u1")
    r.append("syn1", "b", "requested", reason="typo", requested_by="u1")
    r.append("syn2", "a", "requested")
    assert set(r.requested("syn1")) == {"a", "b"}
    assert r.requested("syn1")["a"] == {"reason": "dup", "requestedBy": "u1"}
    r.append("syn1", "a", "removed", removed_in_version=5, run_id="r1", principal="42")
    r.append("syn1", "b", "not_found", run_id="r1")
    assert r.requested("syn1") == {}
    assert r.removed("syn1") == {"a": 5}
    assert r.removal_events("syn1") == {5: {"a"}}
    r.append("syn1", "a", "restored")  # restored: no longer removed, the event stays for replay
    assert r.removed("syn1") == {} and r.removal_events("syn1") == {5: {"a"}}
    r.append("syn1", "a", "requested")  # requested again
    assert set(r.requested("syn1")) == {"a"}
    header = (tmp_path / "sub" / "removals.csv").read_text().splitlines()[0]
    assert header == ",".join(rs.REMOVALS_COLUMNS)


def test_synapse_removals_queries_and_stores():
    with mock.patch.object(rs, "Table") as table:
        table.return_value.query.return_value = pd.DataFrame({
            "recordSetId": ["syn1", "syn1"], "key": ["a", "a"], "reason": ["", ""],
            "requestedBy": ["u", ""], "status": ["requested", "removed"], "removedInVersion": [pd.NA, 5],
            "runId": ["", "r"], "principalId": ["", "42"], "changedOn": [1000, 2000]})
        r = rs.SynapseTableRemovals("syn", "syn99")
        assert r.removed("syn1") == {"a": 5} and r.requested("syn1") == {}
        assert "recordSetId = 'syn1'" in table.return_value.query.call_args.kwargs["query"]
        r.append("syn1", "k", "requested", reason="why", requested_by="u")
        row = table.return_value.store_rows.call_args.kwargs["values"].iloc[0]
        assert (row.recordSetId, row.key, row.status, row.reason) == ("syn1", "k", "requested", "why")
        assert pd.isna(row.removedInVersion)


def test_create_removals_table_has_the_columns():
    with mock.patch.object(rs, "Table") as table:
        table.return_value.store.return_value.id = "syn5"
        assert rs.create_removals_table("syn", "synParent") == "syn5"
    assert table.call_args.kwargs["name"] == "MC2Center_RecordSet_Removals"
    assert [c.name for c in table.call_args.kwargs["columns"]] == rs.REMOVALS_COLUMNS


def test_drop_removed_keys():
    upload = df(["a", "T", ""], ["b", "T", ""], ["c", "T", ""])
    kept, skipped = rs.drop_removed(upload, K, {"b": 5, "z": 2})
    assert list(kept[K]) == ["a", "c"] and list(skipped[K]) == ["b"]
    assert list(skipped["removedInVersion"]) == [5]
    kept, skipped = rs.drop_removed(upload, K, {})
    assert len(kept) == 3 and skipped.empty


# --- store / export ---


def test_store_new_version_sets_the_path_and_stores():
    with mock.patch.object(rs, "RecordSet") as cls:
        cls.return_value.get.return_value.store.return_value.version_number = 8
        assert rs.store_new_version("syn", "syn1", "x.csv") == 8
        entity = cls.return_value.get.return_value
        assert entity.path == "x.csv"
    assert cls.call_args.kwargs == {"id": "syn1", "download_file": False}


def test_export_via_grid_has_no_import():
    from types import SimpleNamespace
    with mock.patch.object(rs, "Grid") as cls, mock.patch.object(rs.time, "sleep") as sleep:
        grid = cls.return_value.create.return_value
        grid.record_set_version_number = 9
        grid.validation_summary_statistics = SimpleNamespace(number_of_unknown_children=0)
        seen = []
        assert rs.export_via_grid("syn", "syn1", validation_wait=45, on_version=seen.append) == [9]
        grid.import_csv.assert_not_called()
        grid.export_to_record_set.assert_called_once()
        grid.delete.assert_called_once()
    assert seen == [9]
    sleep.assert_called_once_with(45)
