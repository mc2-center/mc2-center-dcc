"""Offline tests for merge_tables.py (Synapse fully mocked)."""

import logging
from unittest import mock

import pandas as pd
import pytest

import merge_tables as mt


@pytest.mark.parametrize(
    "ids, expected",
    [
        ([], ""),
        (["syn1"], "SELECT * FROM syn1"),
        (["syn1", "syn2", "syn3"], "SELECT * FROM syn1 UNION SELECT * FROM syn2 UNION SELECT * FROM syn3"),
    ],
)
def test_legacy_query_unchanged(ids, expected):
    assert mt.build_query(ids) == expected


def _run(find_map, caplog=None):
    table_cls = mock.MagicMock()
    table_cls.return_value.query.return_value = pd.DataFrame({"grantId": ["syn10", "syn20"]})
    with mock.patch.object(mt, "Table", table_cls), \
         mock.patch.object(mt.operations, "find_entity_id", side_effect=lambda **kw: find_map(kw)), \
         mock.patch.object(mt, "RecordSet") as rs, \
         mock.patch.object(mt.pd, "read_csv", return_value=pd.DataFrame({"a": [1]})):
        rs.return_value.get.return_value.path = "x.csv"
        df = mt.get_record_sets(mock.MagicMock(), "T_RecordSet", "folder")
    return df, table_cls, rs


def test_grant_query_selects_column_not_literal():
    _, table_cls, _ = _run(lambda kw: None)
    q = table_cls.return_value.query.call_args.kwargs["query"]
    assert q == "SELECT grantId FROM syn21918972"
    assert "'grantId'" not in q


def test_missing_folder_or_recordset_skipped_and_logged(caplog):
    def find(kw):
        if kw["parent"] == "syn10":
            return None  # missing folder
        if kw["name"] == "folder":
            return "synF"
        return None  # missing RecordSet
    with caplog.at_level(logging.WARNING):
        df, _, rs = _run(find)
    rs.assert_not_called()
    assert len(df) == 0
    assert "syn10" in caplog.text and "syn20" in caplog.text


def test_skipped_grants_excluded_from_union_but_found_kept():
    def find(kw):
        if kw["parent"] == "syn10":
            return None
        return "synF" if kw["name"] == "folder" else "synR"
    df, table_cls, rs = _run(find)
    rs.assert_called_once_with(id="synR")
    assert list(df["grantId"]) == ["syn20"]
    assert "" not in df["recordId"].tolist()
