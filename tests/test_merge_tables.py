"""Offline tests for merge_tables.py (Synapse fully mocked)."""

import importlib.util
import logging
import subprocess
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

import merge_tables as mt

ROOT = Path(__file__).resolve().parent.parent


def _original_module():
    """merge_tables.py as of the branch base (before D12 fixes)."""
    src = subprocess.run(
        ["git", "show", "model-alignment:portal_tables/merge_tables.py"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    ns = {"__name__": "merge_tables_orig"}
    exec(compile(src, "merge_tables_orig", "exec"), ns)
    return ns


@pytest.mark.parametrize("ids", [[], ["syn1"], ["syn1", "syn2", "syn3"]])
def test_legacy_query_unchanged(ids):
    orig = _original_module()["build_query"]
    assert mt.build_query(ids) == orig(ids)
    assert mt.build_query(["syn1", "syn2"]) == "SELECT * FROM syn1 UNION SELECT * FROM syn2"


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
