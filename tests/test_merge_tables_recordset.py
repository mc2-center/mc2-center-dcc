"""Offline tests for merge_tables.py RecordSet mode (Synapse fully mocked)."""

import logging
from unittest import mock

import pandas as pd

import merge_tables as mt


def _union(rs_by_grant, grants):
    """build_union with Synapse mocked; grants absent from rs_by_grant have no folder."""

    def find(**kw):
        return f"folder-{kw['parent']}" if kw["parent"] in rs_by_grant else None

    with mock.patch.object(mt.operations, "find_entity_id", side_effect=find), \
         mock.patch.object(mt.recordsets, "find_recordset", side_effect=lambda syn, f, c: "rs-" + f), \
         mock.patch.object(
             mt.recordsets, "read_recordset",
             side_effect=lambda syn, rs: rs_by_grant[rs.removeprefix("rs-folder-")].copy(),
         ):
        return mt.build_union(mock.MagicMock(), "ToolView", grants)


def _rs(ids, names, **extra):
    return pd.DataFrame({"ToolViewId": ids, "ToolName": names, **extra})


def test_union_rows_headers_and_list_cells(caplog):
    g1 = _rs(["t1", "t2"], ["A", "B"])
    g1["GrantViewKey"] = pd.Series([["CA1"], ["CA1", "CA2"]], dtype=object)
    g2 = _rs(["t3"], ["C"], GrantViewKey=[["CA3"]])
    with caplog.at_level(logging.WARNING):
        df, stats = _union({"syn1": g1, "syn2": g2}, ["syn1", "syn2", "syn3"])
    assert stats == {"grants_scanned": 3, "recordsets_found": 2, "missing": 1,
                     "rows": 3, "duplicate_keys": 0}
    assert list(df.columns) == mt.model_contract.template_headers("ToolView")
    assert df["GrantView Key"].tolist() == ["CA1", "CA1, CA2", "CA3"]
    assert df["Tool Name"].tolist() == ["A", "B", "C"]
    assert "syn3" in caplog.text


def test_missing_column_filled_with_empty_string():
    rs = {"syn1": _rs(["t1"], ["A"], ToolVersion=["1.0"]), "syn2": _rs(["t2"], ["B"])}
    df, _ = _union(rs, ["syn1", "syn2"])
    assert df["Tool Version"].tolist() == ["1.0", ""]


def test_duplicate_keys_kept_and_counted():
    rs = {"syn1": _rs(["t1", "t2"], ["A", "B"]), "syn2": _rs(["t1"], ["A"])}
    df, stats = _union(rs, ["syn1", "syn2"])
    assert len(df) == 3
    assert stats["duplicate_keys"] == 1


def test_no_recordsets_gives_header_only_union(tmp_path, capsys):
    with mock.patch.object(mt.operations, "find_entity_id", return_value=None):
        _, stats = mt.run_recordset_mode(mock.MagicMock(), "ToolView", str(tmp_path), ["syn1"])
    assert stats["rows"] == 0 and stats["missing"] == 1
    out = (tmp_path / "ToolView_UNION.csv").read_text().splitlines()
    assert out == [",".join(mt.model_contract.template_headers("ToolView"))]
    assert "rows=0" in capsys.readouterr().out
