"""Offline tests for merge_tables.py RecordSet mode (Synapse fully mocked)."""

import json
import logging
from types import SimpleNamespace
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
             side_effect=lambda syn, rs, **kw: rs_by_grant[rs.removeprefix("rs-folder-")].copy(),
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


def test_component_column_is_filled():
    # RecordSets have no Component column (a Grid import drops it).
    df, _ = _union({"syn1": _rs(["t1"], ["A"])}, ["syn1"])
    assert df["Component"].tolist() == ["ToolView"]


def _publish(existing_columns=None, store_rows=None):
    """Run publish_union against a mocked Synapse table that already exists."""
    schema = mt.union_schema("ToolView")
    existing = mock.MagicMock(id="syn9")
    if store_rows is not None:
        existing.store_rows.side_effect = store_rows
    existing.columns = {c.name: SimpleNamespace(name=c.name, column_type=c.column_type, maximum_size=c.maximum_size)
                        for c in (existing_columns or schema)}
    union = pd.DataFrame(columns=mt.model_contract.template_headers("ToolView"))
    with mock.patch.object(mt.operations, "find_entity_id", return_value="syn9"), \
         mock.patch.object(mt, "Table") as table_cls:
        table_cls.return_value.get.return_value = existing
        result = mt.publish_union(mock.MagicMock(), "ToolView", union, "syn1")
    return existing, result


def test_republish_clears_rows_before_store_rows():
    existing, result = _publish()
    names = [c[0] for c in existing.method_calls]
    assert names.index("delete_rows") < names.index("store_rows")
    assert "ROW_ID, ROW_VERSION" in existing.delete_rows.call_args.kwargs["query"]
    assert result == "syn9"


def test_republish_snapshots_before_delete():
    existing, _ = _publish()
    names = [c[0] for c in existing.method_calls]
    assert names.index("snapshot") < names.index("query") < names.index("delete_rows")


def test_failed_store_restores_snapshot_rows_and_raises():
    import pytest

    calls = []

    def store_rows(values, synapse_client):
        calls.append(values)
        if len(calls) == 1:
            raise RuntimeError("rejected")

    with pytest.raises(RuntimeError, match="rejected"):
        existing, _ = _publish(store_rows=store_rows)
    assert len(calls) == 2  # union store failed, then previous rows put back
    assert calls[1] is not calls[0]


def test_republish_with_different_maximum_size_stops():
    import pytest
    from synapseclient.models import Column, ColumnType

    cols = [Column(name=c.name, column_type=c.column_type, maximum_size=50 if c.maximum_size else None)
            for c in mt.union_schema("ToolView")]
    with pytest.raises(ValueError, match="differ from the fixed"):
        _publish(cols)


def test_republish_with_different_columns_stops():
    import pytest

    with pytest.raises(ValueError, match="differ from the fixed"):
        _publish(mt.union_schema("ToolView")[:-1])


def test_list_columns_are_largetext():
    from synapseclient.models import ColumnType

    cols = {c.name: c for c in mt.union_schema("ToolView")}
    assert cols["Tool Operation"].column_type == ColumnType.LARGETEXT


def test_comma_in_list_item_warns_with_column_and_key(caplog):
    g = _rs(["t1"], ["A"], GrantViewKey=[["CA1", "Smith, J"]])
    with caplog.at_level(logging.WARNING):
        df, _ = _union({"syn1": g}, ["syn1"])
    assert df["GrantView Key"].tolist() == ["CA1, Smith, J"]
    assert any("GrantView Key" in r.message and "t1" in r.message for r in caplog.records)


def test_empty_union_is_not_published(tmp_path):
    import pytest
    with mock.patch.object(mt, "build_union", return_value=(pd.DataFrame(columns=["Component"]),
                           {"grants_scanned": 1, "recordsets_found": 0, "missing": 1, "rows": 0,
                            "duplicate_keys": 0})), \
         mock.patch.object(mt, "publish_union") as publish:
        with pytest.raises(SystemExit):
            mt.run_recordset_mode(mock.MagicMock(), "ToolView", str(tmp_path), ["syn1"], "synT", publish=True)
        publish.assert_not_called()
        mt.run_recordset_mode(mock.MagicMock(), "ToolView", str(tmp_path), ["syn1"], "synT",
                              publish=True, allow_empty=True)
        publish.assert_called_once()


def test_json_keyword_array_round_trips_to_the_portal_string():
    """JSON array -> RecordSet-style list -> union join -> exactly ", ".join(items)."""
    import recordsets

    items = ["Aged, 80 and over", "Mice, Inbred C57BL", "Humans"]
    up = pd.DataFrame({"PublicationViewId": ["p1"], "PublicationKeywords": [json.dumps(items)]})
    parsed = recordsets.parse_upload(up, {"PublicationKeywords"})
    assert parsed.loc[0, "PublicationKeywords"] == items
    joined = mt._join_lists(parsed, "PublicationViewId")
    assert joined.loc[0, "PublicationKeywords"] == ", ".join(items)
