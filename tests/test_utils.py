"""Shared helpers in portal_tables/utils.py."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

import utils


def _nullable_frame():
    return pd.DataFrame(
        {
            "year": pd.array([2020, None], dtype="Int64"),
            "score": pd.array([1.5, None], dtype="Float64"),
            "flag": pd.array([True, None], dtype="boolean"),
            "name": pd.array(["a", None], dtype="string"),
        }
    )


def test_fillna_raises_on_nullable_dtypes():
    """The failure fill_blank exists to avoid."""
    with pytest.raises(TypeError):
        _nullable_frame().fillna("")


def test_fill_blank_fills_nullable_columns_with_empty_strings():
    out = utils.fill_blank(_nullable_frame())
    assert out["year"].tolist() == [2020, ""]
    assert out["score"].tolist() == [1.5, ""]
    assert out["flag"].tolist() == [True, ""]
    assert out["name"].tolist() == ["a", ""]


def test_fill_blank_numeric_false_keeps_numeric_columns_null():
    out = utils.fill_blank(_nullable_frame(), numeric=False)
    assert list(out.columns) == ["year", "score", "flag", "name"]
    assert out["year"].dtype == "Int64" and pd.isna(out["year"].iloc[1])
    assert out["flag"].dtype == "boolean" and pd.isna(out["flag"].iloc[1])
    assert out["name"].tolist() == ["a", ""]


def test_fill_blank_leaves_list_cells_and_the_input_alone():
    df = pd.DataFrame({"tags": [["a", "b"], None]})
    out = utils.fill_blank(df)
    assert out["tags"].iloc[0] == ["a", "b"] and out["tags"].iloc[1] == ""
    assert df["tags"].iloc[1] is None


# --- cell-size guard -------------------------------------------------------


def _live_table(**cols):
    """A mocked live table; ``cols`` maps name -> (type, maximum_size, maximum_list_length)."""
    table = MagicMock()
    table.columns = {
        n: MagicMock(column_type=t, maximum_size=size, maximum_list_length=items)
        for n, (t, size, items) in cols.items()
    }
    table.query.return_value = pd.DataFrame({"old": [1]})
    fake = MagicMock()
    fake.return_value.get.return_value = table
    return table, fake


def _update(monkeypatch, table, fake, df):
    monkeypatch.setattr(utils, "Table", fake)
    utils.update_table(None, "synX", df)


def test_oversized_string_refused_before_snapshot_and_delete(monkeypatch):
    table, fake = _live_table(name=("STRING", 5, None), n=("INTEGER", None, None))
    df = pd.DataFrame({"name": ["ok", "toolong"], "n": [1, 2]})
    with pytest.raises(ValueError, match=r"'name'.*length 7.*limit is 5"):
        _update(monkeypatch, table, fake, df)
    table.snapshot.assert_not_called()
    table.delete_rows.assert_not_called()
    table.store_rows.assert_not_called()


def test_oversized_list_item_and_list_length_refused(monkeypatch):
    table, fake = _live_table(access=("STRING_LIST", 10, 2))
    with pytest.raises(ValueError, match=r"'access'.*length 11.*limit is 10"):
        _update(monkeypatch, table, fake, pd.DataFrame({"access": [["short", "x" * 11]]}))
    with pytest.raises(ValueError, match=r"'access'.*3 list items.*limit is 2"):
        _update(monkeypatch, table, fake, pd.DataFrame({"access": [["a", "b", "c"]]}))
    table.snapshot.assert_not_called()
    table.delete_rows.assert_not_called()


def test_cells_within_limits_and_nulls_are_stored(monkeypatch):
    table, fake = _live_table(
        name=("LINK", 5, None), access=("STRING_LIST", 10, 2), note=("LARGETEXT", None, None)
    )
    df = pd.DataFrame(
        {"name": ["12345", None], "access": [["x" * 10, "y"], None], "note": ["z" * 5000, ""]}
    )
    _update(monkeypatch, table, fake, df)
    table.snapshot.assert_called_once()
    table.store_rows.assert_called_once()


# --- strip_newlines ----------------------------------------------------------


def test_strip_newlines_only_touches_text_columns():
    df = pd.DataFrame(
        {
            "year": pd.array([2020, None], dtype="Int64"),
            "score": [1.5, 2.0],
            "text": ["a\r\nb", "c"],
            "mixed": [["x\ny"], "p\nq"],
            "name": pd.array(["m\nn", None], dtype="string"),
        }
    )
    out = utils.strip_newlines(df)
    assert out["year"].dtype == "Int64" and out["year"].iloc[0] == 2020
    assert out["score"].dtype == "float64"
    assert out["text"].tolist() == ["a b", "c"]
    assert out["mixed"].iloc[0] == ["x\ny"] and out["mixed"].iloc[1] == "p q"
    assert out["name"].iloc[0] == "m n" and pd.isna(out["name"].iloc[1])
    assert df["text"].iloc[0] == "a\r\nb"  # input untouched
