"""Shared helpers in portal_tables/utils.py."""

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
