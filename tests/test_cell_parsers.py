"""How each list-cell parser treats the same inputs.

``SPLIT`` is the reference: what ``cells.parse_list`` gives for each input (it was ``utils.split_cell``).
The other parsers state only where they differ from it. Each row of ``INPUTS`` is one form a cell can take.
"""

import numpy as np
import pandas as pd
import pytest

import cells
import comma_terms
import create_grant_projects as cgp
import model_contract
import promote_staging
import recordsets
import value_maps

NAN = float("nan")

INPUTS = {
    "plain": "a, b",
    "padded": " a ,b ",
    "doubled": "a,,b",
    "leading": ",a",
    "trailing": "a,",
    "empty": "",
    "blank": "   ",
    "none": None,
    "nan": NAN,
    "na": pd.NA,
    "int": 5,
    "float": 1.5,
    "bool": True,
    "list": ["a", " b ", ""],
    "tuple": ("a", "b"),
    "set": {"a"},
    "list_none": [None, 1],
    "list_nan": [NAN, "x"],
    "json": '["a","b, c"]',
    "json_padded": ' [ "a" ] ',
    "json_empty": "[]",
    "json_numbers": "[1, 2]",
    "json_null": '["a", null]',
    "py_quoted": "['a', 'b']",
    "bracketed": "[a, b]",
    "unclosed": '["a"',
    "json_object": '{"a": 1}',
    "json_nested": '[["a"],"b"]',
    "json_blank_items": '["", " x "]',
    "comma_term": "Aged, 80 and over, Humans",
    "json_escape": r'["a\\u2010b"]',
}

SPLIT = {
    "plain": ["a", "b"],
    "padded": ["a", "b"],
    "doubled": ["a", "b"],
    "leading": ["a"],
    "trailing": ["a"],
    "empty": [],
    "blank": [],
    "none": [],
    "nan": [],
    "na": [],
    "int": ["5"],
    "float": ["1.5"],
    "bool": ["True"],
    "list": ["a", "b"],
    "tuple": ["a", "b"],
    "set": ["a"],  # changed: utils.split_cell split the repr of a set ("{'a'}")
    "list_none": ["None", "1"],  # a null item becomes the text "None"
    "list_nan": ["nan", "x"],
    "json": ["a", "b, c"],
    "json_padded": ["a"],
    "json_empty": [],
    "json_numbers": ["1", "2"],
    "json_null": ["a", "None"],
    "py_quoted": ["['a'", "'b']"],  # a single-quoted Python list is split on its commas
    "bracketed": ["[a", "b]"],
    "unclosed": ['["a"'],
    "json_object": ['{"a": 1}'],
    "json_nested": ["['a']", "b"],
    "json_blank_items": ["x"],
    "comma_term": ["Aged", "80 and over", "Humans"],
    "json_escape": ["a\\u2010b"],
}


def check(parser, overrides=None):
    expected = {**SPLIT, **(overrides or {})}
    assert set(expected) == set(INPUTS)
    for name, value in INPUTS.items():
        assert parser(value) == expected[name], name


def test_parse_list():
    check(cells.parse_list)


def test_first_item_is_the_first_list_item_or_empty():
    assert cells.first_item("syn5, syn6") == "syn5"
    assert cells.first_item(", syn5,syn6") == "syn5"
    assert cells.first_item(["a", "b"]) == "a"
    assert cells.first_item("") == "" and cells.first_item(None) == "" and cells.first_item(np.nan) == ""


def test_value_maps_split_items_matches_split_cell():
    check(value_maps.split_items)


def test_split_items_keyword_keeps_known_comma_terms_whole(tmp_path):
    path = tmp_path / "terms.csv"
    path.write_text("term,source,added\n\"Aged, 80 and over\",x,2026-01-01\n", encoding="utf-8")
    check(lambda v: comma_terms.split_keyword_cell(v, path=path), {"comma_term": ["Aged, 80 and over", "Humans"]})
    check(lambda v: comma_terms.split_keyword_cell(v, path=tmp_path / "none.csv"))
    rejoined = []
    assert comma_terms.split_keyword_cell("Aged, 80 and over, Humans", rejoined, path) == ["Aged, 80 and over", "Humans"]
    assert rejoined == [["Aged", "80 and over"]]


def test_parse_list_splits_a_comma_term_unless_given_a_grouping():
    assert cells.parse_list("Aged, 80 and over") == ["Aged", "80 and over"]
    group = lambda pieces: [["Aged", "80 and over"]] if pieces == ["Aged", "80 and over"] else [[p] for p in pieces]  # noqa: E731
    assert cells.parse_list("Aged, 80 and over", group) == ["Aged, 80 and over"]
    assert cells.parse_list('["Aged", "80 and over"]', group) == ["Aged", "80 and over"]  # JSON items are not grouped
    assert cells.parse_list(["Aged", "80 and over"], group) == ["Aged", "80 and over"]


def test_is_null():
    assert all(cells.is_null(v) for v in [None, NAN, pd.NA, pd.NaT, np.nan])
    not_null = ["", " ", "nan", 0, False, [], ["a", "b"], [None], ("a", "b"), {"a"}, np.array([1, 2]), np.array([np.nan]), pd.Series([1, 2])]
    assert not any(cells.is_null(v) for v in not_null)  # a list-like value does not raise


@pytest.mark.parametrize("column", ["", "StudyKeywords"])
def test_recordsets_split_list_cell(column):
    # a list is returned as it is, JSON items are kept as stored (unescaped by Grid rules, not stripped, blanks kept)
    expected = {
        "list": ["a", " b ", ""],
        "list_none": [None, 1],
        "list_nan": [NAN, "x"],
        "json_blank_items": ["", " x "],
        "json_escape": ["a‐b"],
    }
    if column:
        expected["comma_term"] = ["Aged, 80 and over", "Humans"]  # the repository's comma_terms.csv knows this term
    out = {**SPLIT, **expected}
    for name, value in INPUTS.items():
        got = recordsets._split_list_cell(value, column)
        if name == "list_nan":
            assert got[0] is NAN and got[1:] == ["x"]
        else:
            assert got == out[name], name


def test_parse_json_list():
    got = {k: cells.json_list(v) for k, v in INPUTS.items() if isinstance(v, str)}
    assert got == {
        "plain": None, "padded": None, "doubled": None, "leading": None, "trailing": None, "empty": None, "blank": None,
        "json": ["a", "b, c"], "json_padded": ["a"], "json_empty": [], "json_numbers": [1, 2], "json_null": ["a", None],
        "py_quoted": None, "bracketed": None, "unclosed": None, "json_object": None,
        "json_nested": [["a"], "b"], "json_blank_items": ["", " x "], "comma_term": None, "json_escape": ["a\\u2010b"],
    }


def test_model_contract_parse_json_list_changes_only_json_arrays():
    for name, value in INPUTS.items():
        got = model_contract._parse_json_list(value)
        if name in {"json", "json_padded", "json_empty", "json_numbers", "json_null", "json_nested", "json_blank_items", "json_escape"}:
            continue
        if name in {"nan", "na"}:
            assert pd.isna(got), name
        else:
            assert got == value, name
    assert model_contract._parse_json_list('["a","b, c"]') == ["a", "b, c"]
    assert model_contract._parse_json_list(' [ "a" ] ') == ["a"]
    assert model_contract._parse_json_list("[]") == []
    assert model_contract._parse_json_list("[1, 2]") == ["1", "2"]
    assert model_contract._parse_json_list('["a", null]') == ["a", "None"]
    assert model_contract._parse_json_list('[["a"],"b"]') == ["['a']", "b"]
    assert model_contract._parse_json_list('["", " x "]') == ["", " x "]  # items kept as stored
    assert model_contract._parse_json_list(INPUTS["json_escape"]) == ["a‐b"]


def test_recordsets_parse_cell():
    assert recordsets.parse_cell(None) == ""
    assert recordsets.parse_cell(NAN) == ""
    assert recordsets.parse_cell(pd.NA) == ""
    assert recordsets.parse_cell(5) == "5"
    assert recordsets.parse_cell(("a", "b")) == "('a', 'b')"  # a tuple is not a list cell
    assert recordsets.parse_cell(["a", " b ", ""]) == ["a", " b ", ""]
    assert recordsets.parse_cell(" a ,b ") == " a ,b "  # a plain string is returned as it is, not split
    assert recordsets.parse_cell('["a","b, c"]') == ["a", "b, c"]
    assert recordsets.parse_cell(' [ "a" ] ') == ["a"]
    assert recordsets.parse_cell("[]") == []
    assert recordsets.parse_cell("[1, 2]") == ["1", "2"]
    assert recordsets.parse_cell('["a", null]') == ["a", "None"]
    assert recordsets.parse_cell("['a', 'b']") == "['a', 'b']"
    assert recordsets.parse_cell('["a"') == '["a"'
    assert recordsets.parse_cell('{"a": 1}') == '{"a": 1}'
    assert recordsets.parse_cell('["", " x "]') == ["", " x "]
    assert recordsets.parse_cell(INPUTS["json_escape"]) == ["a‐b"]


def test_split_people():
    # a null item of a list is dropped (parse_list keeps it as "None" / "nan")
    check(cgp._split_people, {"list_none": ["1"], "list_nan": ["x"]})
    # fix (a): a JSON-array cell gives its items. The old function split its text on the commas:
    assert cgp._split_people('["Ann Lee","Bo, Jr."]') == ["Ann Lee", "Bo, Jr."]  # was ['["Ann Lee"', '"Bo', 'Jr."]']
    assert cgp._split_people("[]") == []  # was ["[]"]


def test_list_to_string():
    f = cgp._list_to_string
    assert f(["a", " b ", ""]) == "a,  b , "
    assert f(("a", "b")) == "a, b"
    assert f({"a"}) == "a"
    assert f([None, 1]) == "None, 1"
    assert f('["a","b, c"]') == "a, b, c"
    assert f(' [ "a" ] ') == "a"
    assert f("[]") == ""
    assert f("[1, 2]") == "1, 2"
    assert f('["a", null]') == '["a", null]'  # not a Python literal: returned as it is
    assert f("['a', 'b']") == "a, b"  # a single-quoted Python list is read
    assert f("[a, b]") == "[a, b]"
    assert f('["a"') == '["a"'
    assert f('[["a"],"b"]') == "['a'], b"
    assert f(" a ,b ") == " a ,b "  # a plain string is returned as it is
    assert f("") == ""
    assert f(None) is None
    assert f(5) == 5
    assert f(NAN) is NAN


def test_null_tests():
    nulls = [None, NAN, pd.NA]
    assert [recordsets._to_dt(v) for v in nulls] == [pd.NaT] * 3
    assert [recordsets._int_or_none(v) for v in nulls] == [None] * 3
    assert [cgp._text(v) for v in nulls] == [""] * 3
    assert [promote_staging._cell(v) for v in nulls] == [""] * 3
    assert recordsets._int_or_none(0) == 0
    assert recordsets._int_or_none("7") == 7
    assert cgp._text(0) == "0"
    assert cgp._text("") == ""
    assert promote_staging._cell(0) == 0
    assert promote_staging._cell("") == ""
    assert promote_staging._cell(["a", None]) == ("a", "")
    assert str(recordsets._to_dt("2026-01-02")) == "2026-01-02 00:00:00+00:00"
    assert str(recordsets._to_dt(1000)) == "1970-01-01 00:00:01+00:00"
    assert cgp._text(["a", "b"]) == "['a', 'b']"  # changed: a list of several values made pd.isna raise ValueError


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Pending Annotation", True),
        ("pending annotation", True),
        ("  PENDING ANNOTATION  ", True),
        (["Pending Annotation", "pending annotation"], True),
        ("Pending Annotation, Pending Annotation", True),
        (["Pending Annotation", "Lung"], False),
        ('["Pending Annotation"]', True),
        ('["Pending Annotation", "Lung"]', False),
        ("", False),
        ("  ", False),
        (None, False),
        (float("nan"), False),
        ([], False),
    ],
)
def test_is_placeholder(value, expected):
    assert cells.is_placeholder(value) is expected
