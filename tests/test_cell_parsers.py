"""How each list-cell parser treats the same inputs (characterization of the existing behavior).

``SPLIT`` is the reference: what ``utils.split_cell`` gives for each input. The other parsers state
only where they differ from it. Each row of ``INPUTS`` is one form a cell can take.
"""

import pandas as pd
import pytest

import comma_terms
import create_grant_projects as cgp
import model_contract
import promote_staging
import recordsets
import utils
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
    "set": ["{'a'}"],  # a set is not parsed: its repr is split
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


def test_split_cell():
    check(utils.split_cell)


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


def test_split_cell_keeps_a_comma_term_split():
    assert utils.split_cell("Aged, 80 and over") == ["Aged", "80 and over"]


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
    got = {k: utils.parse_json_list(v) for k, v in INPUTS.items() if isinstance(v, str)}
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
    # not parsed as JSON; a null item of a list is dropped, a set is read
    overrides = {
        "set": ["a"],
        "list_none": ["1"],
        "list_nan": ["x"],
        "json": ['["a"', '"b', 'c"]'],
        "json_padded": ['[ "a" ]'],
        "json_empty": ["[]"],
        "json_numbers": ["[1", "2]"],
        "json_null": ['["a"', "null]"],
        "json_nested": ['[["a"]', '"b"]'],
        "json_blank_items": ['[""', '" x "]'],
        "json_escape": [r'["a\\u2010b"]'],
        "py_quoted": ["['a'", "'b']"],
    }
    check(cgp._split_people, overrides)


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


def test_convert_to_stringlist():
    strings = pd.Series(
        ["a, b", " a ,b ", "a,,b", ",a", "a,", "", "   ", '["a","b, c"]', "['a', 'b']", "Aged, 80 and over, Humans"],
        dtype=object,
    )
    assert utils.convert_to_stringlist(strings).tolist() == [
        ["a", "b"],
        [" a ", "b "],  # whitespace is not stripped except after a comma
        ["a", "", "b"],  # empty pieces are kept
        ["", "a"],
        ["a", ""],
        [],
        ["   "],
        ['["a"', '"b', 'c"]'],  # a JSON array is split on its commas
        ["['a'", "'b']"],
        ["Aged", "80 and over", "Humans"],
    ]


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
    with pytest.raises(ValueError):  # a list of several values is not a null test result
        cgp._text(["a", "b"])
