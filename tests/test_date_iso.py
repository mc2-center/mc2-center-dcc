"""The date_iso rule of value_maps: format: date columns."""
import pytest

import value_maps as vm


@pytest.mark.parametrize("value,expected", [
    ("6/1/14", ("2014-06-01", "date_iso")),
    ("12/31/2021", ("2021-12-31", "date_iso")),
    ("2/29/20", ("2020-02-29", "date_iso")),
    ("2020-05-31", ("2020-05-31", None)),
    ("2/30/20", ("2/30/20", "unmapped")),
    ("2020-13-01", ("2020-13-01", "unmapped")),
    ("May 31, 2020", ("May 31, 2020", "unmapped")),
    ("31/5/2020", ("31/5/2020", "unmapped")),
])
def test_normalize_date(value, expected):
    assert vm.normalize_date(value) == expected


def test_two_digit_year_pivot_is_ten_years_ahead():
    import datetime

    now = datetime.date.today().year
    assert vm.normalize_date("3/15/98") == ("1998-03-15", "date_iso")
    assert vm.normalize_date("5/31/30") == ("2030-05-31", "date_iso")
    latest = (now + 10) % 100   # newest year still read as 20yy
    assert vm.normalize_date(f"1/1/{latest:02d}")[0] == f"{2000 + latest}-01-01"
    assert vm.normalize_date(f"1/1/{(latest + 1) % 100:02d}")[0] == f"{1900 + (latest + 1) % 100}-01-01"


def test_date_columns_come_from_the_schema():
    assert vm.date_columns("GrantView") == {"GrantStartDate", "GrantEndDate", "EmbargoEndDate"}
    assert vm.date_columns("ToolView") == {"ToolDateLastModified", "ToolReleaseDate"}
    assert vm.date_columns("PublicationView") == set()


def test_the_normalizer_applies_it_to_date_columns_only():
    import pandas as pd

    norm = vm.Normalizer.for_component("ToolView")
    df = pd.DataFrame({"ToolViewId": ["t1"], "ToolReleaseDate": ["1/2/21"], "ToolName": ["1/2/21"]})
    out, changes, unmapped = norm.normalize_frame(df, "ToolViewId")
    assert out["ToolReleaseDate"].tolist() == ["2021-01-02"] and out["ToolName"].tolist() == ["1/2/21"]
    assert changes[["column", "rule"]].values.tolist() == [["ToolReleaseDate", "date_iso"]]
    assert unmapped.empty
