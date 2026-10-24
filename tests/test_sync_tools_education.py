"""Offline tests for sync_tools.py and sync_education.py (14.0.0 and 13.1.0 headers)."""

import pandas as pd
import pytest

import crosswalk as cw
import model_contract as mc
import sync_education
import sync_tools


def _live(component, **overrides):
    """Live types as of today: list columns STRING_LIST, except license (STRING until I4)."""
    types = {col: "STRING" for col in cw.expected_columns(component, [])}
    for _, col, kind in cw.CROSSWALK[component]:
        if kind == "list":
            types[col] = "STRING_LIST"
    types["license"] = "STRING"
    types.update(overrides)
    return types


def _manifest(component, values):
    """A 14.0.0-header manifest: every template column, ``values`` filled in."""
    cols = mc.template_headers(component)
    df = pd.DataFrame([{c: "" for c in cols}])
    for k, v in values.items():
        df[k] = v
    return df


def _legacy(df, renames):
    return df.rename(columns=renames)


def _load(df, component):
    df = mc.rename_legacy_headers(df.fillna(""), component)
    return mc.normalize_headers(df, to="keys")


GRANTS = pd.DataFrame(
    {
        "grantId": ["g1"],
        "grantNumber": ["CA1"],
        "grantName": ["Grant One"],
        "theme": [["Heterogeneity"]],
        "consortium": [["CSBC"]],
    }
)


def _tool_14():
    return _manifest(
        "ToolView",
        {
            "Tool Name": "T1",
            "Tool Homepage": "https://example.org/t1",
            "GrantView Key": "CA1",
            "Tool Topic": "A, B, A",
            "License": "MIT",
            "Tool Download Url": "https://www.synapse.org/#!Synapse:syn1",
            "Tool Link Url": "",
            "PublicationView Key": "",
        },
    )


def _tool_out(df, live):
    return sync_tools.clean_table(sync_tools.add_missing_info(_load(df, "ToolView"), GRANTS), live)


def test_tool_14_and_13_give_same_output():
    new = _tool_14()
    old = _legacy(new, {"License": "Tool License", "GrantView Key": "Tool Grant Number"})
    assert "Tool License" in old.columns
    live = _live("ToolView")
    a, b = _tool_out(new, live), _tool_out(old, live)
    pd.testing.assert_frame_equal(a, b)
    assert list(a.columns) == cw.expected_columns("ToolView", [])
    row = a.iloc[0]
    assert row["license"] == "MIT"
    assert row["theme"] == ["Heterogeneity"] and row["consortium"] == ["CSBC"]
    assert row["topic"] == ["A", "B"]
    assert row["pubMedId"] == ["Pending Annotation"]
    assert row["downloadLink"] == ""
    assert "synapse.org" in row["synapseLink"]


def test_tool_license_stays_string_until_live_is_list():
    live = _live("ToolView", license="STRING")
    assert _tool_out(_tool_14(), live).iloc[0]["license"] == "MIT"
    live = _live("ToolView", license="STRING_LIST")
    assert _tool_out(_tool_14(), live).iloc[0]["license"] == ["MIT"]


def _edu_14():
    return _manifest(
        "EducationalResource",
        {
            "Component": "EducationalResource",
            "Resource Title": "R1",
            "Resource Link": "https://www.synapse.org/#!Synapse:syn17012903",
            "Resource Topic": "Outreach, Diversity/Equity/Inclusion",
            "Resource Alias": "syn17012903",
            "GrantView Key": "CA1, CA2",
            "License": "CC-BY-4.0",
        },
    )


def _edu_out(df, live):
    return sync_education.clean_table(
        sync_education.add_missing_info(_load(df, "EducationalResource")), live
    )


def test_education_14_and_13_give_same_output():
    new = _edu_14()
    old = _legacy(new, {"License": "Resource License", "GrantView Key": "Resource Grant Number"})
    assert "Resource License" in old.columns
    live = _live("EducationalResource", publicationId="STRING")
    a, b = _edu_out(new, live), _edu_out(old, live)
    pd.testing.assert_frame_equal(a, b)
    assert list(a.columns) == cw.expected_columns("EducationalResource", [])
    row = a.iloc[0]
    assert row["license"] == "CC-BY-4.0"
    assert row["grantNumber"] == ["CA1", "CA2"]
    assert row["topic"] == ["Outreach"]
    assert row["publicationId"] == ""
    assert row["synapseLink"].startswith("[syn17012903]")


def test_education_license_list_when_live_is_list():
    live = _live("EducationalResource", publicationId="STRING", license="STRING_LIST")
    assert _edu_out(_edu_14(), live).iloc[0]["license"] == ["CC-BY-4.0"]
