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


def test_grant_projects_include_the_affiliate_project():
    table = mock.MagicMock()
    table.query.return_value = pd.DataFrame({"grantId": ["syn1", "", "syn2"]})
    with mock.patch.object(mt, "Table", return_value=table):
        assert mt.list_grant_projects(mock.MagicMock()) == ["syn1", "syn2", "syn52963211"]
