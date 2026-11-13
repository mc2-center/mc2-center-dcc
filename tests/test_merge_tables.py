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


def test_publish_is_refused_with_a_grant_subset():
    argv = ["merge_tables.py", "--target_record_set", "-n", "ToolView", "-t", "syn9",
            "--publish", "--grant-projects", "syn1", "syn2"]
    with mock.patch("sys.argv", argv), \
         mock.patch.object(mt.utils, "syn_login") as login, \
         mock.patch.object(mt, "run_recordset_mode") as run:
        with pytest.raises(SystemExit, match="grant-projects"):
            mt.main()
    login.assert_not_called()
    run.assert_not_called()
