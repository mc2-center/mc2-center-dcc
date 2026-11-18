"""portal_tables/promote_staging.py: copy the staging portal tables to production."""

from types import SimpleNamespace

import pandas as pd
import pytest

import promote_staging
import utils


def _col(name, column_type="STRING", maximum_size=50):
    return SimpleNamespace(name=name, column_type=column_type, maximum_size=maximum_size,
                           maximum_list_length=None, enum_values=None)


def _columns(*cols):
    return {c.name: c for c in cols}


GRANT_COLS = ["grantNumber", "grantName"]
TOOL_COLS = ["toolName", "description"]


class World:
    """Fake Synapse: table id -> (columns, rows), plus a call log."""

    def __init__(self):
        self.tables = {}
        self.calls = []

    def add(self, resource, production, rows, cols=None):
        tid = utils.portal_table(resource, production)
        names = cols or (GRANT_COLS if resource == "grant" else TOOL_COLS)
        self.tables[tid] = (_columns(*[_col(n) for n in names]), pd.DataFrame(rows, columns=names))

    def table_class(self):
        world = self

        class FakeTable:
            def __init__(self, id):
                self.id = id

            def get(self, include_columns=True, synapse_client=None):
                return SimpleNamespace(columns=world.tables[self.id][0])

            def query(self, query, synapse_client=None):
                df = world.tables[self.id][1].copy()
                df.insert(0, "ROW_ID", range(len(df)))
                df["ROW_VERSION"] = 1
                return df

            def snapshot(self, comment=None, synapse_client=None):
                world.calls.append(("snapshot", self.id, comment))

        return FakeTable


@pytest.fixture
def world(monkeypatch):
    w = World()
    monkeypatch.setattr(promote_staging, "Table", w.table_class())
    monkeypatch.setattr(utils, "syn_login", lambda: object())

    def fake_update(syn, table_id, df, component=None):
        w.calls.append(("update_table", table_id))
        if table_id in w.corrupt:
            df = df.iloc[:-1]
        w.tables[table_id] = (w.tables[table_id][0], utils.strip_newlines(df).reset_index(drop=True))

    w.corrupt = set()
    monkeypatch.setattr(utils, "update_table", fake_update)
    return w


def _run(tmp_path, *extra):
    return promote_staging.run(["--report-dir", str(tmp_path), "--run-id", "r1", *extra])


def _summary(tmp_path):
    return pd.read_csv(tmp_path / "r1" / "summary.csv", keep_default_na=False).set_index("resource")


def _writes(world):
    return [c for c in world.calls if c[0] in ("snapshot", "update_table")]


def test_schema_mismatch_writes_nothing(world, tmp_path):
    world.add("grant", False, [["G1", "a"]])
    world.add("grant", True, [["G1", "a"]])
    world.add("tool", False, [["T1", "d"]])
    world.add("tool", True, [["T1", "d"]], cols=["toolName", "summary"])
    assert _run(tmp_path, "--tables", "grant", "tool", "--execute") != 0
    assert _writes(world) == []
    diff = pd.read_csv(tmp_path / "r1" / "schema_diff.csv")
    assert list(diff.columns) == ["table", "column", "attribute", "staging", "production"]
    assert set(diff["table"]) == {"tool"} and "name" in set(diff["attribute"])


def test_dry_run_counts_and_no_writes(world, tmp_path):
    world.add("grant", False, [["G1", "same"], ["G2", "new name"], ["G3", "added"]])
    world.add("grant", True, [["G1", "same"], ["G2", "old name"], ["G4", "removed"]])
    assert _run(tmp_path, "--tables", "grant") == 0
    assert _writes(world) == []
    row = _summary(tmp_path).loc["grant"]
    assert (row["added"], row["removed"], row["changed"]) == (1, 1, 1)
    assert row["key_used"] == "grantNumber" and row["status"] == "dry_run"
    diff = pd.read_csv(tmp_path / "r1" / "grant_diff.csv", keep_default_na=False)
    assert sorted(diff["change"]) == ["added", "changed", "removed"]
    changed = diff[diff["change"] == "changed"].iloc[0]
    assert (changed["key"], changed["column"], changed["staging"], changed["production"]) == (
        "G2", "grantName", "new name", "old name")


def test_duplicate_key_falls_back_to_whole_row(world, tmp_path, capsys):
    world.add("grant", False, [["G1", "a"], ["G1", "b"], ["G2", "c"]])
    world.add("grant", True, [["G1", "a"], ["G2", "x"]])
    assert _run(tmp_path, "--tables", "grant") == 0
    row = _summary(tmp_path).loc["grant"]
    assert row["key_used"] == "whole row"
    assert (row["added"], row["removed"], row["changed"]) == (2, 1, 0)
    assert "compared whole rows" in capsys.readouterr().out


def test_list_cells_compare_exactly(world, tmp_path):
    world.add("tool", False, [["T1", ["a", "b"]]])
    world.add("tool", True, [["T1", ["b", "a"]]])
    assert _run(tmp_path, "--tables", "tool") == 0
    assert _summary(tmp_path).loc["tool"]["changed"] == 1


def test_execute_snapshots_staging_then_updates_production(world, tmp_path):
    world.add("grant", False, [["G1", "a"], ["G2", "b"]])
    world.add("grant", True, [["G1", "old"]])
    world.add("tool", False, [["T1", "d"]])
    world.add("tool", True, [])
    assert _run(tmp_path, "--tables", "tool", "grant", "--execute") == 0
    g_s, g_p = utils.portal_table("grant"), utils.portal_table("grant", True)
    t_s, t_p = utils.portal_table("tool"), utils.portal_table("tool", True)
    assert _writes(world) == [
        ("snapshot", t_s, f"promote r1 to {t_p}"), ("update_table", t_p),
        ("snapshot", g_s, f"promote r1 to {g_p}"), ("update_table", g_p),
    ]
    summary = _summary(tmp_path)
    assert list(summary["status"]) == ["promoted", "promoted"]
    assert len(world.tables[g_p][1]) == 2


def test_readback_mismatch_stops_later_tables(world, tmp_path):
    world.add("grant", False, [["G1", "a"], ["G2", "b"]])
    world.add("grant", True, [["G1", "old"]])
    world.add("tool", False, [["T1", "d"]])
    world.add("tool", True, [["T1", "old"]])
    world.corrupt.add(utils.portal_table("grant", True))
    assert _run(tmp_path, "--tables", "grant", "tool", "--execute") != 0
    assert [c[1] for c in _writes(world)] == [
        utils.portal_table("grant"), utils.portal_table("grant", True)]
    summary = _summary(tmp_path)
    assert summary.loc["grant", "status"] == "readback_mismatch"
    assert summary.loc["tool", "status"] == "not_run"


def test_newlines_stripped_on_write_still_promote(world, tmp_path):
    world.add("grant", False, [["G1", "line one\nline two"]])
    world.add("grant", True, [["G1", "old"]])
    assert _run(tmp_path, "--tables", "grant", "--execute") == 0
    assert _summary(tmp_path).loc["grant", "status"] == "promoted"
