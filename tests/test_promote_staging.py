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

            def query(self, query, include_row_id_and_row_version=True, synapse_client=None):
                df = world.tables[self.id][1].copy()
                if include_row_id_and_row_version:
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
    monkeypatch.setattr(utils, "Table", promote_staging.Table)  # read_table queries through utils
    monkeypatch.setattr(utils, "syn_login", lambda: object())

    def fake_update(syn, table_id, df, component=None, allow_production=False):
        w.calls.append(("update_table", table_id))
        w.allow_production.append(allow_production)
        if table_id in w.corrupt:
            df = df.iloc[:-1]
        w.tables[table_id] = (w.tables[table_id][0], utils.strip_newlines(df).reset_index(drop=True))

    w.corrupt = set()
    w.allow_production = []
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


def _csv(tmp_path, name):
    return pd.read_csv(tmp_path / "r1" / name, keep_default_na=False)


def test_dry_run_counts_and_no_writes(world, tmp_path):
    world.add("grant", False, [["G1", "same"], ["G2", "new name"], ["G3", "added"]])
    world.add("grant", True, [["G1", "same"], ["G2", "old name"], ["G4", "removed"]])
    assert _run(tmp_path, "--tables", "grant") == 0
    assert _writes(world) == []
    row = _summary(tmp_path).loc["grant"]
    assert (row["added"], row["removed"], row["changed_records"], row["changed_cells"]) == (1, 1, 1, 1)
    assert row["key"] == "grantNumber" and row["status"] == "dry_run"
    assert list(_csv(tmp_path, "grant_added.csv").iloc[0]) == ["G3", "added"]
    assert list(_csv(tmp_path, "grant_removed.csv").iloc[0]) == ["G4", "removed"]
    changed = _csv(tmp_path, "grant_changed.csv").iloc[0]
    assert (changed["key"], changed["column"], changed["production"], changed["staging"]) == (
        "G2", "grantName", "old name", "new name")
    assert not (tmp_path / "r1" / "duplicate_keys.csv").exists()
    assert not (tmp_path / "r1" / "grant_diff.csv").exists()


def test_empty_files_are_not_written(world, tmp_path):
    world.add("grant", False, [["G1", "a"]])
    world.add("grant", True, [["G1", "a"]])
    assert _run(tmp_path, "--tables", "grant") == 0
    names = {p.name for p in (tmp_path / "r1").iterdir()}
    assert names == {"summary.csv", "format_changes.csv"}


def test_list_order_and_empty_item_are_format_only(world, tmp_path):
    world.add("tool", False, [["T1", ["a", "b"]], ["T2", ["x", ""]], ["T3", ["q"]]])
    world.add("tool", True, [["T1", ["b", "a"]], ["T2", []], ["T3", '["q"]']])
    world.tables[utils.portal_table("tool", True)][1].loc[1, "description"] = ["x"]
    assert _run(tmp_path, "--tables", "tool") == 0
    row = _summary(tmp_path).loc["tool"]
    assert (row["changed_records"], row["changed_cells"], row["format_only_cells"]) == (0, 0, 2)
    kinds = _csv(tmp_path, "format_changes.csv").set_index("kind")["count"].to_dict()
    assert kinds == {"list_order": 1, "empty_list_item": 1}


def test_blank_forms_are_equal(world, tmp_path):
    world.add("tool", False, [["T1", [""]], ["T2", ""], ["T3", "nan"], ["T4", "[]"]])
    world.add("tool", True, [["T1", []], ["T2", None], ["T3", "None"], ["T4", "['']"]])
    assert _run(tmp_path, "--tables", "tool") == 0
    row = _summary(tmp_path).loc["tool"]
    assert (row["changed_records"], row["changed_cells"]) == (0, 0)


def test_newline_vs_space_is_format_only(world, tmp_path):
    world.add("grant", False, [["G1", "line one\nline  two"]])
    world.add("grant", True, [["G1", "line one line two"]])
    assert _run(tmp_path, "--tables", "grant") == 0
    row = _summary(tmp_path).loc["grant"]
    assert (row["changed_cells"], row["format_only_cells"]) == (0, 1)
    assert list(_csv(tmp_path, "format_changes.csv")["kind"]) == ["whitespace"]


def test_booleans_and_numbers_compare_by_value(world, tmp_path):
    cols = ["toolName", "flag", "n"]
    world.add("tool", False, [["T1", "True", 1], ["T2", True, "2"]], cols=cols)
    world.add("tool", True, [["T1", "true", 1.0], ["T2", "true", 2.0]], cols=cols)
    assert _run(tmp_path, "--tables", "tool") == 0
    assert _summary(tmp_path).loc["tool", "changed_cells"] == 0


def test_real_value_change_is_content(world, tmp_path):
    world.add("tool", False, [["T1", ["a", "b"]]])
    world.add("tool", True, [["T1", ["a", "c"]]])
    assert _run(tmp_path, "--tables", "tool") == 0
    row = _summary(tmp_path).loc["tool"]
    assert (row["changed_records"], row["changed_cells"], row["format_only_cells"]) == (1, 1, 0)
    changed = _csv(tmp_path, "tool_changed.csv").iloc[0]
    assert (changed["production"], changed["staging"]) == ('["a", "c"]', '["a", "b"]')


def test_production_duplicate_keys_are_combined(world, tmp_path):
    cols = ["pubMedId", "grant", "title"]
    world.add("publication", False, [["1", ["G1", "G2", "G3"], "t"], ["2", ["G1"], "u"]], cols=cols)
    world.add("publication", True, [["1", ["G2"], "t"], ["1", ["G1"], "t"], ["1", ["G3"], "t"],
                                    ["2", ["G1"], "u"]], cols=cols)
    assert _run(tmp_path, "--tables", "publication") == 0
    row = _summary(tmp_path).loc["publication"]
    assert (row["production_keys_combined"], row["added"], row["removed"], row["changed_cells"]) == (1, 0, 0, 0)
    assert row["production_rows"] == 4 and row["staging_rows"] == 2


def test_production_duplicate_keys_missing_item_is_content(world, tmp_path):
    cols = ["pubMedId", "grant"]
    world.add("publication", False, [["1", ["G1", "G2", "G3"]]], cols=cols)
    world.add("publication", True, [["1", ["G1"]], ["1", ["G2"]]], cols=cols)
    assert _run(tmp_path, "--tables", "publication") == 0
    assert _summary(tmp_path).loc["publication", "changed_cells"] == 1


def test_staging_duplicate_keys_are_reported_not_whole_row(world, tmp_path, capsys):
    world.add("grant", False, [["G1", "a"], ["G1", "b"], ["G2", "c"]])
    world.add("grant", True, [["G1", "a"], ["G2", "x"]])
    assert _run(tmp_path, "--tables", "grant") == 0
    row = _summary(tmp_path).loc["grant"]
    assert (row["staging_duplicate_keys"], row["added"], row["removed"], row["changed_records"]) == (1, 0, 0, 2)
    dup = _csv(tmp_path, "duplicate_keys.csv").iloc[0]
    assert (dup["resource"], dup["key"], dup["row_count"]) == ("grant", "G1", 2)
    assert "whole row" not in capsys.readouterr().out


def test_composite_and_list_valued_keys(world, tmp_path):
    cols = ["projectName", "grant", "summary"]
    world.add("project", False, [["P1", ["G2", "G1"], "a"], ["P1", "G9", "b"], ["P2", ["G1"], "new"]], cols=cols)
    world.add("project", True, [["P1", ["G1", "G2"], "a"], ["P1", "G9", "b"], ["P3", ["G1"], "old"]], cols=cols)
    assert _run(tmp_path, "--tables", "project") == 0
    row = _summary(tmp_path).loc["project"]
    assert row["key"] == "projectName+grant"
    assert (row["added"], row["removed"], row["changed_cells"]) == (1, 1, 0)
    assert _csv(tmp_path, "project_added.csv").iloc[0]["projectName"] == "P2"
    assert _csv(tmp_path, "project_removed.csv").iloc[0]["projectName"] == "P3"


def test_removed_lists_all_rows_of_a_combined_key(world, tmp_path):
    cols = ["pubMedId", "grant"]
    world.add("publication", False, [["9", ["G1"]]], cols=cols)
    world.add("publication", True, [["9", ["G1"]], ["5", ["G1"]], ["5", ["G2"]]], cols=cols)
    assert _run(tmp_path, "--tables", "publication") == 0
    row = _summary(tmp_path).loc["publication"]
    assert (row["added"], row["removed"]) == (0, 1)
    assert len(_csv(tmp_path, "publication_removed.csv")) == 2


def test_count_table_uses_composite_key(world, tmp_path):
    cols = ["theme", "groupBy", "totalCount"]
    world.add("theme_counts", False, [["T1", "g", 2], ["T2", "g", 3]], cols=cols)
    world.add("theme_counts", True, [["T1", "g", 2], ["T2", "g", 1]], cols=cols)
    assert _run(tmp_path, "--tables", "theme_counts") == 0
    row = _summary(tmp_path).loc["theme_counts"]
    assert (row["added"], row["removed"], row["changed_records"]) == (0, 0, 1)


def test_every_resource_has_a_key():
    assert set(promote_staging.KEY_COLUMNS) == set(promote_staging.RESOURCES)


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
    assert world.allow_production == [True, True]


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


def test_text_decimals_do_not_crash_and_keep_version_changes(world, tmp_path):
    cols = ["toolName", "version"]
    world.add("tool", False, [["T1", "2020"], ["T2", "1.10"], ["T3", "2.0"]], cols=cols)
    world.add("tool", True, [["T1", "2020.0"], ["T2", "1.1"], ["T3", "2"]], cols=cols)
    assert _run(tmp_path, "--tables", "tool") == 0
    changed = _csv(tmp_path, "tool_changed.csv")
    assert list(changed["key"]) == ["T2"]  # 1.1 -> 1.10 is content; 2020.0 and 2.0 equal the integers


def test_combined_production_rows_add_no_format_only_cells(world, tmp_path):
    cols = ["pubMedId", "year", "flag"]
    world.add("publication", False, [["1", 2020.0, True]], cols=cols)
    world.add("publication", True, [["1", "2020", "true"], ["1", "2020", "true"]], cols=cols)
    assert _run(tmp_path, "--tables", "publication") == 0
    row = _summary(tmp_path).loc["publication"]
    assert (row["changed_cells"], row["format_only_cells"]) == (0, 0)


def test_execute_needs_tables(world, tmp_path):
    world.add("tool", False, [["T1", "d"]])
    world.add("tool", True, [["T1", "d"]])
    with pytest.raises(SystemExit) as exc:
        _run(tmp_path, "--execute")
    assert exc.value.code == 2 and _writes(world) == []
    with pytest.raises(SystemExit):
        _run(tmp_path, "--tables", "all", "tool", "--execute")
    assert _writes(world) == []


def test_tables_all_selects_every_resource(world, tmp_path, monkeypatch):
    monkeypatch.setattr(promote_staging, "RESOURCES", ["tool", "grant"])
    for resource in ("tool", "grant"):
        world.add(resource, False, [["X1", "a"]])
        world.add(resource, True, [["X1", "a"]])
    assert _run(tmp_path, "--tables", "all", "--execute") == 0
    assert list(_summary(tmp_path)["status"]) == ["promoted", "promoted"]
