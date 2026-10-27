"""A foreign version landing between a removal's version check and its store (D-Q49)."""
import recordsets as rs
from test_removals import K, cell, df
from test_remove_recordset_rows import V1, cmd, process, setup


def test_foreign_version_between_check_and_store_survives_replay():
    v1 = df(["A", "T", ""], ["R", "T", ""])
    v2 = df(["A", "Edited", ""], ["R", "T", ""], ["X", "New", ""])  # foreign, after the removal's check
    v3 = df(["A", "T", ""])  # the removal's store, built from v1: overwrote A's edit and X
    v4 = df(["A", "T", ""])  # the export after it
    frames = list(enumerate([v1, v2, v3, v4], start=1))
    state, owned, _ = rs.replay(frames, {1: 0, 3: 1, 4: 1}, K, (), {3})  # base 1: the removal's, and its export's
    assert set(state[K]) == {"A", "X"}  # R dropped; the contributor's row kept
    assert cell(state, "A", "Title") == "Edited" and ("A", "Title") in owned
    assert cell(state, "X", "Title") == "New"
    # with v - 1 as the base (the old rule) X would be dropped as well
    state, _, _ = rs.replay(frames, {1: 0, 3: 2, 4: 3}, K, (), {3})
    assert set(state[K]) == {"A"}


def test_removed_keys_use_the_removals_base():
    v1 = df(["A", "T", ""], ["R", "T", ""])
    v2 = df(["A", "T", ""], ["R", "T", ""], ["X", "T", ""])
    v3 = df(["A", "T", ""])
    assert rs.removed_keys(list(enumerate([v1, v2, v3], start=1)), {3: 1}, K) == {"R": 3}


def test_a_concurrent_export_racing_the_store_is_warned_about(tmp_path, caplog):
    fake, log, removals = setup(tmp_path, [V1])
    orig = fake.store

    def racing_store(syn, rs_id, path):
        fake.versions.append(V1.copy())  # a foreign version lands between the check and the store
        return orig(syn, rs_id, path)

    fake.store = racing_store
    with caplog.at_level("WARNING"):
        s = process(tmp_path, fake, log, removals)
    assert s["status"] == "removed" and s["version"] == 3 and s["raced"]
    assert "raced it" in caplog.text
    assert log.pipeline_map("synRS")[3] == 1  # the removal's base is the version it read
    assert log.pipeline_map("synRS")[4] == 1  # its export keeps that base, so replay keeps the foreign edits
    assert log.unexported_removals("synRS") == []
