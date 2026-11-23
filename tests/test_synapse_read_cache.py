"""The read cache of SynapseTableWriteLog and SynapseTableRemovals, with a counting fake Synapse table."""
import re

import pandas as pd
import pytest

import recordsets as rs
from test_upload_recordsets import FakeSynapse, run, stored


class CountingTables:
    """A fake ``Table`` class: tables kept in memory by id, with every ``query`` counted."""

    def __init__(self):
        self.rows = {}  # table id -> list of row dicts
        self.queries = []  # (table id, recordSetId or None)

    def __call__(self, id=None):
        tables = self
        table_id = id

        class Handle:
            def query(self, query=None, include_row_id_and_row_version=False, synapse_client=None):
                match = re.search(r"recordSetId = '([^']*)'", query)
                tables.queries.append((table_id, match.group(1) if match else None))
                rows = tables.rows.get(table_id, [])
                columns = rs.WRITE_LOG_COLUMNS if table_id == "synLOG" else rs.REMOVALS_COLUMNS
                if match is None:
                    return pd.DataFrame({"recordSetId": sorted({r["recordSetId"] for r in rows})})
                return pd.DataFrame([r for r in rows if r["recordSetId"] == match.group(1)], columns=columns)

            def store_rows(self, values, synapse_client=None):
                tables.rows.setdefault(table_id, []).extend(values.astype(object).to_dict("records"))

        return Handle()

    def count(self, table_id):
        return sum(1 for t, _ in self.queries if t == table_id)

    def append_directly(self, table_id, row):
        """Another process appends a row: not through the object under test."""
        self.rows.setdefault(table_id, []).append(row)


@pytest.fixture
def tables(monkeypatch):
    fake = CountingTables()
    monkeypatch.setattr(rs, "Table", fake)
    return fake


def other_pending(run_id, when):
    return {"recordSetId": "synA", "version": None, "baseVersion": 1, "runId": run_id, "principalId": "7",
            "status": "pending", "writtenOn": int(when.timestamp() * 1000), "rowsWritten": None}


def test_history_reads_share_one_query_and_an_append_drops_it(tables):
    log = rs.SynapseTableWriteLog("syn", "synLOG")
    log.verified_versions("synA")
    log.pipeline_map("synA")
    assert tables.count("synLOG") == 1
    log.start("synA", 1, "r1", "42")  # an append drops that RecordSet's entry
    assert log.pipeline_map("synA") == {}
    assert tables.count("synLOG") == 2
    log.pipeline_map("synA")
    log.pipeline_map("synB")  # another RecordSet has its own entry
    assert tables.count("synLOG") == 3


def test_a_history_read_returns_a_copy(tables):
    log = rs.SynapseTableWriteLog("syn", "synLOG")
    log.start("synA", 1, "r1", "42")
    frame = log._read("synA")
    frame.drop(frame.index, inplace=True)
    assert len(log._read("synA")) == 1


def test_lock_checks_always_query_and_see_another_process(tables):
    log = rs.SynapseTableWriteLog("syn", "synLOG")
    log.pipeline_map("synA")  # fills the cache
    before = tables.count("synLOG")
    assert log.unresolved_pending("synA") == []
    assert log.locked_by("synA", "me") is None
    assert log.earlier_pending("synA", "me") is None
    assert tables.count("synLOG") == before + 3
    # another run appends straight to the table; the cache does not hide it
    log.pipeline_map("synA")
    tables.append_directly("synLOG", other_pending("other", pd.Timestamp.now(tz="UTC")))
    assert log.locked_by("synA", "me") == "other"
    assert [r["runId"] for r in log.unresolved_pending("synA")] == ["other"]
    log.start("synA", 1, "me", "42")
    assert log.earlier_pending("synA", "me") in (None, "other")  # a query each time, no error
    log.pipeline_map("synA")
    tables.append_directly("synLOG", other_pending("later", pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=1)))
    assert log.earlier_pending("synA", "me") == "later"


def test_a_fresh_read_refreshes_the_history_cache(tables):
    log = rs.SynapseTableWriteLog("syn", "synLOG")
    assert log.pipeline_map("synA") == {}
    tables.append_directly("synLOG", {"recordSetId": "synA", "version": 2, "baseVersion": 1, "runId": "x",
                                      "principalId": "7", "status": "written", "writtenOn": 0, "rowsWritten": 1})
    assert log.pipeline_map("synA") == {}  # cached, as designed
    log.unresolved_pending("synA")  # a lock check reads the table and replaces the entry
    assert log.pipeline_map("synA") == {2: 1}


def test_removals_cache_is_per_recordset(tables):
    removals = rs.SynapseTableRemovals("syn", "synREM")
    removals.append_many("synA", [{"key": "k", "status": "requested"}], "r", "42")
    removals.append_many("synB", [{"key": "k", "status": "requested"}], "r", "42")
    removals.requested("synA")
    removals.requested("synA")
    removals.requested("synB")
    assert tables.queries == [("synREM", "synA"), ("synREM", "synB")]
    removals.append_many("synA", [{"key": "j", "status": "requested"}], "r", "42")
    assert set(removals.requested("synA")) == {"k", "j"}
    removals.requested("synB")
    assert tables.queries[2:] == [("synREM", "synA")]  # A queried again, B still cached
    removals.recordset_ids()
    removals.recordset_ids()
    assert tables.queries[3:] == [("synREM", None), ("synREM", None)]  # never cached


def test_upload_run_makes_fewer_write_log_queries(tmp_path, tables, monkeypatch):
    def execute_run(root, run_id):
        root.mkdir()
        fake = FakeSynapse([stored(["p1", "Old", ["RNA Sequencing"]], ["p9", "Other", ""])])
        tables.queries.clear()
        code, _ = run(root, fake, run_id=run_id)
        assert code == 0
        return [r["status"] for r in tables.rows["synLOG"]], tables.count("synLOG"), tables.count("synREM")

    # the same run, cache off: every history read queries
    with monkeypatch.context() as m:
        m.setattr(rs.SynapseTableWriteLog, "_read", lambda self, rs_id: self._read_fresh(rs_id))
        m.setattr(rs.SynapseTableRemovals, "_read", lambda self, rs_id: rs.Table(id=self.table_id).query(
            query=f"SELECT * FROM {self.table_id} WHERE recordSetId = '{rs_id}'", synapse_client=self.syn))
        writes_before, log_before, rem_before = execute_run(tmp_path / "before", "r1")
    tables.rows.clear()
    writes_after, log_after, rem_after = execute_run(tmp_path / "after", "r1")
    assert log_after < log_before and rem_after <= rem_before
    assert writes_before == writes_after == ["pending", "written", "done"]  # the same writes


def test_recover_pending_reads_fresh_before_it_uses_the_history(tables):
    log = rs.SynapseTableWriteLog("syn", "synLOG")
    assert log.pipeline_map("synA") == {}  # a stale history is now cached
    row = dict(other_pending("rX", pd.Timestamp.now(tz="UTC")), version=2, status="written")
    tables.append_directly("synLOG", row)  # another process logs version 2
    seen, history = [], log.pipeline_map
    log.pipeline_map = lambda rs_id: seen.append(history(rs_id)) or seen[-1]
    rs.recover_pending("syn", "synA", log, 2, meta=lambda v: ("42", pd.Timestamp.now(tz="UTC")))
    assert 2 in seen[0]  # recovery decides from the fresh history, not the cached one
