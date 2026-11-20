"""Stand-ins shared by the test files: RecordSets, Synapse clients, Tables, the write log and removals."""
import io
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from synapseclient.core.constants import concrete_types

import recordsets as rs


# --- write log and removals --------------------------------------------------------------------------------------
# These two functions are the only place the tests name the CSV-backed classes.


def write_log(path):
    return rs.CsvWriteLog(path)


def removals_table(path):
    return rs.CsvRemovals(path)


def assert_nothing_written(fake, log=None):
    """The RecordSet is still at one version with no store or export; with ``log``, that log file was never made."""
    assert len(fake.versions) == 1 and fake.events == []
    if log is not None:
        assert not log.path.exists()


# --- RecordSets ---------------------------------------------------------------------------------------------------


class FakeRS:
    """One RecordSet. ``versions`` holds CSV text or DataFrames (the kind of the last one decides what a store appends).

    A store or export appends a version and is recorded in ``events``; a store also records the write-log statuses
    at that moment, so a test can see the lock row was written first. Every method takes (syn, rs_id) like the
    recordsets function it stands in for.
    """

    def __init__(self, versions, key_cols=(), sessions=(), log=None, list_cols=(), invalid=0):
        self.versions = list(versions)
        self.key_cols = list(key_cols)
        self.sessions = list(sessions)
        self.log = log
        self.list_cols = list_cols
        self.invalid = invalid  # the first ``invalid`` keys fail validation
        self.events = []
        self.writes = []

    def _text(self):
        return isinstance(self.versions[-1], str)

    def info(self, syn, rs_id):
        return len(self.versions), self.key_cols

    def download(self, syn, rs_id, version, dest):
        dest.write_text(self.versions[version - 1], encoding="utf-8")
        return dest

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        version = self.versions[(version or len(self.versions)) - 1]
        return rs._load_csv(io.StringIO(version), list_cols) if isinstance(version, str) else version.copy()

    def store(self, syn, rs_id, path):
        self.events.append(("store", list(self.log._read(rs_id)["status"]) if self.log is not None else None))
        self.versions.append(Path(path).read_text(encoding="utf-8") if self._text() else rs._load_csv(path))
        return len(self.versions)

    def export(self, syn, rs_id, on_version=None, **kw):
        self.events.append(("export", None))
        last = self.versions[-1]
        self.versions.append(last if isinstance(last, str) else last.copy())
        if on_version:
            on_version(len(self.versions))
        return [len(self.versions)]

    def write(self, syn, rs_id, csv_path, on_version=None, **kw):
        written = rs._load_csv(csv_path, self.list_cols)
        self.writes.append(written)
        self.versions.append(written)
        if on_version:
            on_version(len(self.versions))
        return [len(self.versions)]

    def open_sessions(self, syn, rs_id):
        return list(self.sessions)

    def meta(self, syn, rs_id, version):
        return "42", pd.Timestamp.now(tz="UTC")

    def validation(self, syn, rs_id, version, loc, keys):
        return pd.DataFrame({"key": keys, "row_index": range(len(keys)),
                             "is_valid": [i >= self.invalid for i in range(len(keys))],
                             "validation_error_message": [""] * len(keys),
                             "all_validation_messages": [""] * len(keys)})


class FakeSynapse:
    """Routes the stubbed recordsets calls to a FakeRS per RecordSet ID (``rs``: ID -> FakeRS)."""

    def __init__(self, log=None, **by_id):
        self.rs = by_id
        self.log = log
        self.writes = []  # every written frame, in order, over all RecordSets
        for f in by_id.values():
            f.log = f.log if f.log is not None else log

    def info(self, syn, rs_id):
        return self.rs[rs_id].info(syn, rs_id)

    def download(self, syn, rs_id, version, dest):
        return self.rs[rs_id].download(syn, rs_id, version, dest)

    def read(self, syn, rs_id, version=None, cache_dir=None, list_cols=()):
        return self.rs[rs_id].read(syn, rs_id, version, cache_dir, list_cols)

    def store(self, syn, rs_id, path):
        return self.rs[rs_id].store(syn, rs_id, path)

    def export(self, syn, rs_id, on_version=None, **kw):
        return self.rs[rs_id].export(syn, rs_id, on_version, **kw)

    def write(self, syn, rs_id, csv_path, on_version=None, **kw):
        created = self.rs[rs_id].write(syn, rs_id, csv_path, on_version, **kw)
        self.writes.append(self.rs[rs_id].writes[-1])
        return created

    def open_sessions(self, syn, rs_id):
        return self.rs[rs_id].open_sessions(syn, rs_id)

    def meta(self, syn, rs_id, version):
        return self.rs[rs_id].meta(syn, rs_id, version)

    def validation(self, syn, rs_id, version, loc, keys):
        return self.rs[rs_id].validation(syn, rs_id, version, loc, keys)


class FakeProjectSynapse(FakeSynapse):
    """The projects' folders and RecordSets the migration scripts create, plus the RecordSets already there.

    ``key`` is the key column of every RecordSet and ``list_cols`` the columns a written CSV holds as lists.
    Each test file adds its own source tables on top of this.
    """

    def __init__(self, key, list_cols, existing=None):
        super().__init__()
        self.key = key
        self.list_cols = list_cols
        for rs_id, versions in (existing or {}).items():
            self.add(rs_id, versions)
        self.folders = {}        # (project, name) -> folder id
        self.rs_folder = {}      # folder id -> recordset id
        self.created = []        # kwargs of every create_record_based_metadata_task call
        self.created_folders = []
        self.next_id = 100

    def add(self, rs_id, versions):
        self.rs[rs_id] = FakeRS(versions, [self.key], list_cols=self.list_cols)

    def find_entity_id(self, name, parent, synapse_client=None):
        return self.folders.get((parent, name))

    def Folder(self, name, parent_id):
        outer = self

        class _F:
            def store(self, synapse_client=None):
                outer.next_id += 1
                fid = f"synF{outer.next_id}"
                outer.folders[(parent_id, name)] = fid
                outer.created_folders.append((parent_id, name))
                return SimpleNamespace(id=fid)

        return _F()

    def find_recordset(self, syn, folder, component):
        return self.rs_folder.get(folder)

    def create_task(self, **kw):
        self.created.append(kw)
        self.next_id += 1
        rs_id = f"synRS{self.next_id}"
        self.add(rs_id, [pd.DataFrame(columns=[self.key])])
        self.rs_folder[kw["folder_id"]] = rs_id
        return SimpleNamespace(id=rs_id), SimpleNamespace(task_id=1)


# --- other Synapse stand-ins ---------------------------------------------------------------------------------------


class FakeRestSyn:
    """A client whose restGET answers with one concreteType (a RecordSet by default); ``calls`` holds the URIs asked."""

    def __init__(self, concrete=concrete_types.RECORD_SET_ENTITY):
        self.concrete = concrete
        self.calls = []

    def restGET(self, uri):
        self.calls.append(uri)
        return {"id": "syn1", "concreteType": self.concrete}


def make_stub_table(frame=None, column_types=None):
    """A fresh Table class to monkeypatch in: ``query`` returns ``frame``, ``get`` reports ``column_types``,
    and ``stored`` collects the frame of every ``store_rows`` call."""

    class StubTable:
        stored = []

        def __init__(self, id=None):
            self.id = id

        def query(self, query=None, synapse_client=None):
            return frame.copy()

        def get(self, include_columns=True, synapse_client=None):
            cols = {n: SimpleNamespace(column_type=t) for n, t in (column_types or {}).items()}
            return SimpleNamespace(columns=cols)

        def store_rows(self, values, synapse_client=None):
            StubTable.stored.append(values)

    return StubTable
