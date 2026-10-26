"""Shared library for shared RecordSets: read, write through Grid, replay, merge.

Gate G1: each grant has one RecordSet per type (``MC2Center_<Component>_RecordSet``
in the grant's data-type folder) that the pipeline and contributors both edit.
The pipeline writes only through a Grid session (:func:`write_via_grid`), which
validates and versions the data. Every version it creates is recorded in a
:class:`WriteLog` (write log v2), together with ``baseVersion``, the latest
version the run had read and reconciled against.

Replay (:func:`replay`) rebuilds the current state deterministically from the
whole version history, tracking per cell its ``value``, ``owner`` (P pipeline,
C contributor) and ``set_at`` (version):

* Baseline [owner decision]: with nothing logged, the latest version is the
  baseline, all P (a first write). Otherwise the baseline is the last version
  before the first logged one (or empty), all P: pre-log and migration data
  belong to the pipeline.
* Pipeline version ``v`` (base ``b``): a differing cell keeps the state value if
  its owner is C and ``set_at > b`` (the pipeline overwrote an edit it never
  saw; ``kept_unseen_edit``), else takes the version's value as P. State keys
  missing from ``v`` are kept (``kept_unseen_row``); new keys are added as P.
* Foreign version ``v``: new keys are added (``kept_contributor_row``;
  non-blank cells C, blank cells P so the pipeline can fill them). A Grid
  export is the snapshot its session opened on plus the user's edits, so the
  version's snapshot is taken to be the earlier version it agrees with on the
  most cells (newest on ties; :func:`_session_base`). An exact copy of an older
  version is read as a deliberate contributor undo [owner decision], so its
  snapshot is ``v - 1``. Per cell: equal to the
  state, nothing; equal to the snapshot but not the state, the session never
  touched it and it is stale, state kept (``restored_revert``); otherwise a
  contributor edit (``kept_edit``: value, owner C, ``set_at = v``), even when
  it matches an older value (e.g. undoing a pipeline change). A state key
  missing from ``v`` is restored whatever its owner (``restored_missing``); a
  real deletion is done by the DCC.

:func:`merge_upload` then applies an upload to the replayed state: a differing
cell owned by C is kept and reported as a conflict (contributor wins), any
other cell takes the upload value.

Cell values: a list-valued cell is a Python ``list`` of strings (stored in the
RecordSet CSV as a JSON array) and only list columns are parsed; every other
cell is a string, blank is ``""``.

Comparison: a list cell is compared order-insensitively only in *set-like*
columns, the schema's array properties whose items are an enum (see
:func:`set_like_columns`); other lists compare as is. An empty list equals
``""``. Plain decimal numeric strings that parse to the same float are equal
(``2020`` == ``2020.0``); leading zeros (``007``) never count as numeric.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import shutil
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from synapseclient import operations
from synapseclient.models import Column, ColumnType, Grid, RecordSet, Table

import model_contract
import utils

logger = logging.getLogger(__name__)

KEY_SEP = "\x1f"

DECISION_COLUMNS = ["key", "column", "decision", "current_value", "pipeline_value", "version"]
CONFLICT_COLUMNS = ["key", "column", "contributor_value", "upload_value"]

WRITE_LOG_COLUMNS = [
    "recordSetId", "version", "baseVersion", "runId", "principalId", "status", "writtenOn", "rowsWritten",
]
LOCK_HOURS = 2
# Recovery compares Synapse's server-side modifiedOn with the runner's local
# writtenOn, so the two clocks may disagree; accept a version modified up to
# this long before the pending row was written.
CLOCK_TOLERANCE = pd.Timedelta(minutes=10)
_NUMERIC = re.compile(r"-?(0|[1-9]\d*)(\.\d+)?")


# --- cells -------------------------------------------------------------------


def parse_cell(value):
    """A stored CSV cell as a list (JSON array) or string; empty is ``""``."""
    if value is None or (not isinstance(value, (str, list)) and pd.isna(value)):
        return ""
    if isinstance(value, list):
        return value
    text = str(value)
    stripped = text.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            parsed = json.loads(stripped)
        except ValueError:
            return text
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    return text


def cell_to_csv(value) -> str:
    """A cell as written to the RecordSet CSV: lists as JSON arrays."""
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")) if value else ""
    return "" if value is None else str(value)


def _norm(value, set_like: bool = False):
    """Comparable form of a cell (see the module docstring)."""
    if isinstance(value, list):
        if not value:
            return ""
        return tuple(sorted(value)) if set_like else tuple(value)
    return value


def _canon(value, set_like: bool = False):
    """Hashable canonical form: ``_norm`` plus numeric strings as floats."""
    n = _norm(value, set_like)
    if isinstance(n, str) and _NUMERIC.fullmatch(n.strip()):
        return ("#", float(n))
    return n


def _cells_equal(a, b, set_like: bool) -> bool:
    return _canon(a, set_like) == _canon(b, set_like)


def key_columns(key) -> list[str]:
    return [key] if isinstance(key, str) else list(key)


def _key_of(row, key_cols: list[str]) -> str:
    return KEY_SEP.join(str(row[c]).strip() for c in key_cols)


def to_csv(df: pd.DataFrame, path) -> None:
    """Write ``df`` with JSON-array list cells, matching the stored form."""
    out = df.copy()
    for column in out.columns:
        out[column] = out[column].map(cell_to_csv)
    out.to_csv(path, index=False)


# --- schema: which columns are lists ----------------------------------------


def _array_properties(component: str) -> dict[str, dict]:
    with open(model_contract.fetch(f"json_schemas/{component}.json"), encoding="utf-8") as fh:
        properties = json.load(fh)["properties"]
    return {k: v for k, v in properties.items() if v.get("type") == "array"}


def schema_columns(component: str) -> set[str]:
    """Class-label names of every property in the component's schema.

    A Grid import keeps only these; any other column (e.g. ``Component``) is
    dropped by Synapse, so uploads leave them out before merging.
    """
    with open(model_contract.fetch(f"json_schemas/{component}.json"), encoding="utf-8") as fh:
        return set(json.load(fh)["properties"])


def list_columns(component: str) -> set[str]:
    """Class-label names of the component's list (array) columns."""
    return set(_array_properties(component))


def set_like_columns(component: str) -> set[str]:
    """Class-label names of list columns whose items are an enum (order is meaningless)."""
    return {k for k, v in _array_properties(component).items() if "enum" in v.get("items", {})}


def parse_upload(df: pd.DataFrame, list_cols) -> pd.DataFrame:
    """String-dtype upload frame with list columns parsed to lists.

    Accepts ``A, B`` and JSON-array cells; other columns stay strings.
    """
    out = df.fillna("").astype(str)
    for column in out.columns:
        if column in list_cols:
            out[column] = out[column].map(_split_list_cell)
    return out


def _split_list_cell(value: str) -> list[str]:
    parsed = parse_cell(value)
    return parsed if isinstance(parsed, list) else utils.split_cell(value)


# --- reading ----------------------------------------------------------------


DEFAULT_CACHE_DIR = Path.home() / ".cache" / "mc2-center-dcc" / "recordsets"


def _load_csv(path, list_cols=()) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    for column in df.columns:
        if column in list_cols:
            df[column] = df[column].map(parse_cell)
    return df.astype(object)


def read_recordset(
    syn, rs_id: str, version: int | None = None, cache_dir=None, list_cols=()
) -> pd.DataFrame:
    """Read a RecordSet version (default: latest) with string dtype.

    JSON-array cells become Python lists only in ``list_cols`` (see
    :func:`list_columns`); every other cell stays a string, empty is ``""``.
    With ``cache_dir`` and an explicit ``version``, the CSV is cached on disk
    as ``<cache_dir>/<rs_id>/v<version>.csv`` (versions are immutable, the
    latest included) and read from there on later calls. ``version=None`` is
    never cached.
    """
    cached = None
    if cache_dir is not None and version is not None:
        cached = Path(cache_dir) / rs_id / f"v{version}.csv"
        if cached.exists():
            return _load_csv(cached, list_cols)
    entity = RecordSet(id=rs_id, version_number=version, download_file=True).get(synapse_client=syn)
    if cached is not None:
        cached.parent.mkdir(parents=True, exist_ok=True)
        tmp = cached.with_name(f"{cached.name}.{os.getpid()}.{uuid.uuid4().hex}.part")
        shutil.copyfile(entity.path, tmp)
        tmp.replace(cached)
    return _load_csv(entity.path, list_cols)


def check_upload_keys(upload: pd.DataFrame, key) -> pd.DataFrame:
    """Upload with key cells stripped; raises ValueError on blank or duplicate keys."""
    key_cols = key_columns(key)
    out = upload.copy()
    for column in key_cols:
        out[column] = out[column].map(lambda v: str(v).strip())
    keys = [_key_of(row, key_cols) for _, row in out.iterrows()]
    blank = [i + 2 for i, (_, row) in enumerate(out.iterrows()) if any(row[c] == "" for c in key_cols)]
    seen, dupes = set(), []
    for k in keys:
        if k in seen and k not in dupes:
            dupes.append(k)
        seen.add(k)
    problems = []
    if blank:
        problems.append(f"blank key in file line(s) {blank}")
    if dupes:
        problems.append(f"duplicate keys {dupes}")
    if problems:
        raise ValueError("; ".join(problems))
    return out


def recordset_info(syn, rs_id: str) -> tuple[int, list[str]]:
    """``(latest version number, upsert key columns)``, without downloading the file."""
    entity = RecordSet(id=rs_id, download_file=False).get(synapse_client=syn)
    return entity.version_number, list(entity.upsert_keys or [])


def find_recordset(syn, folder_id: str, component: str) -> str | None:
    """ID of ``MC2Center_<component>_RecordSet`` among the folder's children, or None."""
    return operations.find_entity_id(
        name=f"MC2Center_{component}_RecordSet", parent=folder_id, synapse_client=syn
    )


# --- write log --------------------------------------------------------------


def _to_dt(value):
    """A write-log time (datetime, epoch milliseconds or ISO text) as a UTC Timestamp."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return pd.NaT
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        return pd.to_datetime(int(value), unit="ms", utc=True)
    return pd.to_datetime(value, utc=True)


def _int_or_none(value):
    return None if value is None or (not isinstance(value, str) and pd.isna(value)) else int(value)


class WriteLog:
    """Write log v2: which RecordSet versions the pipeline wrote, and which runs are in flight.

    Columns: ``recordSetId, version`` (null while ``pending``), ``baseVersion``
    (the latest version the run read and reconciled against), ``runId``,
    ``principalId`` (the runner's Synapse user ID), ``status`` (``pending``,
    ``written``, ``recovered``, or ``aborted`` when a run failed before any
    export), ``writtenOn``, ``rowsWritten``. Subclasses implement ``_read`` and
    ``_append``.
    """

    def _read(self, rs_id: str) -> pd.DataFrame:
        raise NotImplementedError

    def _append(self, row: dict) -> None:
        raise NotImplementedError

    def _row(self, rs_id, version, base, run_id, principal, status, rows) -> dict:
        return {
            "recordSetId": rs_id, "version": version, "baseVersion": base, "runId": run_id,
            "principalId": str(principal), "status": status,
            "writtenOn": datetime.now(timezone.utc), "rowsWritten": rows,
        }

    def start(self, rs_id: str, base: int, run_id: str, principal) -> None:
        """Record a ``pending`` row before any Grid session is opened."""
        self._append(self._row(rs_id, None, base, run_id, principal, "pending", None))

    def record(self, rs_id, version, run_id, rows, base=None, principal="", status="written") -> None:
        """Record a version the pipeline created (``written`` or ``recovered``).

        ``base`` defaults to ``version - 1``.
        """
        base = version - 1 if base is None and version is not None else base
        self._append(self._row(rs_id, version, base, run_id, principal, status, rows))

    def abort(self, rs_id: str, base: int, run_id: str, principal) -> None:
        """Resolve a ``pending`` row whose run failed before creating any version."""
        self._append(self._row(rs_id, None, base, run_id, principal, "aborted", None))

    def pipeline_map(self, rs_id: str) -> dict[int, int]:
        """``{version: baseVersion}`` of every pipeline version (written or recovered)."""
        df = self._read(rs_id)
        df = df[df["status"].isin(["written", "recovered"]) & df["version"].notna()]
        return {int(v): int(b) if pd.notna(b) else int(v) - 1 for v, b in zip(df["version"], df["baseVersion"])}

    def pipeline_versions(self, rs_id: str) -> list[int]:
        return sorted(self.pipeline_map(rs_id))

    def last_version(self, rs_id: str) -> int | None:
        versions = self.pipeline_versions(rs_id)
        return max(versions) if versions else None

    def unresolved_pending(self, rs_id: str) -> list[dict]:
        """``pending`` rows whose runId has no written, recovered or aborted row."""
        df = self._read(rs_id)
        done = set(df.loc[df["status"].isin(["written", "recovered", "aborted"]), "runId"])
        out = []
        for _, row in df[df["status"] == "pending"].iterrows():
            if row["runId"] not in done:
                out.append({
                    "runId": row["runId"], "baseVersion": _int_or_none(row["baseVersion"]),
                    "principalId": str(row["principalId"]), "writtenOn": _to_dt(row["writtenOn"]),
                })
        return out

    def locked_by(self, rs_id: str, run_id: str, now=None, since=None) -> str | None:
        """runId of a run with an unresolved ``pending`` row younger than 2 h, else None.

        A row of this very ``run_id`` written at or after ``since`` (this
        process's start) is our own and is ignored; an older one with the same
        runId belongs to an earlier process (a crash, or a run still going) and
        counts. Without ``since``, same-runId rows are ignored.
        """
        now = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
        for row in self.unresolved_pending(rs_id):
            if row["runId"] == run_id and (since is None or row["writtenOn"] >= since):
                continue
            if now - row["writtenOn"] < pd.Timedelta(hours=LOCK_HOURS):
                return row["runId"]
        return None

    def earlier_pending(self, rs_id: str, run_id: str, now=None) -> str | None:
        """runId of another live pending row that beats ``run_id``'s own, else None.

        Used right after ``start`` to settle a lock race: the row with the
        earlier ``writtenOn`` (ties broken by runId) keeps the lock.
        """
        now = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
        rows = self.unresolved_pending(rs_id)
        mine = [(r["writtenOn"], r["runId"]) for r in rows if r["runId"] == run_id]
        if not mine:
            return None
        mine_key = max(mine)
        others = [(r["writtenOn"], r["runId"]) for r in rows
                  if r["runId"] != run_id and now - r["writtenOn"] < pd.Timedelta(hours=LOCK_HOURS)]
        beating = [o for o in others if o < mine_key]
        return min(beating)[1] if beating else None


class SynapseTableWriteLog(WriteLog):
    """Write log kept in a Synapse table (see :func:`create_write_log_table`)."""

    def __init__(self, syn, table_id: str):
        self.syn = syn
        self.table_id = table_id

    def _read(self, rs_id: str) -> pd.DataFrame:
        return Table(id=self.table_id).query(
            query=f"SELECT * FROM {self.table_id} WHERE recordSetId = '{rs_id}'",
            include_row_id_and_row_version=False,
            synapse_client=self.syn,
        )

    def _append(self, row: dict) -> None:
        values = dict(row)
        values["writtenOn"] = int(values["writtenOn"].timestamp() * 1000)
        frame = pd.DataFrame([values], columns=WRITE_LOG_COLUMNS)
        for column in ("version", "baseVersion", "rowsWritten"):
            frame[column] = frame[column].astype("Int64")
        Table(id=self.table_id).store_rows(values=frame, synapse_client=self.syn)


class CsvWriteLog(WriteLog):
    """The same log as a local CSV, for tests and dry runs."""

    def __init__(self, path):
        self.path = Path(path)

    def _read(self, rs_id: str) -> pd.DataFrame:
        if not self.path.exists():
            return pd.DataFrame(columns=WRITE_LOG_COLUMNS)
        df = pd.read_csv(self.path, dtype=str, keep_default_na=False)
        df = df[df["recordSetId"] == rs_id].copy()
        for column in ("version", "baseVersion", "rowsWritten"):
            df[column] = pd.to_numeric(df[column].replace("", pd.NA))
        return df

    def _append(self, row: dict) -> None:
        new = not self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if new:
                writer.writerow(WRITE_LOG_COLUMNS)
            writer.writerow(
                ["" if row[c] is None else (row[c].isoformat() if c == "writtenOn" else row[c])
                 for c in WRITE_LOG_COLUMNS]
            )


def create_write_log_table(syn, parent_id: str, name: str = "MC2Center_RecordSet_WriteLog") -> str:
    """Create the write-log table under ``parent_id`` and return its ID.

    A Synapse write. Not called by any script; a migration step runs it once.
    """
    columns = [
        Column(name="recordSetId", column_type=ColumnType.ENTITYID),
        Column(name="version", column_type=ColumnType.INTEGER),
        Column(name="baseVersion", column_type=ColumnType.INTEGER),
        Column(name="runId", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="principalId", column_type=ColumnType.STRING, maximum_size=20),
        Column(name="status", column_type=ColumnType.STRING, maximum_size=20),
        Column(name="writtenOn", column_type=ColumnType.DATE),
        Column(name="rowsWritten", column_type=ColumnType.INTEGER),
    ]
    table = Table(name=name, parent_id=parent_id, columns=columns).store(synapse_client=syn)
    return table.id


def recordset_version_meta(syn, rs_id: str, version: int) -> tuple[str, pd.Timestamp]:
    """``(modifiedBy, modifiedOn)`` of one RecordSet version, without downloading it."""
    entity = RecordSet(id=rs_id, version_number=version, download_file=False).get(synapse_client=syn)
    return str(entity.modified_by), _to_dt(entity.modified_on)


def recover_pending(syn, rs_id, write_log, latest, meta=None, record=True) -> dict[int, int]:
    """Recover versions created by runs that died after exporting.

    For each unresolved ``pending`` row, a version above its ``baseVersion``
    that is not logged, was modified by the row's ``principalId`` and not
    earlier than ``writtenOn - CLOCK_TOLERANCE`` is a pipeline version with
    that base: recorded as ``recovered`` (when ``record``) and returned as
    ``{version: base}``. A pending row with nothing to recover is resolved as
    ``aborted``. Call it before this run's own ``start``; a crashed export of
    the same runId is recovered like any other.
    """
    meta = meta or (lambda v: recordset_version_meta(syn, rs_id, v))
    logged = write_log.pipeline_map(rs_id)
    found: dict[int, int] = {}
    for row in write_log.unresolved_pending(rs_id):
        if row["baseVersion"] is None:
            continue
        mine = []
        for v in range(row["baseVersion"] + 1, latest + 1):
            if v in logged or v in found:
                continue
            by, on = meta(v)
            if by == row["principalId"] and pd.notna(on) and on >= row["writtenOn"] - CLOCK_TOLERANCE:
                mine.append(v)
        for v in mine:
            found[v] = row["baseVersion"]
            if record:
                write_log.record(rs_id, v, row["runId"], None, base=row["baseVersion"],
                                 principal=row["principalId"], status="recovered")
        if not mine and record:
            write_log.abort(rs_id, row["baseVersion"], row["runId"], row["principalId"])
        if mine:
            logger.warning("%s: run %s died after its pending row; recovered versions %s",
                           rs_id, row["runId"], sorted(mine))
        else:
            logger.info("%s: run %s left a pending row and created no version", rs_id, row["runId"])
    return found


# --- replay / merge ----------------------------------------------------------

P, C = "P", "C"


def _frames_by_key(df: pd.DataFrame, key_cols: list[str], columns: list[str]) -> dict[str, dict]:
    """key string -> {column: value}, every frame reindexed to the same columns."""
    out = {}
    for _, row in df.reindex(columns=columns, fill_value="").iterrows():
        out[_key_of(row, key_cols)] = {c: row[c] for c in columns}
    return out


def _all_columns(frames, key_cols) -> list[str]:
    columns = list(key_cols)
    for df in frames:
        columns += [c for c in df.columns if c not in columns]
    return columns


def _is_blank(value) -> bool:
    return _norm(value) == ""


def _session_base(frames, v, non_key, set_like) -> int:
    """The earlier version that version ``v`` was most likely exported from.

    The one agreeing with ``v`` on the most cells (newest on ties). A fresh
    session gives ``v - 1``; a stale session gives the older version it was
    opened on.
    """
    target = frames[v]
    best, best_score = v - 1, -1
    for u in sorted((u for u in frames if u < v), reverse=True):
        other = frames[u]
        score = sum(
            1
            for k in target.keys() & other.keys()
            for c in non_key
            if _cells_equal(target[k][c], other[k][c], c in set_like)
        )
        if score > best_score:
            best, best_score = u, score
    # An exact copy of an older version is either an untouched stale session or
    # a contributor undoing the changes since; the data can't tell them apart.
    # Owner decision (2026-10-26): read it as a deliberate contributor undo.
    exact = target.keys() == frames[best].keys() and best_score == len(target) * len(non_key)
    if exact and best != v - 1:
        return v - 1
    return best


def replay(versions, log_map, key, set_like=()):
    """Rebuild the current state from every version, tracking cell ownership.

    ``versions`` is ``[(version, DataFrame)]`` for every version ``1..latest``;
    ``log_map`` is ``{pipeline version: baseVersion}`` from the write log. The
    rules are in the module docstring. Returns ``(state, owned, decisions)``:
    the state DataFrame, the set of ``(key, column)`` cells owned by a
    contributor, and the decisions (``key, column, decision, current_value,
    pipeline_value, version``) for versions newer than the newest logged one,
    where ``current_value`` is the value in that version and
    ``pipeline_value`` the state's value before it.
    """
    key_cols = key_columns(key)
    set_like = set(set_like)
    versions = sorted(versions, key=lambda t: t[0])
    columns = _all_columns([df for _, df in versions], key_cols)
    frames = {v: _frames_by_key(df, key_cols, columns) for v, df in versions}
    latest = versions[-1][0]
    non_key = [c for c in columns if c not in key_cols]

    decisions: list[dict] = []
    state: dict[str, dict] = {}  # key -> {column: [value, owner, set_at]}

    def fresh(row, owner_of, v):
        return {c: [row[c], owner_of(row[c]), v] for c in columns}

    if not log_map:
        state = {k: fresh(r, lambda _: P, latest) for k, r in frames[latest].items()}
        first, report_after = latest + 1, latest
    else:
        first, report_after = min(log_map), max(log_map)
        if first - 1 in frames:
            state = {k: fresh(r, lambda _: P, first - 1) for k, r in frames[first - 1].items()}

    def decide(v, k, column, decision, cur="", pipe=""):
        if v > report_after:
            decisions.append({"key": k, "column": column, "decision": decision,
                              "current_value": cur, "pipeline_value": pipe, "version": v})

    for v in sorted(frames):
        if v < first:
            continue
        frame = frames[v]
        if v in log_map:
            base = log_map[v]
            for k, row in frame.items():
                if k not in state:
                    state[k] = fresh(row, lambda _: P, v)
                    continue
                for c in non_key:
                    cell = state[k][c]
                    if _cells_equal(row[c], cell[0], c in set_like):
                        continue
                    if cell[1] == C and cell[2] > base:
                        decide(v, k, c, "kept_unseen_edit", row[c], cell[0])
                    else:
                        state[k][c] = [row[c], P, v]
            for k in state:
                if k not in frame:
                    decide(v, k, "", "kept_unseen_row")
        else:
            # A Grid export is the snapshot its session opened on plus the
            # user's edits, so a cell that still equals that snapshot was not
            # touched: if it differs from the state, it is stale. Only cells
            # that differ from the snapshot are contributor edits, even when
            # they match an older value (e.g. undoing a pipeline change).
            snapshot = frames[_session_base(frames, v, non_key, set_like)] if v > 1 else {}
            for k, row in frame.items():
                if k not in state:
                    state[k] = fresh(row, lambda x: P if _is_blank(x) else C, v)
                    decide(v, k, "", "kept_contributor_row")
                    continue
                for c in non_key:
                    cell = state[k][c]
                    if _cells_equal(row[c], cell[0], c in set_like):
                        continue
                    if k in snapshot and _cells_equal(row[c], snapshot[k][c], c in set_like):
                        decide(v, k, c, "restored_revert", row[c], cell[0])
                    else:
                        decide(v, k, c, "kept_edit", row[c], cell[0])
                        state[k][c] = [row[c], C, v]
            for k in state:
                if k not in frame:
                    decide(v, k, "", "restored_missing")

    rows = [{c: cells[c][0] for c in columns} for cells in state.values()]
    owned = {(k, c) for k, cells in state.items() for c in non_key if cells[c][1] == C}
    return (
        pd.DataFrame(rows, columns=columns),
        owned,
        pd.DataFrame(decisions, columns=DECISION_COLUMNS),
    )


def merge_upload(reconciled, upload, key, edited, set_like=()):
    """Upsert ``upload`` rows into ``reconciled`` (a replayed state); contributors win.

    Existing keys are updated cell by cell, new keys appended, keys absent from
    the upload left alone. For a cell in ``edited`` (``replay``'s contributor-owned
    cells) where the upload value differs, the state value is kept
    and a conflict recorded. Returns ``(merged DataFrame, conflicts DataFrame)``
    with columns ``key, column, contributor_value, upload_value``.
    """
    key_cols = key_columns(key)
    set_like = set(set_like)
    columns = _all_columns([reconciled, upload], key_cols)
    merged = _frames_by_key(reconciled, key_cols, columns)
    conflicts = []
    for k, up_row in _frames_by_key(upload, key_cols, columns).items():
        if k not in merged:
            row = {c: "" for c in columns}
            row.update({c: up_row[c] for c in upload.columns})
            merged[k] = row
            continue
        row = merged[k]
        for column in upload.columns:
            if column in key_cols:
                continue
            new, old = up_row[column], row[column]
            if _cells_equal(new, old, column in set_like):
                continue
            if (k, column) in edited:
                conflicts.append(
                    {"key": k, "column": column, "contributor_value": old, "upload_value": new}
                )
            else:
                row[column] = new
    return (
        pd.DataFrame(list(merged.values()), columns=columns),
        pd.DataFrame(conflicts, columns=CONFLICT_COLUMNS),
    )


def differences(expected: pd.DataFrame, actual: pd.DataFrame, key, set_like=()) -> list[str]:
    """Human-readable differences between two frames (empty when equal).

    Row order does not matter; cells compare as in :func:`reconcile`.
    """
    key_cols = key_columns(key)
    set_like = set(set_like)
    columns = _all_columns([expected, actual], key_cols)
    exp = _frames_by_key(expected, key_cols, columns)
    act = _frames_by_key(actual, key_cols, columns)
    out = [f"missing key {k}" for k in exp if k not in act]
    out += [f"unexpected key {k}" for k in act if k not in exp]
    for k in exp.keys() & act.keys():
        for column in columns:
            if not _cells_equal(exp[k][column], act[k][column], column in set_like):
                out.append(f"{k} / {column}: expected {exp[k][column]!r}, found {act[k][column]!r}")
    return out


# --- writing -----------------------------------------------------------------


def write_via_grid(
    syn,
    rs_id: str,
    csv_path,
    timeout: int = 120,
    validation_wait: int = 60,
    retry_wait: int = 30,
    max_exports: int = 4,
    on_version=None,
) -> list[int]:
    """Upsert ``csv_path`` into the RecordSet through a Grid session; return every version created.

    Fresh session, import, wait for validation, export (a new RecordSet
    version), delete the session. The grid session exposes no validation
    statistics before an export (only the export response carries a summary),
    so the wait is a fixed ``validation_wait`` seconds after the import; the
    export summary's unknown-row count is then checked, and while rows are
    still unknown the session waits ``retry_wait`` seconds and exports again
    (at most ``max_exports`` exports; each creates a version with the same
    data). ``on_version(v)`` is called right after each export, so the caller
    can log it before anything else can fail. A failing ``grid.delete`` is
    logged and never masks the result or the original exception.
    """
    grid = Grid(record_set_id=rs_id).create(
        attach_to_previous_session=False, timeout=timeout, synapse_client=syn
    )
    try:
        grid.import_csv(path=str(csv_path), timeout=timeout, synapse_client=syn)
        wait, versions = validation_wait, []
        for attempt in range(1, max_exports + 1):
            time.sleep(wait)
            grid.export_to_record_set(timeout=timeout, synapse_client=syn)
            versions.append(grid.record_set_version_number)
            if on_version is not None:
                on_version(grid.record_set_version_number)
            stats = grid.validation_summary_statistics
            unknown = getattr(stats, "number_of_unknown_children", 0) if stats else 0
            if not unknown:
                break
            logger.warning(
                "%s v%s: %s rows not yet validated (export %d of %d)",
                rs_id, grid.record_set_version_number, unknown, attempt, max_exports,
            )
            wait = retry_wait
        else:
            logger.warning("%s: rows still unvalidated after %d exports", rs_id, max_exports)
        return versions
    finally:
        try:
            grid.delete(synapse_client=syn)
        except Exception:
            logger.exception("%s: could not delete grid session %s", rs_id, getattr(grid, "session_id", ""))


def get_validation(syn, rs_id: str, version: int, download_location, keys: list[str]):
    """Detailed validation results of a version, with ``row_index`` mapped to the key.

    ``keys`` are the key strings in stored-row order. Returns a DataFrame with
    ``key`` first, or None when the version has no validation results.
    """
    entity = RecordSet(id=rs_id, version_number=version, download_file=False).get(synapse_client=syn)
    results = entity.get_detailed_validation_results(
        download_location=str(download_location), synapse_client=syn
    )
    if results is None:
        return None
    results = results.copy()
    results.insert(0, "key", results["row_index"].map(lambda i: keys[int(i)] if int(i) < len(keys) else ""))
    return results
