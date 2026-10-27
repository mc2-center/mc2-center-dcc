"""Shared library for shared RecordSets: read, write through Grid, replay, merge.

Gate G1: each grant has one RecordSet per type (``MC2Center_<Component>_RecordSet``
in the grant's data-type folder) that the pipeline and contributors both edit.
The pipeline writes only through a Grid session (:func:`write_via_grid`), which
validates and versions the data; the one exception is a DCC row removal
(D-Q49, below). Every version it creates is recorded in a
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
  export is the snapshot its session opened on plus the user's edits. The
  snapshot is ``v - 1`` (a fresh session) unless the export is a *complete
  reversion* of an older version ``u``: every cell that differs between ``u``
  and ``v - 1`` has ``u``'s value in ``v`` (rows added or deleted since ``u``
  count cell by cell), something changed since ``u``, and ``v`` is not an
  exact copy of an older version. Then the session was stale and its snapshot
  is the newest such ``u`` (:func:`_session_base`, computed incrementally from
  per-step diffs). Ambiguous cases go to the contributor [owner decision]: a
  partial reversion, or an exact copy of an older version, is a deliberate
  undo, snapshot ``v - 1``. Note a fresh session that reverts *everything* the
  pipeline changed is indistinguishable from a stale one and is treated as
  stale. Per cell: equal to the
  state, nothing; equal to the snapshot but not the state, the session never
  touched it and it is stale, state kept (``restored_revert``); otherwise a
  contributor edit (``kept_edit``: value, owner C, ``set_at = v``), even when
  it matches an older value (e.g. undoing a pipeline change). A state key
  missing from ``v`` is restored whatever its owner (``restored_missing``); a
  real deletion is a DCC removal (D-Q49).
* Removal version (D-Q49) [owner decision]: a Grid import is an upsert and
  cannot remove a row, so real deletions are the one write that is not a Grid
  session. The DCC removal step (``annotations/remove_recordset_rows.py``)
  stores a new version without the rows (:func:`store_new_version`; binding,
  upsert key and CurationTask are kept), logs it ``removed``, then exports a
  fresh Grid session so the data is re-validated (logged ``written``). The
  keys go in the removals table (:class:`Removals`) with ``removedInVersion``.
  At a removal version, which is a pipeline version, the keys it dropped (in
  ``v - 1``, not in ``v``: exact, because the write is checked to be on the
  latest version) are first dropped from the state (``removed``), then the
  version is processed as any pipeline version. Replay reads this off the
  version history and the write log's ``removed`` rows alone, never the
  removals table, so a crash between the store and the table can't bring a row
  back. A removed key that reappears is remembered: in a pipeline
  version it is added as P (``readded_by_pipeline``, should not happen, so reported on every replay
  unless the key was ``restored``); in a
  foreign version it is added back as a contributor row
  (``readded_removed_key``, as ``kept_contributor_row``, except that when the
  session's snapshot had the row only the cells that differ from it are
  contributor-owned) and reported, never undone automatically. A contributor's
  Grid session opened before the removal can do this; the removal step's
  session check sees only the runner's visible sessions (best effort).
  The pipeline skips uploading rows whose key is currently ``removed``.

:func:`merge_upload` then applies an upload to the replayed state: a differing
cell owned by C is kept and reported as a conflict (contributor wins), any
other cell takes the upload value.

Cell values: a list-valued cell is a Python ``list`` of strings (stored in the
RecordSet CSV as a JSON array) and only list columns are parsed; every other
cell is a string, blank is ``""``.

Comparison: a list cell is compared order-insensitively only in *set-like*
columns, the schema's array properties whose items are an enum (see
:func:`set_like_columns`); other lists compare as is. An empty list equals
``""``. Plain numbers compare exactly as ``Decimal`` (``2020`` == ``2020.0``;
a 20-digit string never equals a different one; no float); leading zeros
(``007``) and exponents never count as numeric.
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
from decimal import Decimal
from pathlib import Path

import pandas as pd
from synapseclient import operations
from synapseclient.models import Column, ColumnType, Grid, RecordSet, Table

import comma_terms
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
REMOVAL_RUN_PREFIX = "dcc-removal-"  # runId of a DCC removal run (D-Q49); recover_pending keys on it
# Recovery compares Synapse's server-side modifiedOn with the runner's local
# writtenOn, so the two clocks may disagree; accept a version modified up to
# this long before the pending row was written.
CLOCK_TOLERANCE = pd.Timedelta(minutes=10)
_NUMERIC = re.compile(r"-?(0|[1-9]\d*)(\.\d+)?")
_NUMBER_START = frozenset("-0123456789")
_ABSENT = ("<absent>",)  # a cell of a row that a version does not have


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
    """Hashable canonical form: ``_norm`` plus plain numbers as exact ``Decimal``.

    ``2020`` and ``2020.0`` are equal; integers and decimals are never
    converted to float, so long digit strings stay distinct.
    """
    n = _norm(value, set_like)
    if isinstance(n, str):
        text = n.strip()
        if text and text[0] in _NUMBER_START and _NUMERIC.fullmatch(text):
            return ("#", Decimal(text))
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
            out[column] = out[column].map(lambda v: _split_list_cell(v, column))
    return out


def learn_comma_terms(parsed: pd.DataFrame, component: str, path=None) -> list[str]:
    """Add comma-containing items of ``parsed`` keyword columns to comma_terms.csv; the terms added."""
    items = [i for c in parsed.columns if comma_terms.is_keyword_column(c) for v in parsed[c] if isinstance(v, list) for i in v]
    return comma_terms.learn(items, f"upload {component}", path)


def _split_list_cell(value: str, column: str = "") -> list[str]:
    parsed = parse_cell(value)
    if isinstance(parsed, list):
        return parsed
    if comma_terms.is_keyword_column(column):
        return comma_terms.split_keyword_cell(value)
    return utils.split_cell(value)


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


def duplicate_keys(df: pd.DataFrame, key) -> list[str]:
    """Key strings that occur more than once in a stored version."""
    key_cols = key_columns(key)
    if df.empty or any(c not in df.columns for c in key_cols):
        return []
    keys = [KEY_SEP.join(str(r[c]).strip() for c in key_cols) for r in df[key_cols].to_dict("records")]
    return sorted({k for k in keys if keys.count(k) > 1}) if len(set(keys)) != len(keys) else []


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
    ``principalId`` (the runner's Synapse user ID), ``status`` (``pending``;
    ``written`` per export; ``removed`` for a DCC removal's direct-store
    version (D-Q49); ``done`` once every export of the write is logged;
    ``recovered``; or ``aborted`` when a run created nothing), ``writtenOn``,
    ``rowsWritten``. A ``pending`` row is resolved only by a later ``done``,
    ``recovered`` or ``aborted`` row with the same ``runId`` and
    ``baseVersion``, so a reused runId, a rerun, or a write with several exports
    keeps its lock and stays recoverable until it finishes. The migration also
    logs ``created`` (the RecordSet's creation version) and ``verified`` (a
    version read back and matched); neither is a pipeline version nor closes a
    ``pending`` row. Subclasses implement ``_read`` and ``_append``.
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
        """Record a version the pipeline created (``written``, ``removed`` or ``recovered``).

        ``base`` defaults to ``version - 1``.
        """
        base = version - 1 if base is None and version is not None else base
        self._append(self._row(rs_id, version, base, run_id, principal, status, rows))

    def removed(self, rs_id, version, base, run_id, principal, rows) -> None:
        """Record a DCC removal's direct-store version (``removed``: a pipeline version)."""
        self._append(self._row(rs_id, version, base, run_id, principal, "removed", rows))

    def finish(self, rs_id: str, base: int, run_id: str, principal) -> None:
        """Resolve a ``pending`` row once every export of its write is logged."""
        self._append(self._row(rs_id, None, base, run_id, principal, "done", None))

    def abort(self, rs_id: str, base: int, run_id: str, principal) -> None:
        """Resolve a ``pending`` row whose run failed before creating any version."""
        self._append(self._row(rs_id, None, base, run_id, principal, "aborted", None))

    def created(self, rs_id: str, version: int, run_id: str, principal) -> None:
        """Record the RecordSet's creation version (``created``: not a pipeline version, no lock)."""
        self._append(self._row(rs_id, version, version, run_id, principal, "created", None))

    def verified(self, rs_id: str, version: int, base: int, run_id: str, principal) -> None:
        """Record that ``version`` was read back and matched what was loaded (``verified``:
        neither a pipeline version nor a pending closer)."""
        self._append(self._row(rs_id, version, base, run_id, principal, "verified", None))

    def verified_versions(self, rs_id: str) -> set[int]:
        df = self._read(rs_id)
        df = df[(df["status"] == "verified") & df["version"].notna()]
        return {int(v) for v in df["version"]}

    def created_version(self, rs_id: str) -> int | None:
        """The creation version in the newest ``created`` row, or None."""
        df = self._read(rs_id)
        df = df[(df["status"] == "created") & df["version"].notna()]
        return int(max(df["version"])) if len(df) else None

    def pipeline_map(self, rs_id: str) -> dict[int, int]:
        """``{version: baseVersion}`` of every pipeline version (written, removed or recovered)."""
        df = self._read(rs_id)
        df = df[df["status"].isin(["written", "removed", "recovered"]) & df["version"].notna()]
        return {int(v): int(b) if pd.notna(b) else int(v) - 1 for v, b in zip(df["version"], df["baseVersion"])}

    def removal_versions(self, rs_id: str, run_id: str | None = None) -> set[int]:
        """Versions logged ``removed`` (a DCC removal's direct store), of one run if ``run_id`` is given."""
        df = self._read(rs_id)
        df = df[(df["status"] == "removed") & df["version"].notna()]
        if run_id is not None:
            df = df[df["runId"] == run_id]
        return {int(v) for v in df["version"]}

    def pipeline_versions(self, rs_id: str) -> list[int]:
        return sorted(self.pipeline_map(rs_id))

    def last_version(self, rs_id: str) -> int | None:
        versions = self.pipeline_versions(rs_id)
        return max(versions) if versions else None

    def unresolved_pending(self, rs_id: str) -> list[dict]:
        """``pending`` rows not yet resolved by a later ``done``/``recovered``/``aborted``
        row of the same runId and baseVersion."""
        df = self._read(rs_id)
        closers = [
            (r["runId"], _int_or_none(r["baseVersion"]), _to_dt(r["writtenOn"]))
            for _, r in df[df["status"].isin(["done", "recovered", "aborted"])].iterrows()
        ]
        out = []
        for _, row in df[df["status"] == "pending"].iterrows():
            base, when = _int_or_none(row["baseVersion"]), _to_dt(row["writtenOn"])
            if not any(r == row["runId"] and b == base and w >= when for r, b, w in closers):
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


# --- removals (D-Q49) --------------------------------------------------------

REMOVALS_COLUMNS = [
    "recordSetId", "key", "reason", "requestedBy", "status", "removedInVersion", "runId", "principalId",
    "changedOn",
]
REMOVAL_STATUSES = ("requested", "removed", "not_found", "restored")


class Removals:
    """Append-only removals table: which rows the DCC asked to remove, and what became of them.

    Columns: ``recordSetId``, ``key`` (the key string, :data:`KEY_SEP`-joined
    for a composite key), ``reason``, ``requestedBy``, ``status``
    (``requested``; ``removed``; ``not_found``; ``restored``, which lifts a
    removal so the pipeline uploads the key again), ``removedInVersion`` (the
    direct-store version, on ``removed`` rows), ``runId``, ``principalId``,
    ``changedOn``. The latest row per (recordSetId, key) is its current status.
    Subclasses implement ``_read`` and ``_append``.
    """

    def _read(self, rs_id: str) -> pd.DataFrame:
        raise NotImplementedError

    def _append(self, row: dict) -> None:
        raise NotImplementedError

    def append(self, rs_id, key, status, reason="", requested_by="", removed_in_version=None,
               run_id="", principal="") -> None:
        if status not in REMOVAL_STATUSES:
            raise ValueError(f"unknown removal status {status!r}")
        self._append({
            "recordSetId": rs_id, "key": key, "reason": reason, "requestedBy": requested_by,
            "status": status, "removedInVersion": removed_in_version, "runId": run_id,
            "principalId": str(principal), "changedOn": datetime.now(timezone.utc),
        })

    def recordset_ids(self) -> list[str]:
        """RecordSets that have any row."""
        raise NotImplementedError

    def _latest(self, rs_id: str) -> pd.DataFrame:
        df = self._read(rs_id)
        if df.empty:
            return df
        df = df.assign(_on=df["changedOn"].map(_to_dt)).sort_values("_on", kind="stable")
        return df.drop_duplicates("key", keep="last")

    def requested(self, rs_id: str) -> dict[str, dict]:
        """``{key: {reason, requestedBy}}`` of keys whose current status is ``requested``."""
        df = self._latest(rs_id)
        df = df[df["status"] == "requested"] if len(df) else df
        return {r["key"]: {"reason": r["reason"], "requestedBy": r["requestedBy"]} for _, r in df.iterrows()}

    def removed(self, rs_id: str) -> dict[str, int]:
        """``{key: removedInVersion}`` of keys whose current status is ``removed``."""
        df = self._latest(rs_id)
        df = df[(df["status"] == "removed") & df["removedInVersion"].notna()] if len(df) else df
        return {r["key"]: int(r["removedInVersion"]) for _, r in df.iterrows()}

    def removal_events(self, rs_id: str) -> dict[int, set[str]]:
        """``{removedInVersion: keys}`` of every ``removed`` row, restored or not (for :func:`replay`)."""
        df = self._read(rs_id)
        df = df[(df["status"] == "removed") & df["removedInVersion"].notna()] if len(df) else df
        out: dict[int, set[str]] = {}
        for _, r in df.iterrows():
            out.setdefault(int(r["removedInVersion"]), set()).add(r["key"])
        return out


class SynapseTableRemovals(Removals):
    """Removals kept in a Synapse table (see :func:`create_removals_table`)."""

    def __init__(self, syn, table_id: str):
        self.syn = syn
        self.table_id = table_id

    def _read(self, rs_id: str) -> pd.DataFrame:
        return Table(id=self.table_id).query(
            query=f"SELECT * FROM {self.table_id} WHERE recordSetId = '{rs_id}'",
            include_row_id_and_row_version=False,
            synapse_client=self.syn,
        )

    def recordset_ids(self) -> list[str]:
        df = Table(id=self.table_id).query(
            query=f"SELECT DISTINCT recordSetId FROM {self.table_id}",
            include_row_id_and_row_version=False,
            synapse_client=self.syn,
        )
        return sorted(df["recordSetId"])

    def _append(self, row: dict) -> None:
        values = dict(row)
        values["changedOn"] = int(values["changedOn"].timestamp() * 1000)
        frame = pd.DataFrame([values], columns=REMOVALS_COLUMNS)
        frame["removedInVersion"] = frame["removedInVersion"].astype("Int64")
        Table(id=self.table_id).store_rows(values=frame, synapse_client=self.syn)


class CsvRemovals(Removals):
    """The same table as a local CSV, for tests and dry runs."""

    def __init__(self, path):
        self.path = Path(path)

    def _read(self, rs_id: str) -> pd.DataFrame:
        if not self.path.exists():
            return pd.DataFrame(columns=REMOVALS_COLUMNS)
        df = pd.read_csv(self.path, dtype=str, keep_default_na=False)
        df = df[df["recordSetId"] == rs_id].copy()
        df["removedInVersion"] = pd.to_numeric(df["removedInVersion"].replace("", pd.NA))
        return df

    def recordset_ids(self) -> list[str]:
        if not self.path.exists():
            return []
        return sorted(set(pd.read_csv(self.path, dtype=str, keep_default_na=False)["recordSetId"]))

    def _append(self, row: dict) -> None:
        new = not self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if new:
                writer.writerow(REMOVALS_COLUMNS)
            writer.writerow(
                ["" if row[c] is None else (row[c].isoformat() if c == "changedOn" else row[c])
                 for c in REMOVALS_COLUMNS]
            )


def create_removals_table(syn, parent_id: str, name: str = "MC2Center_RecordSet_Removals") -> str:
    """Create the removals table under ``parent_id`` and return its ID.

    A Synapse write. Not called by any script; a migration step runs it once.
    """
    columns = [
        Column(name="recordSetId", column_type=ColumnType.ENTITYID),
        Column(name="key", column_type=ColumnType.STRING, maximum_size=500),
        Column(name="reason", column_type=ColumnType.STRING, maximum_size=500),
        Column(name="requestedBy", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="status", column_type=ColumnType.STRING, maximum_size=20),
        Column(name="removedInVersion", column_type=ColumnType.INTEGER),
        Column(name="runId", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="principalId", column_type=ColumnType.STRING, maximum_size=20),
        Column(name="changedOn", column_type=ColumnType.DATE),
    ]
    table = Table(name=name, parent_id=parent_id, columns=columns).store(synapse_client=syn)
    return table.id


def drop_removed(upload: pd.DataFrame, key, removed: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split an upload into ``(kept, skipped)``; ``skipped`` rows have a key currently ``removed``.

    ``removed`` is :meth:`Removals.removed`'s ``{key: removedInVersion}``;
    ``skipped`` gets a ``removedInVersion`` column.
    """
    key_cols = key_columns(key)
    keys = [_key_of(row, key_cols) for _, row in upload.iterrows()]
    hit = pd.Series([k in removed for k in keys], index=upload.index, dtype=bool)
    skipped = upload[hit].copy()
    skipped["removedInVersion"] = [removed[k] for k, h in zip(keys, hit) if h]
    return upload[~hit].copy(), skipped


def recordset_version_meta(syn, rs_id: str, version: int) -> tuple[str, pd.Timestamp]:
    """``(modifiedBy, modifiedOn)`` of one RecordSet version, without downloading it."""
    entity = RecordSet(id=rs_id, version_number=version, download_file=False).get(synapse_client=syn)
    return str(entity.modified_by), _to_dt(entity.modified_on)


class Recovered(dict):
    """``{version: base}`` from :func:`recover_pending`; ``removal`` holds the versions of it that are removal stores."""

    removal: set

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.removal = set()


def recover_pending(syn, rs_id, write_log, latest, meta=None, record=True,
                    run_id=None, skip_younger_than=None, now=None) -> Recovered:
    """Recover versions created by runs that died after exporting.

    For each unresolved ``pending`` row, a version above its ``baseVersion``
    that is not logged, was modified by the row's ``principalId`` and not
    earlier than ``writtenOn - CLOCK_TOLERANCE`` is a pipeline version with
    that base: recorded as ``recovered`` (when ``record``) and returned as
    ``{version: base}``. A pending row with nothing to recover is resolved as
    ``aborted``. Call it before this run's own ``start``; a crashed export of
    the same runId is recovered like any other. Before ``start``, pass
    ``skip_younger_than=LOCK_HOURS``: a younger row belongs to a live run
    (``locked_by`` already let us through), and must not be aborted under it.
    After a failed write, pass ``run_id`` to resolve only this run's own row.
    A DCC removal run (runId with :data:`REMOVAL_RUN_PREFIX`) stores its version
    directly: the first recovered version of such a row, when the run has not
    logged its ``removed`` version yet, is the store and is logged ``removed``
    (replay drops its keys); later ones are Grid exports, ``recovered``.
    """
    meta = meta or (lambda v: recordset_version_meta(syn, rs_id, v))
    logged = write_log.pipeline_map(rs_id)
    found = Recovered()
    now = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
    for row in write_log.unresolved_pending(rs_id):
        if row["baseVersion"] is None:
            continue
        if run_id is not None and row["runId"] != run_id:
            continue
        if skip_younger_than is not None and now - row["writtenOn"] < pd.Timedelta(hours=skip_younger_than):
            continue
        mine = []
        for v in range(row["baseVersion"] + 1, latest + 1):
            if v in logged or v in found:
                continue
            by, on = meta(v)
            if by == row["principalId"] and pd.notna(on) and on >= row["writtenOn"] - CLOCK_TOLERANCE:
                mine.append(v)
        store = None
        if (mine and str(row["runId"]).startswith(REMOVAL_RUN_PREFIX)
                and not write_log.removal_versions(rs_id, row["runId"])):
            store = mine[0]
            found.removal.add(store)
        for v in mine:
            found[v] = row["baseVersion"]
            if record and v == store:
                write_log.removed(rs_id, v, row["baseVersion"], row["runId"], row["principalId"], None)
            elif record:
                write_log.record(rs_id, v, row["runId"], None, base=row["baseVersion"],
                                 principal=row["principalId"], status="recovered")
        if store is not None and record:  # ``removed`` doesn't close the row; the owed export is a later run's
            write_log.finish(rs_id, row["baseVersion"], row["runId"], row["principalId"])
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
    for record in df.reindex(columns=columns, fill_value="").to_dict("records"):
        out[KEY_SEP.join(str(record[c]).strip() for c in key_cols)] = record
    return out


def _all_columns(frames, key_cols) -> list[str]:
    columns = list(key_cols)
    for df in frames:
        columns += [c for c in df.columns if c not in columns]
    return columns


def _is_blank(value) -> bool:
    return _norm(value) == ""


def _session_base(cf, diffs, fingerprints, v) -> int:
    """The version a foreign version ``v`` was exported from: ``v - 1`` or an older stale base.

    A Grid export is the snapshot its session opened on plus the user's edits.
    A fresh session opens on ``v - 1``. A session opened earlier, on ``u``
    (``u <= v - 2``), is recognised only by a *complete reversion*: every cell
    that differs between ``u`` and ``v - 1`` (a row missing from one side
    counts as differing, cell by cell) has ``u``'s value in ``v``, so the
    session undid everything changed since ``u``. The newest such ``u`` wins;
    a ``u`` with nothing changed since is skipped. Anything weaker (a partial
    reversion) is a contributor's undo and gets snapshot ``v - 1``, as does an
    exact copy of an older version [owner decisions: ambiguous cases go to the
    contributor].

    The walk is incremental: ``diffs[w]`` holds the cell changes of the step
    ``w - 1 -> w``, folded in one step at a time while ``u`` moves down,
    keeping the cells changed since ``u`` and how many of them ``v`` reverts.
    """
    if v < 3:
        return v - 1
    if any(fingerprints[u] == fingerprints[v] and cf[u] == cf[v] for u in range(1, v - 1)):
        return v - 1  # an exact copy of an older version: a deliberate undo
    target, previous = cf[v], cf[v - 1]

    def in_v(k, i):
        row = target.get(k)
        return _ABSENT if row is None else row[i]

    def in_previous(k, i):
        row = previous.get(k)
        return _ABSENT if row is None else row[i]

    changed: dict[tuple, object] = {}  # cell -> its value in u, for cells differing from v - 1
    reverted = 0  # how many of them v sets back to u's value
    for u in range(v - 2, 0, -1):
        for cell, old in diffs[u + 1].items():
            k, i = cell
            if cell in changed:
                reverted -= changed.pop(cell) == in_v(k, i)
            if old != in_previous(k, i):
                changed[cell] = old
                reverted += old == in_v(k, i)
        if changed and reverted == len(changed):
            return u
    return v - 1


def replay(versions, log_map, key, set_like=(), removal_versions=(), restored=()):
    """Rebuild the current state from every version, tracking cell ownership.

    ``versions`` is ``[(version, DataFrame)]`` for every version ``1..latest``;
    ``log_map`` is ``{pipeline version: baseVersion}`` from the write log;
    ``removal_versions`` are the versions logged ``removed``
    (:meth:`WriteLog.removal_versions`, D-Q49); the keys a removal dropped are
    read off the history itself (in ``v - 1``, not in ``v``), so replay never
    depends on the removals table. ``restored`` are keys whose removal was
    lifted: their re-adding by the pipeline is not reported.
    The rules are in the module docstring. Returns ``(state, owned, decisions)``:
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
    non_key = [c for c in columns if c not in key_cols]
    flags = [c in set_like for c in non_key]
    frames = {v: _frames_by_key(df, key_cols, columns) for v, df in versions}
    # canonical cells of every version: key -> tuple aligned with non_key. Rows
    # unchanged since the previous version reuse its tuple; string cells are memoised.
    memo: dict[str, object] = {}

    def canon_cell(value, flag):
        if isinstance(value, str):
            if value not in memo:
                memo[value] = _canon(value)
            return memo[value]
        return _canon(value, flag)

    cf: dict[int, dict] = {}
    previous = None
    for v, _ in versions:
        out = {}
        for k, r in frames[v].items():
            if previous is not None and k in frames[previous] and frames[previous][k] == r:
                out[k] = cf[previous][k]
            else:
                out[k] = tuple(canon_cell(r[c], f) for c, f in zip(non_key, flags))
        cf[v], previous = out, v
    latest = versions[-1][0]

    decisions: list[dict] = []
    state: dict[str, dict] = {}  # key -> {column: [value, owner, set_at, canonical value]}

    def fresh(k, v, owner_of):
        row, crow = frames[v][k], cf[v][k]
        cells = {c: [row[c], owner_of(row[c]), v, crow[i]] for i, c in enumerate(non_key)}
        cells.update({c: [row[c], P, v, row[c]] for c in key_cols})
        return cells

    if not log_map:
        state = {k: fresh(k, latest, lambda _: P) for k in frames[latest]}
        first, report_after = latest + 1, latest
    else:
        first, report_after = min(log_map), max(log_map)
        if first - 1 in frames:
            state = {k: fresh(k, first - 1, lambda _: P) for k in frames[first - 1]}

    def decide(v, k, column, decision, cur="", pipe="", always=False):
        if always or v > report_after:
            decisions.append({"key": k, "column": column, "decision": decision,
                              "current_value": cur, "pipeline_value": pipe, "version": v})

    removal_versions, restored = set(removal_versions), set(restored)
    gone: set[str] = set()  # keys removed by the DCC and not yet seen again
    diffs = fingerprints = None
    if log_map and any(v not in log_map and v >= first for v in frames):
        diffs = _version_diffs(cf, latest)
        fingerprints = {v: hash(frozenset(cf[v].items())) for v in cf}

    for v in sorted(frames):
        if v < first:
            continue
        frame, crows = frames[v], cf[v]
        if v in log_map:
            base = log_map[v]
            if v in removal_versions and v - 1 in frames:
                for k in [k for k in frames[v - 1] if k not in frame]:
                    if k in state:
                        del state[k]
                        gone.add(k)
                        decide(v, k, "", "removed")
            for k, row in frame.items():
                if k not in state:
                    state[k] = fresh(k, v, lambda _: P)
                    if k in gone:
                        gone.discard(k)
                        if k not in restored:
                            decide(v, k, "", "readded_by_pipeline", always=True)
                    continue
                crow = crows[k]
                for i, c in enumerate(non_key):
                    cell = state[k][c]
                    if crow[i] == cell[3]:
                        continue
                    if cell[1] == C and cell[2] > base:
                        decide(v, k, c, "kept_unseen_edit", row[c], cell[0])
                    else:
                        state[k][c] = [row[c], P, v, crow[i]]
            for k in state:
                if k not in frame:
                    decide(v, k, "", "kept_unseen_row")
        else:
            # A Grid export is the snapshot its session opened on plus the
            # user's edits, so a cell that still equals that snapshot was not
            # touched: if it differs from the state, it is stale. Only cells
            # that differ from the snapshot are contributor edits, even when
            # they match an older value (e.g. undoing a pipeline change).
            snapshot = cf[_session_base(cf, diffs, fingerprints, v)] if v > 1 else {}
            for k, row in frame.items():
                if k not in state:
                    state[k] = fresh(k, v, lambda x: P if _is_blank(x) else C)
                    if k in gone:
                        gone.discard(k)
                        srow = snapshot.get(k)
                        if srow is not None:  # the session had the row: only its edits are contributor cells
                            for i, c in enumerate(non_key):
                                if crows[k][i] == srow[i]:
                                    state[k][c][1] = P
                        decide(v, k, "", "readded_removed_key")
                    else:
                        decide(v, k, "", "kept_contributor_row")
                    continue
                crow, srow = crows[k], snapshot.get(k)
                for i, c in enumerate(non_key):
                    cell = state[k][c]
                    if crow[i] == cell[3]:
                        continue
                    if srow is not None and crow[i] == srow[i]:
                        decide(v, k, c, "restored_revert", row[c], cell[0])
                    else:
                        decide(v, k, c, "kept_edit", row[c], cell[0])
                        state[k][c] = [row[c], C, v, crow[i]]
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


def _version_diffs(cf, latest) -> dict[int, dict]:
    """``{w: {(key, column index): old canonical value}}`` for each step ``w - 1 -> w``.

    A cell of a row that is absent on one side has the value ``_ABSENT`` there.
    """
    diffs = {}
    for w in range(2, latest + 1):
        prev, cur, out = cf[w - 1], cf[w], {}
        for k, row in cur.items():
            old = prev.get(k)
            if old is None:
                for i in range(len(row)):
                    out[(k, i)] = _ABSENT
            elif old != row:
                for i, (a, b) in enumerate(zip(old, row)):
                    if a != b:
                        out[(k, i)] = a
        for k, old in prev.items():
            if k not in cur:
                for i, a in enumerate(old):
                    out[(k, i)] = a
        diffs[w] = out
    return diffs


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


def write_via_grid(syn, rs_id: str, csv_path, **kwargs) -> list[int]:
    """Upsert ``csv_path`` into the RecordSet through a Grid session; return every version created.

    See :func:`_grid_session` for the keyword arguments and the wait/retry rules.
    """
    return _grid_session(syn, rs_id, csv_path, **kwargs)


def export_via_grid(syn, rs_id: str, **kwargs) -> list[int]:
    """Re-validate the RecordSet: a fresh Grid session with no import and one export (D-Q49).

    Same waits, retries and cleanup as :func:`write_via_grid`.
    """
    return _grid_session(syn, rs_id, None, **kwargs)


def open_grid_sessions(syn, rs_id: str) -> list:
    """Grid sessions currently open on the RecordSet (``Grid.list(source_id=...)``)."""
    return list(Grid.list(source_id=rs_id, synapse_client=syn))


def store_new_version(syn, rs_id: str, csv_path) -> int:
    """Store ``csv_path`` as a new RecordSet version directly (no Grid session); return its number.

    The only write that can drop rows (D-Q49). The entity is fetched without
    its file and its ``path`` replaced, so the binding, upsert key and
    CurationTask are kept.
    """
    entity = RecordSet(id=rs_id, download_file=False).get(synapse_client=syn)
    entity.path = str(csv_path)
    return entity.store(synapse_client=syn).version_number


def _grid_session(
    syn,
    rs_id: str,
    csv_path=None,
    timeout: int = 120,
    validation_wait: int = 60,
    retry_wait: int = 30,
    max_exports: int = 4,
    on_version=None,
) -> list[int]:
    """Fresh Grid session, import ``csv_path`` (none: skip), wait for validation, export
    (a new RecordSet version), delete the session. The grid session exposes no validation
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
        if csv_path is not None:
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


def principal_id(syn) -> str:
    """The runner's Synapse user ID, as logged in the write log."""
    return str(syn.getUserProfile()["ownerId"])


class WriteRefused(Exception):
    """A logged write was not started: ``status`` is ``locked`` or ``retry``."""

    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def write_logged(syn, rs_id: str, csv_path, write_log, run_id: str, principal, rows: int, base: int,
                 retry_note: str = "the next run replays it") -> list[int]:
    """Write ``csv_path`` through a Grid session behind a write-log ``pending`` row.

    ``base`` is the RecordSet version the content was computed against. Logs
    ``start``, one ``written`` row per exported version, then ``done``, and
    returns the versions created. Raises :class:`WriteRefused` (nothing
    written, the pending row aborted) when another run's earlier pending row
    holds the lock, or when the RecordSet moved past ``base`` since it was read.
    If the write fails, exports that completed are already logged; a version
    whose export raised after it was created is claimed through
    :func:`recover_pending`, or the run's pending row is resolved as aborted,
    and the original exception propagates.
    """
    write_log.start(rs_id, base, run_id, principal)
    created: list[int] = []

    def on_version(v):
        write_log.record(rs_id, v, run_id, rows, base=base, principal=principal)
        created.append(v)

    try:
        # Lock race: another run may have started between the caller's check and
        # our pending row. The earlier pending row (ties by runId) keeps the lock.
        winner = write_log.earlier_pending(rs_id, run_id)
        if winner:
            write_log.abort(rs_id, base, run_id, principal)
            raise WriteRefused("locked", f"run {winner} started its write first; skipped")
        # The content was computed against ``base``; if the RecordSet moved, recompute next run.
        now_latest, _ = recordset_info(syn, rs_id)
        if now_latest != base:
            write_log.abort(rs_id, base, run_id, principal)
            raise WriteRefused("retry", (
                f"{rs_id} moved from v{base} to v{now_latest} before the write; nothing written, "
                f"{retry_note}"))
        write_via_grid(syn, rs_id, csv_path, on_version=on_version)
        write_log.finish(rs_id, base, run_id, principal)
    except WriteRefused:
        raise
    except BaseException:
        try:
            newest, _ = recordset_info(syn, rs_id)
            recover_pending(syn, rs_id, write_log, newest, run_id=run_id)
        except Exception:
            logger.exception("%s: could not resolve the pending write-log row", rs_id)
        raise
    return created


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
