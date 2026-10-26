"""Shared library for shared RecordSets: read, write through Grid, reconcile.

Gate G1: each grant has one RecordSet per type (``MC2Center_<Component>_RecordSet``
in the grant's data-type folder) that the pipeline and contributors both edit.
The pipeline writes only through a Grid session (``write_via_grid``), which
validates and versions the data, and logs every version it writes in a
:class:`WriteLog`. Before a new upload, :func:`reconcile` compares the latest
version with what the pipeline last wrote so contributor edits survive, and
:func:`merge_upload` applies the upload on top.

Cell values: a list-valued cell is a Python ``list`` of strings (stored in the
RecordSet CSV as a JSON array); every other cell is a string, blank is ``""``.

List comparison rule: a list cell is compared order-insensitively only in
*set-like* columns, which are the schema's array properties whose items are an
enum (Assay, Tissue, Tumor Type, ...; see :func:`set_like_columns`). Any other
list (authors, keywords, aliases) is compared as is, so a reorder counts as an
edit. An empty list and ``""`` are the same value.
"""

from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from synapseclient import operations
from synapseclient.models import Column, ColumnType, Grid, RecordSet, Table

import model_contract
import utils

logger = logging.getLogger(__name__)

KEY_SEP = "|"

DECISION_COLUMNS = ["key", "column", "decision", "current_value", "pipeline_value", "version"]
CONFLICT_COLUMNS = ["key", "column", "contributor_value", "upload_value"]

WRITE_LOG_COLUMNS = ["recordSetId", "version", "runId", "writtenOn", "rowsWritten"]


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


def _cells_equal(a, b, set_like: bool) -> bool:
    return _norm(a, set_like) == _norm(b, set_like)


def key_columns(key) -> list[str]:
    return [key] if isinstance(key, str) else list(key)


def _key_of(row, key_cols: list[str]) -> str:
    return KEY_SEP.join(str(row[c]) for c in key_cols)


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


def read_recordset(syn, rs_id: str, version: int | None = None) -> pd.DataFrame:
    """Read a RecordSet version (default: latest) with string dtype.

    JSON-array cells become Python lists, other cells stay strings, empty
    cells are ``""``.
    """
    entity = RecordSet(id=rs_id, version_number=version, download_file=True).get(synapse_client=syn)
    try:
        df = pd.read_csv(entity.path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    return df.map(parse_cell)


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


class WriteLog:
    """Which RecordSet versions the pipeline wrote."""

    def pipeline_versions(self, rs_id: str) -> list[int]:
        raise NotImplementedError

    def record(self, rs_id: str, version: int, run_id: str, rows: int) -> None:
        raise NotImplementedError

    def last_version(self, rs_id: str) -> int | None:
        versions = self.pipeline_versions(rs_id)
        return max(versions) if versions else None


class SynapseTableWriteLog(WriteLog):
    """Write log kept in a Synapse table (see :func:`create_write_log_table`)."""

    def __init__(self, syn, table_id: str):
        self.syn = syn
        self.table_id = table_id

    def pipeline_versions(self, rs_id: str) -> list[int]:
        df = Table(id=self.table_id).query(
            query=f"SELECT version FROM {self.table_id} WHERE recordSetId = '{rs_id}'",
            include_row_id_and_row_version=False,
            synapse_client=self.syn,
        )
        return sorted(int(v) for v in df["version"].dropna())

    def record(self, rs_id: str, version: int, run_id: str, rows: int) -> None:
        row = pd.DataFrame(
            [{
                "recordSetId": rs_id,
                "version": int(version),
                "runId": run_id,
                "writtenOn": int(time.time() * 1000),
                "rowsWritten": int(rows),
            }]
        )
        Table(id=self.table_id).store_rows(values=row, synapse_client=self.syn)


class CsvWriteLog(WriteLog):
    """The same log as a local CSV, for tests and ``--dryrun``."""

    def __init__(self, path):
        self.path = Path(path)

    def _read(self) -> pd.DataFrame:
        if not self.path.exists():
            return pd.DataFrame(columns=WRITE_LOG_COLUMNS)
        return pd.read_csv(self.path, dtype=str, keep_default_na=False)

    def pipeline_versions(self, rs_id: str) -> list[int]:
        df = self._read()
        return sorted(int(v) for v in df.loc[df["recordSetId"] == rs_id, "version"])

    def record(self, rs_id: str, version: int, run_id: str, rows: int) -> None:
        new = not self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if new:
                writer.writerow(WRITE_LOG_COLUMNS)
            writer.writerow(
                [rs_id, int(version), run_id, datetime.now(timezone.utc).isoformat(), int(rows)]
            )


def create_write_log_table(syn, parent_id: str, name: str = "MC2Center_RecordSet_WriteLog") -> str:
    """Create the write-log table under ``parent_id`` and return its ID.

    A Synapse write. Not called by any script; a migration step runs it once.
    """
    columns = [
        Column(name="recordSetId", column_type=ColumnType.ENTITYID),
        Column(name="version", column_type=ColumnType.INTEGER),
        Column(name="runId", column_type=ColumnType.STRING, maximum_size=100),
        Column(name="writtenOn", column_type=ColumnType.DATE),
        Column(name="rowsWritten", column_type=ColumnType.INTEGER),
    ]
    table = Table(name=name, parent_id=parent_id, columns=columns).store(synapse_client=syn)
    return table.id


# --- reconcile / merge -------------------------------------------------------


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


def reconcile(current, pipeline_last, history, key, set_like=(), version=""):
    """Fold contributor edits into the pipeline's last write.

    ``current`` is the latest version; ``pipeline_last`` the rows at the
    pipeline's most recent write; ``history`` the rows at every version up to
    and including that one (a list of DataFrames). Anything newer than
    ``pipeline_last`` is a contributor edit. ``set_like`` names list columns
    compared order-insensitively; ``version`` tags every decision with the
    version ``current`` came from.

    Per key in the union:

    * only in ``pipeline_last``: restore the pipeline row (``restored_missing``)
    * only in ``current``: keep it (``kept_contributor_row``)
    * in both, per non-key column: equal, nothing; the current value equals a
      value the cell had in ``history`` (so it is a revert to an old value):
      restore the pipeline value (``restored_revert``); else keep the current
      value (``kept_edit``)

    Returns ``(reconciled DataFrame, decisions DataFrame)``; decisions have the
    columns ``key, column, decision, current_value, pipeline_value``.
    """
    key_cols = key_columns(key)
    set_like = set(set_like)
    columns = _all_columns([current, pipeline_last, *history], key_cols)
    cur = _frames_by_key(current, key_cols, columns)
    pipe = _frames_by_key(pipeline_last, key_cols, columns)
    hist = [_frames_by_key(h, key_cols, columns) for h in history]

    rows, decisions = [], []

    def decide(k, column, decision, cur_v="", pipe_v=""):
        decisions.append(
            {"key": k, "column": column, "decision": decision,
             "current_value": cur_v, "pipeline_value": pipe_v, "version": version}
        )

    for k, cur_row in cur.items():
        if k not in pipe:
            rows.append(cur_row)
            decide(k, "", "kept_contributor_row")
            continue
        pipe_row, out = pipe[k], dict(cur_row)
        for column in columns:
            if column in key_cols:
                continue
            c, p = cur_row[column], pipe_row[column]
            if _cells_equal(c, p, column in set_like):
                continue
            was_old_value = any(
                k in h and _cells_equal(h[k][column], c, column in set_like) for h in hist
            )
            if was_old_value:
                out[column] = p
                decide(k, column, "restored_revert", c, p)
            else:
                decide(k, column, "kept_edit", c, p)
        rows.append(out)
    for k, pipe_row in pipe.items():
        if k not in cur:
            rows.append(dict(pipe_row))
            decide(k, "", "restored_missing")

    reconciled = pd.DataFrame(rows, columns=columns)
    return reconciled, pd.DataFrame(decisions, columns=DECISION_COLUMNS)


def reconcile_versions(foreign, pipeline_last, history, key, set_like=()):
    """Fold foreign versions into the pipeline's last write, oldest first.

    ``foreign`` is a list of ``(version, DataFrame)`` newer than
    ``pipeline_last``, in version order; ``history`` the versions up to and
    including the pipeline's. Each step is ``reconcile(current=f,
    pipeline_last=state, history=history)`` and then ``f`` joins ``history``,
    so an edit that a later stale-session export wiped is seen as a revert to
    a known value and restored. Returns ``(state, decisions, edited)``:
    decisions from every step tagged with their version, and the
    ``(key, column)`` cells that hold a contributor edit in the final state
    (a ``kept_edit`` at some step whose final value differs from
    ``pipeline_last``), for :func:`merge_upload`.
    """
    key_cols = key_columns(key)
    set_like = set(set_like)
    state, history, parts = pipeline_last, list(history), []
    for version, frame in foreign:
        state, step = reconcile(frame, state, history, key, set_like, version=version)
        parts.append(step)
        history.append(frame)
    decisions = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=DECISION_COLUMNS)
    columns = _all_columns([state, pipeline_last], key_cols)
    final = _frames_by_key(state, key_cols, columns)
    base = _frames_by_key(pipeline_last, key_cols, columns)
    edited = {
        (k, c)
        for k, c in edited_cells(decisions)
        if k in final and k in base and not _cells_equal(final[k][c], base[k][c], c in set_like)
    }
    return state, decisions, edited


def edited_cells(decisions: pd.DataFrame) -> set[tuple[str, str]]:
    """``(key, column)`` pairs of the ``kept_edit`` decisions."""
    kept = decisions[decisions["decision"] == "kept_edit"]
    return set(zip(kept["key"], kept["column"]))


def merge_upload(reconciled, upload, key, edited, set_like=()):
    """Upsert ``upload`` rows into ``reconciled``; contributors win on edited cells.

    Existing keys are updated cell by cell, new keys appended, keys absent from
    the upload left alone. For a cell in ``edited`` (the ``kept_edit``
    decisions) where the upload value differs, the reconciled value is kept
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
) -> list[int]:
    """Upsert ``csv_path`` into the RecordSet through a Grid session; return every version created.

    Fresh session, import, wait for validation, export (a new RecordSet
    version), delete the session. The grid session exposes no validation
    statistics before an export (only the export response carries a summary),
    so the wait is a fixed ``validation_wait`` seconds after the import; the
    export summary's unknown-row count is then checked, and while rows are
    still unknown the session waits ``retry_wait`` seconds and exports again
    (at most ``max_exports`` exports; each creates a version with the same
    data). All created versions are returned, oldest first; log each one.
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
        grid.delete(synapse_client=syn)


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
