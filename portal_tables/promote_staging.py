"""Promote the staging CCKP portal tables to production.

Make each production portal table an exact copy of its staging table (full
replace). This is a dry run unless ``--execute`` is given: the dry run reads
both tables and writes only local report files (schema check, row diff,
summary). Nothing is written to Synapse.

This script is the only one that writes the production portal tables; every
other script writes the staging tables.

Writing production tables needs explicit sign-off. Do not run ``--execute``
without it.

Steps:
  1. Compare the columns of every selected staging and production table (name,
     type, size, list length, enum values). If any table differs, write
     ``schema_diff.csv`` and stop. No table is written.
  2. Diff the rows and write the report files. Each resource has one key (a
     column, or a tuple of columns). Key values are compared in canonical form.
     Production rows that share a key are combined into one record (list cells:
     union of the items; scalar cells: the distinct non-blank values). Staging
     rows that share a key are a data problem: they go to ``duplicate_keys.csv``
     and are combined the same way. Cells are compared in canonical form: blank
     forms are equal, list cells are sets of items, text has its whitespace
     collapsed, booleans ignore case, numbers compare by value. A cell whose raw
     values differ but whose canonical values are equal is a format-only change
     (``list_order``, ``empty_list_item``, ``whitespace`` or ``other``). Other
     differences are content changes. Files:
       summary.csv                one line per resource
       <resource>_added.csv       staging rows whose key is not in production
       <resource>_removed.csv     production rows whose key is not in staging
       <resource>_changed.csv     one line per content-changed cell
       format_changes.csv         format-only cells: count and one example
       duplicate_keys.csv         staging duplicate keys
     The added, removed, changed and duplicate files exist only when not empty.
  3. With ``--execute``, for each table in order: snapshot the staging table,
     replace the production rows (``utils.update_table``; it snapshots
     production and restores it on failure), read production back and compare
     it with staging. Stop at the first failure; later tables are not run.
     Tables promoted before a failure stay promoted.

Reports go to ``<report-dir>/<run-id>/``.

Example:
    python portal_tables/promote_staging.py --run-id 2026-11-18 --tables tool
    python portal_tables/promote_staging.py --run-id 2026-11-18 --execute
"""

import argparse
import ast
import json
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from synapseclient.models import Table

from cells import is_null, json_list
import utils

RESOURCES = [r for r, c in utils.CONFIG.items() if "staging_table" in c]

# Portal columns that make the record key of each resource.
KEY_COLUMNS = {
    "publication": ["pubMedId"],
    "dataset": ["datasetAlias"],
    "tool": ["toolName"],
    "education": ["alias"],
    "grant": ["grantNumber"],
    "people": ["synapseProfileId"],
    "project": ["projectName", "grant"],
    "consortium_counts": ["consortium", "groupBy"],
    "consortium_theme_counts": ["theme", "consortium", "groupBy"],
    "theme_counts": ["theme", "groupBy"],
}

COLUMN_ATTRS = ["name", "column_type", "maximum_size", "maximum_list_length", "enum_values"]
SYSTEM_COLUMNS = ["ROW_ID", "ROW_VERSION", "ROW_ETAG"]
BLANK_TEXT = {"", "nan", "None"}
NUMBER = re.compile(r"-?(0|[1-9]\d*)(\.\d+)?")
SUMMARY_COLUMNS = [
    "resource", "staging_id", "production_id", "staging_rows", "production_rows", "key",
    "production_keys_combined", "staging_duplicate_keys", "added", "removed",
    "changed_records", "changed_cells", "format_only_cells", "status",
]


def _cell(value):
    """Hashable, comparable form of a cell: nulls are blank, lists are tuples."""
    if isinstance(value, (list, tuple)):
        return tuple(_cell(v) for v in value)
    if is_null(value):
        return ""
    return value


def _attr(col, attr) -> str:
    value = getattr(col, attr, None)
    if attr == "column_type" and value is not None:
        value = getattr(value, "value", value)
    if isinstance(value, (list, tuple)):
        value = list(value)
    return "" if value is None else str(value)


def schema_diff(resource: str, staging, production) -> list[dict]:
    """Differences between the columns (in order) of two tables."""
    s_cols, p_cols = list(staging.columns.values()), list(production.columns.values())
    rows = []
    for i in range(max(len(s_cols), len(p_cols))):
        s = s_cols[i] if i < len(s_cols) else None
        p = p_cols[i] if i < len(p_cols) else None
        for attr in COLUMN_ATTRS:
            sv = "" if s is None else _attr(s, attr)
            pv = "" if p is None else _attr(p, attr)
            if sv != pv or (s is None) != (p is None):
                rows.append({"table": resource, "column": (s or p).name, "attribute": attr,
                             "staging": sv, "production": pv})
    return rows


def read_table(syn, table_id: str) -> pd.DataFrame:
    """All rows of a table, without system columns, blanks normalised."""
    df = utils.read_table(syn, table_id, include_row_id_and_row_version=True)  # the system columns are dropped below
    df = df.drop(columns=[c for c in SYSTEM_COLUMNS if c in df.columns])
    return utils.fill_blank(df, numeric=False).reset_index(drop=True)


def _signature(row) -> tuple:
    return tuple(_cell(v) for v in row)


def _squash(text: str) -> str:
    return " ".join(text.split())


def _number(value) -> str:
    return str(int(value)) if float(value) == int(float(value)) else repr(float(value))


def _scalar(value) -> str:
    """Canonical text of a non-list cell: blank is "", numbers by value, booleans lower case."""
    if is_null(value):
        return ""
    if isinstance(value, (bool, np.bool_)):
        return str(bool(value)).lower()
    if isinstance(value, (int, float, np.integer, np.floating)):
        return _number(value)
    text = _squash(str(value))
    if text in BLANK_TEXT:
        return ""
    if text.lower() in ("true", "false"):
        return text.lower()
    return _number(text) if NUMBER.fullmatch(text) else text


def _python_list(text: str) -> list | None:
    """The list a Python-repr array string (``['a', 'b']``) holds, or None."""
    text = text.strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return None
        return parsed if isinstance(parsed, list) else None
    return None


def _raw_items(value) -> list | None:
    """The items of a list cell (a list or a JSON-array string) as text; None for other cells."""
    if isinstance(value, str):
        value = json_list(value) or _python_list(value)
    if isinstance(value, (list, tuple, np.ndarray)):
        return [str(v) for v in value]
    return None


def canon(value):
    """Comparable form of a cell: "" for blank, a string, or a frozenset of items for a list."""
    items = _raw_items(value)
    if items is None:
        return _scalar(value)
    return frozenset(i for i in map(_scalar, items) if i) or ""


def combine(values: list):
    """One canonical value for the cells of the rows of one key."""
    items, scalars = set(), set()
    is_list = False
    for c in map(canon, values):
        if isinstance(c, frozenset):
            is_list = True
            items |= c
        elif c:
            scalars.add(c)
    if is_list or len(scalars) > 1:
        return frozenset(items | scalars) or ""
    return next(iter(scalars), "")


def _show(value) -> str:
    """Text of a canonical or raw cell for a report."""
    if isinstance(value, frozenset):
        return json.dumps(sorted(value), ensure_ascii=False)
    items = _raw_items(value)
    if items is not None:
        return json.dumps(items, ensure_ascii=False)
    return "" if is_null(value) else str(value)


def format_kind(production, staging) -> str:
    """Why two cells with equal canonical values differ as written."""
    p, s = _raw_items(production), _raw_items(staging)
    if p is None and s is None:
        both_text = isinstance(production, str) and isinstance(staging, str)
        return "whitespace" if both_text and _squash(production) == _squash(staging) else "other"
    if p is None or s is None:
        # One side is a list; the other is a scalar. Equal canonical values: the scalar is blank.
        return "empty_list_item"
    p_full, s_full = [i for i in p if i.strip()], [i for i in s if i.strip()]
    if (len(p_full) != len(p) or len(s_full) != len(s)) and set(p_full) == set(s_full):
        return "empty_list_item"
    if set(p_full) == set(s_full):
        return "list_order"
    return "whitespace" if {_squash(i) for i in p_full} == {_squash(i) for i in s_full} else "other"


def _group(df: pd.DataFrame, key: list[str]) -> dict:
    """Rows of ``df`` as dicts, grouped by canonical key (a tuple of canonical values)."""
    groups = {}
    for row in df.to_dict("records"):
        groups.setdefault(tuple(canon(row[c]) for c in key), []).append(row)
    return groups


def _key_text(k: tuple) -> str:
    return " | ".join(_show(v) for v in k)


def _combined(rows: list[dict], column: str):
    """(canonical value, raw value) of a column over the rows of one key.

    Several rows have no single raw value: the raw value is the combined one.
    """
    value = combine([r[column] for r in rows])
    if len(rows) == 1:
        return value, rows[0][column]
    return value, sorted(value) if isinstance(value, frozenset) else value


def diff_tables(resource: str, staging: pd.DataFrame, production: pd.DataFrame, key: list[str]) -> dict:
    """Compare two tables by key. Returns the report parts."""
    s_groups, p_groups = _group(staging, key), _group(production, key)
    columns = [c for c in staging.columns if c in production.columns and c not in key]
    added = [k for k in s_groups if k not in p_groups]
    removed = [k for k in p_groups if k not in s_groups]
    changed, formats = [], []
    for k in s_groups.keys() & p_groups.keys():
        for c in columns:
            (s_val, s_raw), (p_val, p_raw) = _combined(s_groups[k], c), _combined(p_groups[k], c)
            if s_val != p_val:
                changed.append({"key": _key_text(k), "column": c,
                                "production": _show(p_val), "staging": _show(s_val)})
            elif _show(p_raw) != _show(s_raw):
                formats.append({"resource": resource, "column": c, "kind": format_kind(p_raw, s_raw),
                                "example_key": _key_text(k), "example_production": _show(p_raw),
                                "example_staging": _show(s_raw)})
    return {
        "added": pd.DataFrame([r for k in added for r in s_groups[k]], columns=list(staging.columns)),
        "removed": pd.DataFrame([r for k in removed for r in p_groups[k]], columns=list(production.columns)),
        "n_added": len(added), "n_removed": len(removed),
        "changed": pd.DataFrame(sorted(changed, key=lambda d: (d["key"], d["column"])),
                                columns=["key", "column", "production", "staging"]),
        "formats": formats,
        "duplicates": [{"resource": resource, "key": _key_text(k), "row_count": len(r)}
                       for k, r in s_groups.items() if len(r) > 1],
        "combined": sum(len(r) > 1 for r in p_groups.values()),
    }


def same_rows(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    """True if both frames hold the same rows (any order, same count)."""
    return (len(a) == len(b) and list(a.columns) == list(b.columns)
            and Counter(_signature(r) for r in a.itertuples(index=False))
            == Counter(_signature(r) for r in b.itertuples(index=False)))


def promote(syn, resource: str, run_id: str, staging_df: pd.DataFrame) -> str:
    """Replace the production table of ``resource``; return the status."""
    staging_id, production_id = utils.portal_table(resource), utils.portal_table(resource, True)
    Table(id=staging_id).snapshot(
        comment=f"promote {run_id} to {production_id}", synapse_client=syn
    )
    utils.update_table(syn, production_id, staging_df, allow_production=True)
    back = read_table(syn, production_id)
    # update_table stores the rows with newlines replaced by spaces.
    return "promoted" if same_rows(utils.strip_newlines(staging_df), back) else "readback_mismatch"


def run(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tables", nargs="+", choices=RESOURCES, default=RESOURCES,
                        help="resources to promote (default: all)")
    parser.add_argument("--execute", action="store_true",
                        help="write the production tables (default: dry run)")
    parser.add_argument("--report-dir", default="./promote_report")
    parser.add_argument("--run-id", required=True,
                        help="label for snapshot comments and the report folder")
    args = parser.parse_args(argv)

    out_dir = Path(args.report_dir) / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    syn = utils.syn_login()

    # Step 1: schemas of every table, before anything is written.
    mismatches = []
    for resource in args.tables:
        staging = Table(id=utils.portal_table(resource)).get(include_columns=True, synapse_client=syn)
        production = Table(id=utils.portal_table(resource, True)).get(include_columns=True, synapse_client=syn)
        mismatches += schema_diff(resource, staging, production)
    if mismatches:
        pd.DataFrame(mismatches, columns=["table", "column", "attribute", "staging", "production"]
                     ).to_csv(out_dir / "schema_diff.csv", index=False)
        print(f"Schema differences found; see {out_dir / 'schema_diff.csv'}. No table was written.")
        return 1

    # Step 2: row diffs and summary.
    summary, frames, formats, duplicates = [], {}, [], []
    for resource in args.tables:
        staging_id, production_id = utils.portal_table(resource), utils.portal_table(resource, True)
        staging_df, production_df = read_table(syn, staging_id), read_table(syn, production_id)
        frames[resource] = staging_df
        key = KEY_COLUMNS[resource]
        result = diff_tables(resource, staging_df, production_df, key)
        for name in ("added", "removed", "changed"):
            if len(result[name]):
                result[name].to_csv(out_dir / f"{resource}_{name}.csv", index=False)
        formats += result["formats"]
        duplicates += result["duplicates"]
        summary.append({
            "resource": resource, "staging_id": staging_id, "production_id": production_id,
            "staging_rows": len(staging_df), "production_rows": len(production_df), "key": "+".join(key),
            "production_keys_combined": result["combined"], "staging_duplicate_keys": len(result["duplicates"]),
            "added": result["n_added"], "removed": result["n_removed"],
            "changed_records": result["changed"]["key"].nunique(), "changed_cells": len(result["changed"]),
            "format_only_cells": len(result["formats"]), "status": "pending" if args.execute else "dry_run",
        })
    format_df = pd.DataFrame(formats, columns=["resource", "column", "kind", "example_key",
                                               "example_production", "example_staging"])
    format_df.groupby(["resource", "column", "kind"], sort=False).agg(
        count=("kind", "size"), example_key=("example_key", "first"),
        example_production=("example_production", "first"), example_staging=("example_staging", "first"),
    ).reset_index().to_csv(out_dir / "format_changes.csv", index=False)
    if duplicates:
        pd.DataFrame(duplicates).to_csv(out_dir / "duplicate_keys.csv", index=False)

    # Step 3: promote, in the order given, stopping at the first failure.
    code = 0
    if args.execute:
        failed = False
        for row in summary:
            if failed:
                row["status"] = "not_run"
                continue
            try:
                row["status"] = promote(syn, row["resource"], args.run_id, frames[row["resource"]])
            except Exception as exc:
                print(f"{row['resource']}: promotion failed: {exc}")
                row["status"] = "failed"
            if row["status"] != "promoted":
                failed, code = True, 1

    summary_df = pd.DataFrame(summary, columns=SUMMARY_COLUMNS)
    summary_df.to_csv(out_dir / "summary.csv", index=False)
    print(summary_df.to_string(index=False))
    return code


if __name__ == "__main__":
    sys.exit(run())
