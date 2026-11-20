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
  2. Diff the rows (``<resource>_diff.csv``) and write ``summary.csv``. The
     rows are matched on a key column per resource. If the key is missing or
     not unique in either table, the whole row is compared instead. The count
     tables have no key column: their whole rows are compared.
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
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from synapseclient.models import Table

import utils

RESOURCES = [r for r, c in utils.CONFIG.items() if "staging_table" in c]

# Portal column used as the record key of each resource (from crosswalk.py).
# Tool, education, project and people have no id column: use the column that
# names the record (tool, project), or the one that identifies it (education
# alias is a synID; people synapseProfileId). The count tables have no key.
KEY_COLUMNS = {
    "publication": "pubMedId",
    "dataset": "datasetId",
    "grant": "grantNumber",
    "tool": "toolName",
    "education": "alias",
    "project": "projectName",
    "people": "synapseProfileId",
}

COLUMN_ATTRS = ["name", "column_type", "maximum_size", "maximum_list_length", "enum_values"]
SYSTEM_COLUMNS = ["ROW_ID", "ROW_VERSION", "ROW_ETAG"]
DIFF_COLUMNS = ["change", "key", "column", "staging", "production"]


def _cell(value):
    """Hashable, comparable form of a cell: nulls are blank, lists are tuples."""
    if isinstance(value, (list, tuple)):
        return tuple(_cell(v) for v in value)
    if value is None or (not isinstance(value, str) and pd.isna(value)):
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


def _usable_key(key: str, *frames) -> bool:
    for df in frames:
        if key not in df.columns:
            return False
        values = [_cell(v) for v in df[key]]
        if any(v == "" for v in values) or len(set(values)) != len(values):
            return False
    return True


def _signature(row) -> tuple:
    return tuple(_cell(v) for v in row)


def diff_rows(staging: pd.DataFrame, production: pd.DataFrame, key: str | None):
    """Return (diff rows, key used). Falls back to whole rows without a usable key."""
    out = []
    if key is not None and _usable_key(key, staging, production):
        s = {_cell(r[key]): r for _, r in staging.iterrows()}
        p = {_cell(r[key]): r for _, r in production.iterrows()}
        for k in s.keys() - p.keys():
            out.append({"change": "added", "key": k, "column": "", "staging": "", "production": ""})
        for k in p.keys() - s.keys():
            out.append({"change": "removed", "key": k, "column": "", "staging": "", "production": ""})
        for k in s.keys() & p.keys():
            for col in staging.columns:
                if _cell(s[k][col]) != _cell(p[k][col]):
                    out.append({"change": "changed", "key": k, "column": col,
                                "staging": s[k][col], "production": p[k][col]})
        return out, key
    s_rows = Counter(_signature(r) for r in staging.itertuples(index=False))
    p_rows = Counter(_signature(r) for r in production.itertuples(index=False))
    for sig in (s_rows - p_rows).elements():
        out.append({"change": "added", "key": " | ".join(map(str, sig)), "column": "", "staging": "", "production": ""})
    for sig in (p_rows - s_rows).elements():
        out.append({"change": "removed", "key": " | ".join(map(str, sig)), "column": "", "staging": "", "production": ""})
    return out, "whole row"


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
    summary, frames = [], {}
    for resource in args.tables:
        staging_id, production_id = utils.portal_table(resource), utils.portal_table(resource, True)
        staging_df, production_df = read_table(syn, staging_id), read_table(syn, production_id)
        frames[resource] = staging_df
        diff, key_used = diff_rows(staging_df, production_df, KEY_COLUMNS.get(resource))
        pd.DataFrame(diff, columns=DIFF_COLUMNS).to_csv(out_dir / f"{resource}_diff.csv", index=False)
        counts = Counter(d["change"] for d in diff)
        # A changed row can list several columns: count rows, not cells.
        changed_rows = len({d["key"] for d in diff if d["change"] == "changed"})
        summary.append({
            "resource": resource, "staging_id": staging_id, "production_id": production_id,
            "staging_rows": len(staging_df), "production_rows": len(production_df),
            "added": counts["added"], "removed": counts["removed"], "changed": changed_rows,
            "key_used": key_used, "status": "pending" if args.execute else "dry_run",
        })

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

    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(out_dir / "summary.csv", index=False)
    print(summary_df.to_string(index=False))
    for row in summary:
        if row["key_used"] == "whole row" and row["resource"] in KEY_COLUMNS:
            print(f"{row['resource']}: key column missing or not unique; compared whole rows.")
    return code


if __name__ == "__main__":
    sys.exit(run())
