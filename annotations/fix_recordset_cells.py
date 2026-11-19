"""
fix_recordset_cells.py

Change single cells of rows in grant RecordSets from a CSV, without code.
The script builds one upload file per grant and then runs
``upload_recordsets.py`` on it, so the normal upload rules apply (write log,
contributor-owned cells, read-back check).

``--changes`` is a CSV with these columns, one cell per line:
``component``, ``grant``, ``key``, ``column``, ``new_value``.

- ``component``: ``PublicationView``, ``DatasetView``, ``ToolView`` or
  ``EducationalResource``. One run takes one component.
- ``grant``: one grant number, for example ``CA209891``. A row that lists
  several grants lives in the RecordSet of each grant. To fix such a row,
  add one line for each grant.
- ``key``: the key value of the row, for example a PMID (PublicationView) or
  a Synapse ID (DatasetView, ToolView).
- ``column``: the display name or the RecordSet key name of the column.
- ``new_value``: the full new cell value. Write a list cell as a comma
  separated list. The script changes whole cells only.

Steps: find the grant project in the staging Grants table, find its component
folder and RecordSet, read the latest version. Check every line. A line is
refused when its grant, folder or RecordSet is not found, its key is not a row,
its column is unknown, its column is the key column (use
``rekey_recordset_rows.py``), two lines set one cell to different values, or
the value fails the JSON Schema of the column. If any line is refused, the
script writes ``refused.csv`` and nothing else, and it exits with 1.

Then, for each grant, the script writes an upload file with the key column and
the changed columns only (display-name headers). A cell of those columns that
the changes file does not set keeps its current value, because a blank cell
would clear the stored value. The script writes also ``paths.csv`` (columns
``File Paths``, the folder column of the component and ``grantId``) and
``cells.csv`` (grant, recordset, key, column, current, new) and
``overwrite.csv`` (grantId, key, column). Then it calls ``upload_recordsets.py``
with ``--overwrite-csv``: exactly the cells in the changes file may replace a
value that a curator edited in Curator (a cell that a curator owns is kept by
the standard upload). Every other curator-owned cell still wins. The replaced
values are in ``<grant>/overwritten.csv`` of the upload report. Dry run is the default; ``--execute`` writes to
Synapse.

Files are in ``<report-dir>/<run-id>/fix_cells/``; the upload reports are in
``<report-dir>/<run-id>/``.

Example:
    python fix_recordset_cells.py --changes fixes.csv --report-dir reports
    python fix_recordset_cells.py --changes fixes.csv --report-dir reports --execute

author: orion.banks
"""

import argparse
import importlib.util
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import jsonschema
import pandas as pd

# removed when the mc2dcc package (CLI consolidation plan) lands
HERE = Path(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, str(HERE / ".." / "portal_tables"))
import merge_tables
import model_contract
import recordsets
import schema_validation
import utils

logger = logging.getLogger(__name__)

COMPONENTS = ("PublicationView", "DatasetView", "ToolView", "EducationalResource")
FOLDER_COLUMNS = {  # as gen-mp-csv.py writes them; upload_recordsets.py reads the one folderId* column
    "PublicationView": "folderIdPublication",
    "DatasetView": "folderIdDatasets",
    "ToolView": "folderIdTools",
    "EducationalResource": "folderIdEducation",
}
RUN_PREFIX = "dcc-fixcells-"
CHANGE_COLUMNS = ["component", "grant", "key", "column", "new_value"]


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument("--changes", required=True, help=f"CSV with columns {', '.join(CHANGE_COLUMNS)}")
    parser.add_argument("--write-log-table", default="syn78404341", help="Synapse ID of the write-log table")
    parser.add_argument("--removals-table", default="syn78404342", help="Synapse ID of the removals table")
    parser.add_argument("--report-dir", required=True, help="directory for the files and reports")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument(
        "--run-id", default=RUN_PREFIX + datetime.now().strftime("%Y%m%dT%H%M%S"),
        help=f"default: {RUN_PREFIX!r} plus a timestamp",
    )
    return parser.parse_args(argv)


def load_changes(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in CHANGE_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"{path} lacks column(s) {missing}")
    df = df[CHANGE_COLUMNS].copy()
    for c in ("component", "grant", "key", "column"):
        df[c] = df[c].str.strip()
    return df


def grant_projects(syn) -> dict[str, str]:
    """``{grantNumber: grant project ID}`` from the staging Grants table."""
    from synapseclient.models import query
    table = utils.portal_table("grant")
    df = query(f"SELECT grantNumber, grantId FROM {table}", synapse_client=syn)
    return dict(zip(df["grantNumber"], df["grantId"]))


def find_folder(syn, project: str, component: str):
    from synapseclient.operations import find_entity_id
    return find_entity_id(name=merge_tables.folder_name(project, component), parent=project, synapse_client=syn)


def resolve_column(name: str, columns) -> str | None:
    """The RecordSet column for a display name or a key name; None when unknown."""
    if name in columns:
        return name
    try:
        label = model_contract.normalize_headers(pd.DataFrame(columns=[name]), to="keys").columns[0]
    except ValueError:
        return None
    return label if label in columns else None


def schema_problem(component: str, column: str, value: str) -> str | None:
    """Why the schema rejects ``value`` for ``column``; None when it passes."""
    schema = schema_validation.load_schema(component)
    parsed = recordsets.parse_upload(pd.DataFrame({column: [value]}), recordsets.list_columns(component))
    record = schema_validation._instance({column: parsed[column].iloc[0]}, schema["properties"])
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema, format_checker=schema_validation._format_checker())
    errors = [e for e in validator.iter_errors(record) if e.path and str(e.path[0]) == column]
    return errors[0].message if errors else None


def join_list(value):
    return ", ".join(value) if isinstance(value, list) else value


def plan(syn, changes: pd.DataFrame, component: str):
    """Check the changes. Returns ``(grants, refused)``.

    ``grants`` is ``{grant: {"project", "folder", "recordset", "key", "frame", "cells"}}``
    where ``frame`` is the upload frame (key column and changed columns, key names)
    and ``cells`` the report rows. ``refused`` is a list of changes with a ``reason``.
    """
    refused, grants = [], {}
    list_cols = recordsets.list_columns(component)
    projects = grant_projects(syn)

    def refuse(row, why):
        refused.append({**row, "reason": why})

    for grant, grp in changes.groupby("grant", sort=False):
        rows = grp.to_dict("records")
        project = projects.get(grant)
        if project is None:
            for r in rows:
                refuse(r, "grant is not in the staging Grants table")
            continue
        folder = find_folder(syn, project, component)
        rs_id = recordsets.find_recordset(syn, folder, component) if folder else None
        if rs_id is None:
            for r in rows:
                refuse(r, f"no {component} folder or RecordSet in {project}")
            continue
        _, key_cols = recordsets.recordset_info(syn, rs_id)
        if len(key_cols) != 1:
            for r in rows:
                refuse(r, f"{rs_id} upsert key is not exactly one column: {key_cols}")
            continue
        key_col = key_cols[0]
        current = recordsets.read_recordset(syn, rs_id, list_cols=list_cols)
        current = current.drop_duplicates(subset=key_col, keep="last").set_index(key_col)
        edits, cells = {}, []
        for r in rows:
            column = resolve_column(r["column"], set(current.columns) | {key_col})
            if column is None:
                refuse(r, "unknown column")
            elif column == key_col:
                refuse(r, "key column; use rekey_recordset_rows.py")
            elif r["key"] not in current.index:
                refuse(r, "key is not a row of the RecordSet")
            elif (r["key"], column) in edits and edits[(r["key"], column)] != r["new_value"]:
                refuse(r, "the cell is set twice to different values")
            elif (why := schema_problem(component, column, r["new_value"])):
                refuse(r, f"value fails the schema: {why}")
            else:
                if (r["key"], column) not in edits:
                    cells.append({"grant": grant, "recordset": rs_id, "key": r["key"], "column": column,
                                  "current": join_list(current.at[r["key"], column]), "new": r["new_value"]})
                edits[(r["key"], column)] = r["new_value"]
        if not edits:
            continue
        columns = list(dict.fromkeys(c for _, c in edits))
        keys = list(dict.fromkeys(k for k, _ in edits))
        frame = current.loc[keys, columns].copy().astype(object)  # a blank upload cell would clear the stored value
        for c in columns:
            frame[c] = frame[c].map(join_list)
        for (k, c), v in edits.items():
            frame.at[k, c] = v
        grants[grant] = {"project": project, "folder": folder, "recordset": rs_id, "key": key_col,
                         "frame": frame.reset_index(), "cells": cells}
    return grants, refused


def run_upload(argv, syn):
    """Run ``upload_recordsets.py`` in-process, as ``upload_workflow.py`` does."""
    spec = importlib.util.spec_from_file_location("upload_recordsets_cmd", HERE / "upload_recordsets.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.main(argv, syn=syn)


def main(argv=None, syn=None):
    args = get_args(argv)
    changes = load_changes(args.changes)
    bad = sorted(set(changes["component"]) - set(COMPONENTS))
    if bad or changes["component"].nunique() != 1:
        raise SystemExit(f"one run needs one component of {list(COMPONENTS)}; found {sorted(set(changes['component']))}")
    component = changes["component"].iloc[0]
    syn = syn or utils.syn_login()
    out = Path(args.report_dir) / args.run_id / "fix_cells"
    out.mkdir(parents=True, exist_ok=True)

    grants, refused = plan(syn, changes, component)
    if refused:
        pd.DataFrame(refused).to_csv(out / "refused.csv", index=False)
        print(f"{len(refused)} change(s) refused, nothing written (see {out / 'refused.csv'}):")
        print(pd.DataFrame(refused)[["grant", "key", "column", "reason"]].to_string(index=False))
        return 1

    paths, cells = [], []
    for grant, g in grants.items():
        path = out / "files" / f"{grant}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        model_contract.normalize_headers(g["frame"], to="display").to_csv(path, index=False)
        paths.append({"File Paths": str(path), FOLDER_COLUMNS[component]: g["folder"], "grantId": g["project"]})
        cells += g["cells"]
    pd.DataFrame(paths).to_csv(out / "paths.csv", index=False)
    cells_df = pd.DataFrame(cells)
    cells_df.to_csv(out / "cells.csv", index=False)
    # Every line of the changes file is a deliberate overwrite, also of a curator-owned cell.
    pd.DataFrame([{"grantId": g["project"], "key": c["key"], "column": c["column"]}
                  for g in grants.values() for c in g["cells"]]).to_csv(out / "overwrite.csv", index=False)
    print(cells_df.to_string(index=False))
    print(f"{len(cells_df)} cell(s) in {len(grants)} grant(s); files in {out}")

    upload_argv = ["--paths-csv", str(out / "paths.csv"), "--component", component,
                   "--write-log-table", args.write_log_table, "--removals-table", args.removals_table,
                   "--overwrite-csv", str(out / "overwrite.csv"),
                   "--report-dir", args.report_dir, "--run-id", args.run_id]
    if args.execute:
        upload_argv.append("--execute")
    return run_upload(upload_argv, syn) or 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
