"""
repair_recordset.py

Repair a RecordSet that Synapse Grid can no longer open because a list cell (a
JSON-array column) in its stored CSV is invalid JSON. Grid stores a ``"`` or
``\\`` inside a list item badly (``M"ockl L`` becomes ``"M""ockl L"``), after
which Grid answers HTTP 500, so the normal Grid-based write path cannot fix it.
This is the other direct-store writer next to ``remove_recordset_rows.py``: it
downloads the latest version's stored CSV raw, finds every non-empty list cell
that fails ``json.loads`` (the broken cells), replaces those cells with
curator-supplied corrections, stores the result as a new version directly, and
opens one Grid export so Synapse re-validates it. The binding, upsert key and
CurationTask are kept.

``--fixes`` is a CSV with columns ``key``, ``column`` and ``value``: ``key`` is
the row's upsert-key value (for a multi-column key, the values in key order
joined with the unit separator ``\\x1f``, the key separator of
``portal_tables/recordsets.py``), ``column`` a list column of ``--component``,
and ``value`` the full corrected cell as a JSON array of strings, none
containing ``"`` or ``\\``. Every broken cell needs a fix; a fix for a cell that
is not broken is allowed (a curator may correct a neighbour) and is listed.

Steps: refuse while another run holds the write-log lock (as
``remove_recordset_rows.py`` does; an orphaned pending row is recovered the same
way); download the latest version; write ``broken_cells.csv``; validate the
fixes; build ``corrected.csv`` (the downloaded file with only the fixed cells
replaced) and ``changes.csv``, and check with ``differences`` that nothing else
differs. Dry run (the default) stops there. ``--execute``: write-log ``start``
(pending), store the new version, ``record`` it as a pipeline write (base = the
version read), one Grid export (each exported version recorded), ``finish``;
if it fails before a version exists the pending row is aborted. The final
version is then read back and compared with the corrected file.

The run id starts with ``dcc-repair-`` (generated; a ``--run-id`` without it is
rejected). ``--write-log-table`` defaults to the production table; a dry run reads
it and writes nothing to it.

Exit status: 0 ok (dry run complete, or repaired and the read-back matches);
1 locked (another run holds the lock), retry (the RecordSet moved before the
write), refused (no upsert key, broken cell without a fix, invalid fix,
unexpected difference), failed, or the read-back does not match.

author: orion.banks
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import recordsets
import utils

logger = logging.getLogger(__name__)

REPAIR_RUN_PREFIX = "dcc-repair-"
PROCESS_START = pd.Timestamp.now(tz="UTC")  # a pending row of our runId older than this is not ours


class Refused(Exception):
    """The repair cannot go ahead; nothing was written."""


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument("--recordset", required=True, help="RecordSet ID")
    parser.add_argument("--component", required=True, help="the RecordSet's component, e.g. PublicationView")
    parser.add_argument("--fixes", required=True, help="CSV with columns key, column, value (a JSON array)")
    recordsets.add_log_args(parser, removals=False)
    parser.add_argument("--report-dir", required=True, help="directory for the CSVs and summary")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument(
        "--run-id", default=recordsets.new_run_id(REPAIR_RUN_PREFIX),
        help=f"must start with {REPAIR_RUN_PREFIX!r}; default: that plus a timestamp",
    )
    args = parser.parse_args(argv)
    if not args.run_id.startswith(REPAIR_RUN_PREFIX):
        parser.error(f"--run-id must start with {REPAIR_RUN_PREFIX!r}")
    return args


def find_broken(rows, key_cols, list_cols) -> list[dict]:
    """Non-empty list cells that are not valid JSON: ``[{key, column, value}]``."""
    broken = []
    for row in rows:
        for column in sorted(list_cols & row.keys()):
            raw = row[column]
            if not raw.strip():
                continue
            try:
                json.loads(raw)
            except ValueError:
                broken.append({"key": recordsets._key_of(row, key_cols), "column": column, "value": raw})
    return broken


def load_fixes(path, list_cols) -> dict[tuple[str, str], str]:
    """``{(key, column): corrected cell text}`` from the fixes CSV; raises :class:`Refused` on an invalid fix."""
    df = recordsets.read_csv_str(path)
    missing = {"key", "column", "value"} - set(df.columns)
    if missing:
        raise Refused(f"{path} lacks column(s) {sorted(missing)}")
    fixes, problems = {}, []
    for _, f in df.iterrows():
        where = f"{f['key']!r} / {f['column']}"
        if f["column"] not in list_cols:
            problems.append(f"{where}: not a list column")
            continue
        if (f["key"], f["column"]) in fixes:
            problems.append(f"{where}: fixed twice")
            continue
        try:
            items = json.loads(f["value"])
        except ValueError:
            items = None
        if not isinstance(items, list) or not all(isinstance(i, str) for i in items):
            problems.append(f"{where}: value is not a JSON array of strings")
        elif any('"' in i or "\\" in i for i in items):
            problems.append(f"{where}: an item contains '\"' or a backslash")
        else:
            fixes[(f["key"], f["column"])] = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
    if problems:
        raise Refused("invalid fixes: " + "; ".join(problems))
    return fixes


def write_rows(path: Path, fieldnames, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def repair(syn, args, write_log, report_dir: Path, principal) -> dict:
    """Repair one RecordSet. Returns a summary dict; raises :class:`Refused` when it must not go ahead."""
    rs_id, run_id = args.recordset, args.run_id
    summary = {"recordset": rs_id, "status": "", "message": ""}

    locked = recordsets.lock_message(write_log, rs_id, run_id, PROCESS_START)
    if locked:
        summary.update(status="locked", message=locked)
        return summary
    latest, key_cols = recordsets.recordset_info(syn, rs_id)
    if not key_cols:
        raise Refused(f"{rs_id} has no upsert key")
    recordsets.recover_pending(
        syn, rs_id, write_log, latest, record=args.execute, skip_younger_than=recordsets.LOCK_HOURS)
    summary["base"] = latest

    report_dir.mkdir(parents=True, exist_ok=True)
    original = recordsets._download_version_file(syn, rs_id, latest, report_dir / f"v{latest}_original.csv")
    reader = csv.DictReader(io.StringIO(original.read_text(encoding="utf-8"), newline=""))
    fieldnames, rows = list(reader.fieldnames or []), list(reader)
    if any(c not in fieldnames for c in key_cols):
        raise Refused(f"v{latest} lacks key column(s) {key_cols}")
    list_cols = recordsets.list_columns(args.component)

    by_key: dict[str, dict] = {}
    for row in rows:
        k = recordsets._key_of(row, key_cols)
        if k in by_key:
            raise Refused(f"v{latest} has the key {k!r} twice")
        by_key[k] = row

    broken = find_broken(rows, key_cols, list_cols)
    pd.DataFrame(broken, columns=["key", "column", "value"]).rename(columns={"value": "raw_value"}).to_csv(
        report_dir / "broken_cells.csv", index=False)
    summary["broken"] = len(broken)

    fixes = load_fixes(args.fixes, list_cols)
    problems = [f"{k!r} / {c}: no such row" for (k, c) in fixes if k not in by_key]
    problems += [f"{k!r} / {c}: no such column in v{latest}" for (k, c) in fixes if c not in fieldnames]
    problems += [f"broken cell {b['key']!r} / {b['column']} has no fix"
                 for b in broken if (b["key"], b["column"]) not in fixes]
    if problems:
        raise Refused("; ".join(problems))
    broken_cells = {(b["key"], b["column"]) for b in broken}
    for k, c in fixes:
        if (k, c) not in broken_cells:
            logger.warning("%s: fix for %r / %s, which is not broken; applied", rs_id, k, c)
    summary["fixes"] = len(fixes)

    changes = []
    corrected_rows = [dict(r) for r in rows]
    for row in corrected_rows:
        k = recordsets._key_of(row, key_cols)
        for column in fieldnames:
            if (k, column) in fixes:
                changes.append({"key": k, "column": column, "old": row[column], "new": fixes[(k, column)]})
                row[column] = fixes[(k, column)]
    corrected = report_dir / "corrected.csv"
    write_rows(corrected, fieldnames, corrected_rows)
    pd.DataFrame(changes, columns=["key", "column", "old", "new"]).to_csv(report_dir / "changes.csv", index=False)

    # nothing but the fixed cells may differ from the downloaded file
    after = recordsets._load_csv(corrected, list_cols)
    diffs = recordsets.differences(after, recordsets._load_csv(original, list_cols), key_cols)
    allowed = tuple(f"{k} / {c}:" for k, c in fixes)
    stray = [d for d in diffs if not d.startswith(allowed)]
    if stray:
        raise Refused(f"the corrected file differs from v{latest} beyond the fixes: {stray[:5]}")

    if not args.execute:
        summary.update(status="dryrun", message=f"dry run: {len(broken)} broken cells, {len(fixes)} fixes; nothing written")
        return summary

    created: list[int] = []
    try:
        with recordsets.locked_write(syn, rs_id, write_log, run_id, principal, latest, retry_note="rerun"):
            version = recordsets.store_new_version(syn, rs_id, corrected)
            created.append(version)
            write_log.record(rs_id, version, run_id, len(rows), base=latest, principal=principal)
            summary["version"] = version

            def on_version(v):
                write_log.record(rs_id, v, run_id, len(rows), base=version, principal=principal)
                created.append(v)

            recordsets.export_via_grid(syn, rs_id, on_version=on_version)
    except recordsets.WriteRefused as err:  # locked or retry; nothing written
        summary.update(status=err.status, message=err.message)
        return summary

    final = recordsets.read_recordset(syn, rs_id, version=created[-1], list_cols=list_cols)
    mismatch = recordsets.differences(after, final[after.columns], key_cols)
    if mismatch:
        summary.update(status="mismatch", message=(
            f"stored v{version}, exported v{created[-1]}, but the final version differs from the corrected file: "
            f"{mismatch[:5]}"))
    else:
        summary.update(status="repaired", message=(
            f"repaired {len(fixes)} cells in v{version} (re-validated in v{created[-1]}); matches the corrected file"))
    return summary


def main(argv=None, syn=None):
    args = get_args(sys.argv[1:] if argv is None else argv)
    syn = syn or utils.syn_login()
    write_log, _ = recordsets.open_logs(syn, args)
    principal = recordsets.principal_id(syn) if args.execute else ""
    report_dir = Path(args.report_dir)
    print(f"Run {args.run_id}: {'EXECUTE' if args.execute else 'dry run'}, {args.recordset} ({args.component})")
    try:
        summary = repair(syn, args, write_log, report_dir, principal)
    except Refused as err:
        summary = {"recordset": args.recordset, "status": "refused", "message": str(err)}
    except Exception as err:
        logger.exception("RecordSet %s failed", args.recordset)
        summary = {"recordset": args.recordset, "status": "failed", "message": repr(err)}
    print(f"{summary['recordset']}: {summary['status']} {summary['message']}")
    report_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([summary]).to_csv(report_dir / "summary.csv", index=False)
    return 0 if summary["status"] in ("dryrun", "repaired") else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
