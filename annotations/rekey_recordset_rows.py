"""
rekey_recordset_rows.py

Change the upsert-key value (the record id, e.g. ``DatasetViewId`` or
``ToolViewId``) of listed rows in grant RecordSets, keeping every other cell.
Used to repoint dataset rows to new Synapse ids, and to rewrite ``ToolViewId``
to shared tool ids. A Grid import is an upsert, so it would add the new id as a
second row and keep the old one: a rekey is a direct store, like
``repair_recordset.py`` and ``remove_recordset_rows.py``. The binding, upsert
key and CurationTask are kept. One new version is stored per RecordSet.

``--changes`` is a CSV with columns ``recordset``, ``old_key`` and ``new_key``
(one row per change, many RecordSets allowed) and an optional ``reason``.

Steps, for each RecordSet in turn (one that fails or is refused does not stop
the others): refuse while another run holds the write-log lock (as
``repair_recordset.py`` does; an orphaned pending row is recovered the same
way); refuse an upsert key that is not exactly one column; download the latest
version raw; validate the changes. ``old_key`` must be a row. ``new_key`` must
be non-empty, differ from ``old_key``, not be the target of two changes, and not
be a key of the RecordSet already, unless that row is itself rekeyed away in
this run (so a swap, A to B while B to C, is allowed). A RecordSet with any
refused change is not written at all; the refused changes go to
``refused.csv``. Then build ``rekeyed.csv`` (the downloaded file with only the
key cells changed) and ``changes.csv``, and check with ``differences`` that,
with the keys put back, nothing else differs. Dry run (the default) stops there.
``--execute``: write-log ``start`` (pending), store the new version, log it
``removed`` (a removal version, D-Q49; base = the version read), one Grid export
(each exported version recorded, base = the stored version), ``finish``; if it
fails before a version exists the pending row is aborted. The stored version is
logged ``removed``, not ``written``, because replay then drops the old keys
(in its base, not in it) and adds the new keys as pipeline rows; a plain
pipeline version would keep the old keys as ``kept_unseen_row`` and the next
upload would write them back. Cells a contributor owned under an old key are
not carried to the new key (ownership is per key). Right after the store is logged (before the export, as
``remove_recordset_rows.py`` does), one ``removed`` row per old key is appended
to the removals table (``removedInVersion`` = the stored version; reason
``rekeyed to <new_key> (<run id>)``, plus the CSV ``reason``), so a later upload
of a manifest that still has the old id does not add it again. An old key that
is also a new key in the run (a chain or swap) is live again and gets no row,
and a ``new_key`` that the removals table lists as ``removed`` is refused. A
failure after the store is reported ``failed`` with the stored version. The final
version is read back and compared with ``rekeyed.csv``.

Files are in ``<report-dir>/<run-id>/<recordset>/``; ``summary.csv`` in
``<report-dir>/<run-id>/`` has one row per RecordSet: recordset, status (dryrun,
rekeyed, refused, locked, failed, readback_mismatch), changes, version, message.

The run id starts with ``dcc-rekey-`` (generated; a ``--run-id`` without it is
rejected). ``--write-log-table`` and ``--removals-table`` default to the production
tables; a dry run reads them and writes nothing to them.

Exit status: 0 ok (every RecordSet dry-run complete, or rekeyed and the
read-back matches); 1 if any RecordSet is refused, locked, failed, or its
read-back does not match.

Example:
    python rekey_recordset_rows.py --changes rekey.csv --component DatasetView \\
        --report-dir reports --execute

author: orion.banks
"""

import argparse
import csv
import io
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

REKEY_RUN_PREFIX = "dcc-rekey-"
PROCESS_START = pd.Timestamp.now(tz="UTC")  # a pending row of our runId older than this is not ours
OK_STATUSES = ("dryrun", "rekeyed")


class Refused(Exception):
    """The RecordSet cannot be rekeyed; nothing was written."""


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument("--changes", required=True, help="CSV with columns recordset, old_key, new_key[, reason]")
    parser.add_argument("--component", required=True, help="the RecordSets' component, e.g. DatasetView")
    recordsets.add_log_args(parser)
    parser.add_argument("--report-dir", required=True, help="directory for the CSVs and summary")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument(
        "--run-id", default=recordsets.new_run_id(REKEY_RUN_PREFIX),
        help=f"must start with {REKEY_RUN_PREFIX!r}; default: that plus a timestamp",
    )
    args = parser.parse_args(argv)
    if not args.run_id.startswith(REKEY_RUN_PREFIX):
        parser.error(f"--run-id must start with {REKEY_RUN_PREFIX!r}")
    return args


def load_changes(path) -> dict[str, list[dict]]:
    """``{recordset: [{old_key, new_key, reason}]}`` from the changes CSV, in file order."""
    df = recordsets.read_csv_str(path)
    missing = {"recordset", "old_key", "new_key"} - set(df.columns)
    if missing:
        raise SystemExit(f"{path} lacks column(s) {sorted(missing)}")
    if "reason" not in df.columns:
        df["reason"] = ""
    by_rs: dict[str, list[dict]] = {}
    for _, c in df.iterrows():
        by_rs.setdefault(c["recordset"].strip(), []).append(
            {"old_key": c["old_key"].strip(), "new_key": c["new_key"].strip(), "reason": c["reason"].strip()})
    return by_rs


def validate_changes(changes, keys: set[str], removed=frozenset()) -> list[dict]:
    """The refused changes (each with a ``reason``); empty when every change is valid."""
    old_keys = [c["old_key"] for c in changes]
    new_keys = [c["new_key"] for c in changes]
    refused = []
    for c in changes:
        old, new = c["old_key"], c["new_key"]
        if old not in keys:
            why = "old_key is not a row"
        elif old_keys.count(old) > 1:
            why = "old_key is rekeyed twice"
        elif not new:
            why = "new_key is empty"
        elif new == old:
            why = "new_key equals old_key"
        elif new_keys.count(new) > 1:
            why = "new_key is the target of two changes"
        elif new in keys and new not in old_keys:
            why = "new_key is already a row"
        elif new in removed:
            why = "new_key is a removed key"
        else:
            continue
        refused.append({**c, "reason": why})
    return refused


def write_rows(path: Path, fieldnames, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def rekey(syn, args, rs_id, changes, write_log, removals, report_dir: Path, principal) -> dict:
    """Rekey one RecordSet. Returns a summary dict; raises :class:`Refused` when it must not go ahead."""
    run_id = args.run_id
    summary = {"recordset": rs_id, "status": "", "changes": len(changes), "version": "", "message": ""}

    locker = write_log.locked_by(rs_id, run_id, since=PROCESS_START)
    if locker:
        summary.update(status="locked", message=(
            f"run {locker} has an unfinished write (pending row under {recordsets.LOCK_HOURS} h old)"))
        return summary
    latest, key_cols = recordsets.recordset_info(syn, rs_id)
    if len(key_cols) != 1:
        raise Refused(f"{rs_id} has {len(key_cols)} upsert key column(s) {key_cols}; need exactly one")
    key_col = key_cols[0]
    recordsets.recover_pending(
        syn, rs_id, write_log, latest, record=args.execute, skip_younger_than=recordsets.LOCK_HOURS)

    report_dir.mkdir(parents=True, exist_ok=True)
    original = recordsets._download_version_file(syn, rs_id, latest, report_dir / f"v{latest}_original.csv")
    reader = csv.DictReader(io.StringIO(original.read_text(encoding="utf-8"), newline=""))
    fieldnames, rows = list(reader.fieldnames or []), list(reader)
    if key_col not in fieldnames:
        raise Refused(f"v{latest} lacks key column {key_col}")
    keys = [row[key_col] for row in rows]
    if len(set(keys)) != len(keys):
        raise Refused(f"v{latest} has a key twice")
    list_cols = recordsets.list_columns(args.component)

    refused = validate_changes(changes, set(keys), set(removals.removed(rs_id)))
    if refused:
        pd.DataFrame([{"recordset": rs_id, **r} for r in refused],
                     columns=["recordset", "old_key", "new_key", "reason"]).to_csv(
            report_dir / "refused.csv", index=False)
        raise Refused(f"{len(refused)} of {len(changes)} changes refused (see refused.csv): "
                      + "; ".join(f"{r['old_key']!r} -> {r['new_key']!r}: {r['reason']}" for r in refused[:5]))

    new_of = {c["old_key"]: c["new_key"] for c in changes}
    rekeyed_rows = [dict(r) for r in rows]
    for row in rekeyed_rows:
        row[key_col] = new_of.get(row[key_col], row[key_col])
    rekeyed = report_dir / "rekeyed.csv"
    write_rows(rekeyed, fieldnames, rekeyed_rows)
    pd.DataFrame(changes, columns=["old_key", "new_key", "reason"]).to_csv(report_dir / "changes.csv", index=False)

    # nothing but the key cells may differ: put the old keys back in the rekeyed file and compare
    old_of = {v: k for k, v in new_of.items()}
    restored = report_dir / "rekeyed_restored.csv"
    write_rows(restored, fieldnames, [{**r, key_col: old_of.get(r[key_col], r[key_col])} for r in rekeyed_rows])
    after = recordsets._load_csv(rekeyed, list_cols)
    diffs = recordsets.differences(
        recordsets._load_csv(restored, list_cols), recordsets._load_csv(original, list_cols), key_cols)
    restored.unlink()
    if diffs:
        raise Refused(f"the rekeyed file differs from v{latest} beyond the key cells: {diffs[:5]}")

    if not args.execute:
        summary.update(status="dryrun", message=f"dry run: {len(changes)} keys would change; nothing written")
        return summary

    write_log.start(rs_id, latest, run_id, principal)
    created: list[int] = []
    try:
        winner = write_log.earlier_pending(rs_id, run_id)
        if winner:
            write_log.abort(rs_id, latest, run_id, principal)
            raise Refused(f"run {winner} started its write first")
        now_latest, _ = recordsets.recordset_info(syn, rs_id)
        if now_latest != latest:
            write_log.abort(rs_id, latest, run_id, principal)
            raise Refused(f"{rs_id} moved from v{latest} to v{now_latest} before the write; rerun")
        version = recordsets.store_new_version(syn, rs_id, rekeyed)
        created.append(version)
        write_log.removed(rs_id, version, latest, run_id, principal, len(rows))
        summary["version"] = version
        # a later upload of a manifest that still has the old id must not add it again; an old key that is
        # also a new key (a chain or swap) is live again and gets no row. Before the export, so a failed
        # export or read-back cannot leave the old keys without rows.
        reused = {c["new_key"] for c in changes}
        try:
            removals.append_many(rs_id, [
                {"key": c["old_key"], "status": "removed", "removed_in_version": version, "requested_by": principal,
                 "reason": f"rekeyed to {c['new_key']} ({run_id})" + (f"; {c['reason']}" if c["reason"] else "")}
                for c in changes if c["old_key"] not in reused], run_id, principal)  # one write: all keys or none
        except Exception as err:
            raise RuntimeError(f"the removals rows for the old keys are missing: {err!r}") from err

        def on_version(v):
            write_log.record(rs_id, v, run_id, len(rows), base=version, principal=principal)
            created.append(v)

        recordsets.export_via_grid(syn, rs_id, on_version=on_version)
        write_log.finish(rs_id, latest, run_id, principal)
    except Refused:
        raise
    except BaseException as err:
        try:
            newest, _ = recordsets.recordset_info(syn, rs_id)
            recordsets.recover_pending(syn, rs_id, write_log, newest, run_id=run_id)  # aborts when nothing was created
        except Exception:
            logger.exception("%s: could not resolve the pending write-log row", rs_id)
        if isinstance(err, Exception) and created:
            raise RuntimeError(f"v{created[0]} was stored, then: {err!r}") from err
        raise

    try:
        final = recordsets.read_recordset(syn, rs_id, version=created[-1], list_cols=list_cols)
    except Exception as err:
        raise RuntimeError(f"v{version} was stored, then the read-back failed: {err!r}") from err
    mismatch = recordsets.differences(after, final[after.columns], key_cols)
    if mismatch:
        summary.update(status="readback_mismatch", message=(
            f"stored v{version}, exported v{created[-1]}, but the final version differs from rekeyed.csv: "
            f"{mismatch[:5]}"))
    else:
        summary.update(status="rekeyed", message=(
            f"rekeyed {len(changes)} rows in v{version} (re-validated in v{created[-1]}); matches rekeyed.csv"))
    return summary


def main(argv=None, syn=None):
    args = get_args(sys.argv[1:] if argv is None else argv)
    by_rs = load_changes(args.changes)
    syn = syn or utils.syn_login()
    write_log, removals = recordsets.open_logs(syn, args)
    principal = recordsets.principal_id(syn) if args.execute else ""
    run_dir = Path(args.report_dir) / args.run_id
    print(f"Run {args.run_id}: {'EXECUTE' if args.execute else 'dry run'}, {len(by_rs)} RecordSets ({args.component})")

    summaries = []
    for rs_id, changes in by_rs.items():
        try:
            summary = rekey(syn, args, rs_id, changes, write_log, removals, run_dir / rs_id, principal)
        except Refused as err:
            summary = {"recordset": rs_id, "status": "refused", "changes": len(changes), "version": "",
                       "message": str(err)}
        except Exception as err:  # one RecordSet's failure must not stop the others
            logger.exception("RecordSet %s failed", rs_id)
            summary = {"recordset": rs_id, "status": "failed", "changes": len(changes), "version": "",
                       "message": repr(err)}
        print(f"{rs_id}: {summary['status']} {summary['message']}")
        summaries.append(summary)

    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(summaries, columns=["recordset", "status", "changes", "version", "message"]).to_csv(
        run_dir / "summary.csv", index=False)
    return 0 if all(s["status"] in OK_STATUSES for s in summaries) else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
