"""
remove_recordset_rows.py

The DCC row-removal step for shared RecordSets (D-Q49). A Grid import is an
upsert and cannot remove a row, so a real deletion is the one write that is not
a Grid session: for each RecordSet with ``requested`` keys in the removals
table, this stores a new version without those rows directly, then opens a
fresh Grid session with no import and one export, so Synapse re-validates it.
The binding, upsert key and CurationTask are kept.

Per RecordSet: take the write-log lock as ``write_logged`` does (a ``pending``
row, settled with ``earlier_pending``); refuse, writing nothing, while a Grid
session is open on it that the runner can see (``Grid.list`` may list only the
runner's own sessions: best effort, so a contributor's stale session can still
re-add a removed row, which replay reports as ``readded_removed_key``); read
the latest version; a requested key that is not in it is logged ``not_found``; store the rest-of-the-rows CSV as a new version, log it
``removed`` (base = the version read) and append ``removed`` rows to the
removals table (``removedInVersion`` = that version); export, log it
``written`` (base = the removal version); ``finish`` the pending row. The
pipeline then skips uploading those keys and replay drops them
(``portal_tables/recordsets.py``).

The run id starts with ``dcc-removal-`` (generated; a ``--run-id`` without it is
rejected). A run that dies between the store and the log is recovered by the
next run of either script (or, on a failure, at once) through
``recover_pending``: the stored version is a version above the pending row's
base, made by the same principal after it was written, and because the row's
runId has that prefix the first such version is logged ``removed`` (later ones
``recovered``). Replay needs nothing more (it reads the dropped keys off the
history); the removals table is then completed here: the keys that left the
RecordSet in a ``removed`` version with no removals rows are appended as
``removed`` rows (audit, and the upload's skip of ``restored`` keys).

A removal version owes a Grid export until a pipeline version is based on it
(``WriteLog.unexported_removals``). At the start of each run, per RecordSet and
under the lock, any owed export (a crash after the removals rows, before the
export) is made and logged ``written`` (base = the removal version), even when
nothing new is requested.

``remove_recordset_rows.py request --recordset syn... --key VALUE [VALUE...]
--reason TEXT`` appends ``requested`` rows (one ``--key`` per row, one value per
upsert key column, joined with the key separator); dry run unless ``--execute``.

Dry run is the default: it prints, per RecordSet, the keys that would be
removed, the keys not found and any blocking session or lock; nothing is
written. Use ``--execute`` to write; it needs ``--write-log-table`` and
``--removals-table`` (CSV versions are for dry runs and tests).

The exit status is 1 if any RecordSet needs attention (failed, locked, blocked
or retry).

author: orion.banks
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import recordsets
import utils

logger = logging.getLogger(__name__)

NEEDS_ATTENTION = frozenset({"failed", "locked", "blocked", "retry"})  # "revalidated" is ok
PROCESS_START = pd.Timestamp.now(tz="UTC")  # a pending row of our runId older than this is not ours


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    removals = parser.add_mutually_exclusive_group(required=True)
    removals.add_argument("--removals-table", help="Synapse ID of the removals table")
    removals.add_argument("--removals-csv", help="local CSV removals table (dry runs and tests only)")
    log = parser.add_mutually_exclusive_group(required=True)
    log.add_argument("--write-log-table", help="Synapse ID of the write-log table")
    log.add_argument("--write-log-csv", help="local CSV write log (dry runs and tests only)")
    parser.add_argument("--recordset", help="only this RecordSet ID")
    parser.add_argument("--report-dir", required=True, help="directory for the stored CSVs and summary")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument(
        "--cache-dir",
        default=str(recordsets.DEFAULT_CACHE_DIR),
        help="on-disk cache of downloaded RecordSet versions (immutable); default: %(default)s",
    )
    parser.add_argument(
        "--run-id", default=recordsets.REMOVAL_RUN_PREFIX + datetime.now().strftime("%Y%m%dT%H%M%S"),
        help=f"must start with {recordsets.REMOVAL_RUN_PREFIX!r}, which recover_pending recognises; default: that plus a timestamp",
    )
    args = parser.parse_args(argv)
    if not args.run_id.startswith(recordsets.REMOVAL_RUN_PREFIX):
        parser.error(f"--run-id must start with {recordsets.REMOVAL_RUN_PREFIX!r}")
    if args.execute and not (args.write_log_table and args.removals_table):
        parser.error("--execute needs --write-log-table and --removals-table; CSVs are for dry runs and tests only")
    return args


def _keys_of(df: pd.DataFrame, key_cols: list[str]) -> list[str]:
    return [recordsets._key_of(row, key_cols) for _, row in df.iterrows()]


def claim_removed(syn, rs_id, removals, versions, requested, key_cols, cache_dir, run_id, principal) -> dict[str, int]:
    """Append ``removed`` rows for requested keys that left the RecordSet in ``versions``.

    Used for versions claimed from a crashed removal (see the module
    docstring): a requested key present in ``v - 1`` and absent from ``v`` was
    removed in ``v``. Returns ``{key: version}``.
    """
    claimed: dict[str, int] = {}
    entries = []
    for v in sorted(versions):
        if v < 2:
            continue
        before = set(_keys_of(recordsets.read_recordset(syn, rs_id, v - 1, cache_dir), key_cols))
        after = set(_keys_of(recordsets.read_recordset(syn, rs_id, v, cache_dir), key_cols))
        for k in sorted(set(requested) & before - after - set(claimed)):
            claimed[k] = v
            info = requested[k]
            entries.append({"key": k, "status": "removed", "reason": info["reason"],
                            "requested_by": info["requestedBy"], "removed_in_version": v})
    removals.append_many(rs_id, entries, run_id, principal)  # one write: all or none
    return claimed


def claim_unclaimed(syn, rs_id, removals, write_log, recovered, requested, key_cols, cache_dir, run_id, principal):
    """:func:`claim_removed` over ``removed`` versions (logged, or just recovered) that have no removals rows."""
    unclaimed = set(getattr(recovered, "removal", ())) | (
        write_log.removal_versions(rs_id) - set(removals.removal_events(rs_id)))
    if not unclaimed:
        return {}
    return claim_removed(syn, rs_id, removals, unclaimed, requested, key_cols, cache_dir, run_id, principal)


def _under_lock_checks(syn, rs_id, write_log, run_id, principal, base):
    """After ``start``: ``(status, message)`` of a refusal (pending row aborted), or None to go ahead."""
    refusal = None
    winner = write_log.earlier_pending(rs_id, run_id)
    if winner:
        refusal = ("locked", f"run {winner} started its write first; skipped")
    elif recordsets.open_grid_sessions(syn, rs_id):  # one may have opened since the first check
        refusal = ("blocked", "a Grid session was opened meanwhile (visible to the runner; best effort); nothing written")
    else:
        now_latest, _ = recordsets.recordset_info(syn, rs_id)
        if now_latest != base:
            refusal = ("retry", (f"{rs_id} moved from v{base} to v{now_latest} before the write; "
                                 "nothing written, the next run retries"))
    if refusal:
        write_log.abort(rs_id, base, run_id, principal)
    return refusal


def _revalidate(syn, rs_id, write_log, run_id, principal, latest, owed):
    """Export the Grid session a crashed removal never got to (logged ``written``, base = the removal version).

    Returns a refusal ``(status, message)`` or None when done.
    """
    run_id += recordsets.REVALIDATE_RUN_SUFFIX  # marks the pending row as export-only for recover_pending
    write_log.start(rs_id, latest, run_id, principal)
    try:
        refusal = _under_lock_checks(syn, rs_id, write_log, run_id, principal, latest)
        if refusal:
            return refusal
        recordsets.export_via_grid(
            syn, rs_id, on_version=lambda v: write_log.record(rs_id, v, run_id, None, base=max(owed), principal=principal))
        write_log.finish(rs_id, latest, run_id, principal)
    except BaseException:
        try:
            newest, _ = recordsets.recordset_info(syn, rs_id)
            recordsets.recover_pending(syn, rs_id, write_log, newest, run_id=run_id)
        except Exception:
            logger.exception("%s: could not resolve the pending write-log row", rs_id)
        raise
    return None


def process_recordset(syn, rs_id, removals, write_log, args, report_dir: Path, principal) -> dict:
    """Remove one RecordSet's requested keys. Returns a summary dict with a ``status``."""
    summary = {"recordset": rs_id, "status": "", "message": ""}
    requested = removals.requested(rs_id)
    if not requested and not write_log.unexported_removals(rs_id):
        summary.update(status="nothing_to_remove", message="no requested keys")
        return summary

    locker = write_log.locked_by(rs_id, args.run_id, since=PROCESS_START)
    if locker:
        summary.update(status="locked", message=(
            f"run {locker} has an unfinished write (pending row under {recordsets.LOCK_HOURS} h old); skipped"))
        return summary
    latest, key_cols = recordsets.recordset_info(syn, rs_id)
    if not key_cols:
        summary.update(status="failed", message=f"{rs_id} has no upsert key")
        return summary

    # A removal that died after its store (or after its log) is claimed first.
    recovered = recordsets.recover_pending(
        syn, rs_id, write_log, latest, record=args.execute, skip_younger_than=recordsets.LOCK_HOURS
    )
    claimed = claim_unclaimed(syn, rs_id, removals if args.execute else _ReadOnly(removals), write_log, recovered,
                              requested, key_cols, args.cache_dir, args.run_id, principal)
    if claimed:
        summary["claimed"] = claimed
        requested = {k: v for k, v in requested.items() if k not in claimed}
        logger.warning("%s: claimed keys removed by an earlier crashed run: %s", rs_id, claimed)
    # A removal version owes a Grid export (re-validation) until a pipeline version is based on it.
    owed = sorted(set(write_log.unexported_removals(rs_id)) | set(recovered.removal))
    if not requested and not owed:
        summary.update(status="nothing_to_remove", message="every requested key was removed by an earlier run")
        return summary

    sessions = recordsets.open_grid_sessions(syn, rs_id)
    if sessions:
        ids = [getattr(s, "session_id", "?") for s in sessions]
        summary.update(status="blocked", message=(
            f"open Grid session(s) {ids} visible to the runner (best effort: others' sessions may not be listed); "
            f"close them first, an export from an older session brings the rows back; "
            f"{len(requested)} key(s) wait"))
        return summary

    if owed:
        summary["owed_export"] = owed
        if not args.execute:
            summary["message"] = f"would re-validate removal version(s) {owed} (Grid export); "
        else:
            refusal = _revalidate(syn, rs_id, write_log, args.run_id, principal, latest, owed)
            if refusal:
                summary.update(status=refusal[0], message=refusal[1])
                return summary
            latest, _ = recordsets.recordset_info(syn, rs_id)
            if not requested:
                summary.update(status="revalidated", message=f"re-validated removal version(s) {owed} (Grid export)")
                return summary
        if not requested:
            summary.update(status="dryrun", message=summary["message"] + "nothing written")
            return summary

    current = recordsets.read_recordset(syn, rs_id, latest, args.cache_dir)  # JSON-array cells stay text, lossless
    if any(c not in current.columns for c in key_cols):
        summary.update(status="failed", message=f"v{latest} lacks key column(s) {key_cols}")
        return summary
    present = set(_keys_of(current, key_cols))
    to_remove = [k for k in requested if k in present]
    not_found = [k for k in requested if k not in present]
    summary.update(remove=len(to_remove), not_found=len(not_found), base=latest)
    if not args.execute:
        summary.update(status="dryrun", message=(
            summary["message"] + f"dry run: would remove {to_remove}; not found {not_found}; nothing written"))
        return summary

    run_id = args.run_id
    write_log.start(rs_id, latest, run_id, principal)
    created: list[int] = []
    try:
        refusal = _under_lock_checks(syn, rs_id, write_log, run_id, principal, latest)
        if refusal:
            summary.update(status=refusal[0], message=refusal[1])
            return summary
        removals.append_many(rs_id, [
            {"key": k, "status": "not_found", "reason": requested[k]["reason"],
             "requested_by": requested[k]["requestedBy"]} for k in not_found], run_id, principal)
        if not to_remove:
            write_log.abort(rs_id, latest, run_id, principal)
            summary.update(status="nothing_to_remove", message=f"none of the requested keys is in v{latest}")
            return summary

        remaining = current[[k not in set(to_remove) for k in _keys_of(current, key_cols)]]
        report_dir.mkdir(parents=True, exist_ok=True)
        path = report_dir / "removal.csv"
        recordsets.to_csv(remaining, path)
        version = recordsets.store_new_version(syn, rs_id, path)
        created.append(version)
        raced = version != latest + 1
        if raced:
            logger.warning(
                "%s: v%s landed between the version check and the store, so the removal is v%s: a concurrent export "
                "raced it and the next upload will restore its changes", rs_id, latest + 1, version)
            summary["raced"] = True
        write_log.removed(rs_id, version, latest, run_id, principal, len(remaining))
        removals.append_many(rs_id, [
            {"key": k, "status": "removed", "reason": requested[k]["reason"],
             "requested_by": requested[k]["requestedBy"], "removed_in_version": version}
            for k in to_remove], run_id, principal)  # one write: all keys or none
        summary["version"] = version

        def on_version(v):
            # base = the removal version; if a foreign version raced the store, the removal's own base,
            # so replay keeps that version's edits (kept_unseen_edit) instead of treating them as seen
            write_log.record(rs_id, v, run_id, len(remaining), base=latest if raced else version, principal=principal)
            created.append(v)

        recordsets.export_via_grid(syn, rs_id, on_version=on_version)
        write_log.finish(rs_id, latest, run_id, principal)
    except BaseException:
        try:
            newest, _ = recordsets.recordset_info(syn, rs_id)
            recovered = recordsets.recover_pending(syn, rs_id, write_log, newest, run_id=run_id)
            claim_unclaimed(syn, rs_id, removals, write_log, recovered, requested, key_cols, args.cache_dir,
                            run_id, principal)
        except Exception:
            logger.exception("%s: could not resolve the pending write-log row", rs_id)
        raise
    summary.update(status="removed", message=(
        f"removed {to_remove} in v{version} (re-validated in v{created[-1]}); not found {not_found}"))
    return summary


class _ReadOnly:
    """The removals table in a dry run: reads pass through, appends are dropped."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def append_many(self, *args, **kwargs):
        pass


def summary_line(s: dict) -> str:
    parts = [f"{s['recordset']}: {s['status']}"]
    for field in ("base", "version", "remove", "not_found", "claimed"):
        if field in s:
            parts.append(f"{field}={s[field]}")
    if s["message"]:
        parts.append(s["message"])
    return " ".join(parts)


def get_request_args(argv):
    parser = argparse.ArgumentParser(
        prog="remove_recordset_rows.py request", description="Request row removals: append `requested` rows")
    removals = parser.add_mutually_exclusive_group(required=True)
    removals.add_argument("--removals-table", help="Synapse ID of the removals table")
    removals.add_argument("--removals-csv", help="local CSV removals table (dry runs and tests only)")
    parser.add_argument("--recordset", required=True, help="RecordSet ID")
    parser.add_argument(
        "--key", nargs="+", action="append", required=True, metavar="VALUE",
        help="one row to remove: its value for each upsert key column, in key order (repeat --key per row)")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--requested-by", default="", help="default: the runner's Synapse user ID (--execute)")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    args = parser.parse_args(argv)
    if args.execute and not args.removals_table:
        parser.error("--execute needs --removals-table; a CSV is for dry runs and tests only")
    return args


def request_main(argv, syn=None):
    """``request``: append ``requested`` rows (one write) for the given keys; dry run by default."""
    args = get_request_args(argv)
    syn = syn or utils.syn_login()
    removals = (
        recordsets.SynapseTableRemovals(syn, args.removals_table)
        if args.removals_table
        else recordsets.CsvRemovals(args.removals_csv)
    )
    _, key_cols = recordsets.recordset_info(syn, args.recordset)
    if not key_cols:
        print(f"{args.recordset} has no upsert key")
        return 1
    keys = []
    for parts in args.key:
        if len(parts) != len(key_cols):
            print(f"--key needs {len(key_cols)} value(s), one per key column {key_cols}; got {parts}")
            return 1
        keys.append(recordsets.KEY_SEP.join(p.strip() for p in parts))
    pending = set(removals.requested(args.recordset))
    gone = removals.removed(args.recordset)
    new = [k for k in dict.fromkeys(keys) if k not in pending and k not in gone]
    for k in keys:
        if k in pending:
            print(f"already requested, skipped: {k!r}")
        elif k in gone:
            print(f"already removed (v{gone[k]}), nothing to do: {k!r}")
    print(f"{'EXECUTE' if args.execute else 'dry run'}: {args.recordset} request {len(new)} key(s) {new}")
    if args.execute and new:
        principal = recordsets.principal_id(syn)
        removals.append_many(args.recordset, [
            {"key": k, "status": "requested", "reason": args.reason, "requested_by": args.requested_by or principal}
            for k in new], principal=principal)
    return 0


def main(argv=None, syn=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "request":
        return request_main(argv[1:], syn)
    args = get_args(argv)
    syn = syn or utils.syn_login()
    write_log = (
        recordsets.SynapseTableWriteLog(syn, args.write_log_table)
        if args.write_log_table
        else recordsets.CsvWriteLog(args.write_log_csv)
    )
    removals = (
        recordsets.SynapseTableRemovals(syn, args.removals_table)
        if args.removals_table
        else recordsets.CsvRemovals(args.removals_csv)
    )
    ids = [args.recordset] if args.recordset else removals.recordset_ids()
    principal = recordsets.principal_id(syn) if args.execute else ""
    run_dir = Path(args.report_dir) / args.run_id
    print(f"Run {args.run_id}: {'EXECUTE' if args.execute else 'dry run'}, {len(ids)} RecordSets")

    summaries, failed = [], 0
    for rs_id in ids:
        try:
            summary = process_recordset(syn, rs_id, removals, write_log, args, run_dir / rs_id, principal)
        except Exception as err:  # one RecordSet's failure must not stop the others
            logger.exception("RecordSet %s failed", rs_id)
            summary = {"recordset": rs_id, "status": "failed", "message": repr(err)}
        if summary["message"] == "no requested keys":
            continue  # only resolved keys: not worth a line
        if summary["status"] in NEEDS_ATTENTION:
            failed += 1
        print(summary_line(summary))
        summaries.append(summary)

    if summaries:
        run_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(summaries).to_csv(run_dir / "summary.csv", index=False)
    print(f"{len(summaries) - failed} of {len(summaries)} RecordSets ok; {failed} need attention")
    return 1 if failed else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
