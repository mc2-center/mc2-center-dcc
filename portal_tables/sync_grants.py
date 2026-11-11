"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the grant manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import sys

from synapseclient.models import EntityView, Table
import crosswalk
import utils
from create_grant_projects import COMPONENT, process_new_grants


def update_scopes(syn, table_ids, grant_ids):
    """Add each new grant project to the scope of every view in ``table_ids``.

    Each view is compared against the full list of grant projects, so a
    grant already in one view's scope is still added to the others. IDs of
    projects that were not created (``"None"``) are skipped.
    """
    wanted = {g for g in grant_ids if isinstance(g, str) and g.startswith("syn")}
    for table in table_ids:
        view = EntityView(id=table).get(synapse_client=syn)
        missing = wanted - set(view.scope_ids)
        if missing:
            view.scope_ids = set(view.scope_ids) | missing
            view.store(synapse_client=syn)
            print(f"Scope updated for table: {view.name} (+{len(missing)})")
        else:
            print(f"Scope already up to date for table: {view.name}")


def check_grant_ids(table):
    """Raise if any row has no real project: ``"None"``, empty or missing grantId.

    Such a row would be written to the live table and never retried.
    """
    ids = table["grantId"].astype(str).str.strip()
    bad = table.loc[ids.isin(["None", "nan", "<NA>", ""]), "grantViewId"].to_list()
    if bad:
        raise ValueError(
            f"Refusing to write grants without a project (grantId None or empty): {bad}"
        )


def check_portal_grant_ids(syn, table_id):
    """``check_grant_ids`` on the live portal table (read-only)."""
    check_grant_ids(
        Table(id=table_id).query(
            query=f"SELECT grantId, grantViewId FROM {table_id}",
            include_row_id_and_row_version=False,
            synapse_client=syn,
        )
    )


def main():
    """Main function. Returns the process exit code (1 if any grant failed)."""
    syn = utils.syn_login()
    args = utils.get_args("grant")
    if isinstance(getattr(args, "manifest_csv", None), str):
        raise SystemExit("--manifest-csv is not supported for the grant sync")

    # Info for table scope updates
    project_tables = "syn52750482"
    all_files_table = "syn27210848"
    table_list = [project_tables, all_files_table]

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    # Fail before any project is created if the live table drifted.
    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, COMPONENT)

    # A live row with no project would be rewritten by the sync and never
    # retried; fail before process_new_grants creates any project or team
    # (also on a dry run).
    check_portal_grant_ids(syn, args.portal_table_id)

    manifest_id = utils.resolve_manifest_id(syn, args, "grant")
    manifest = process_new_grants(
        manifest_id, args.portal_table_id, args.dryrun, synapse_client=syn,
        live_types=live_types,
    )
    failed = manifest.attrs.get("failed_grants", [])
    
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing grant staging database...")
    final_database = crosswalk.to_portal(manifest, COMPONENT, live_types)

    if args.verbose:
        print("\n🔍 Grant(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        check_grant_ids(final_database)
        update_scopes(syn, table_list, final_database.grantId.to_list())
        utils.update_table(syn, args.portal_table_id, final_database, component=COMPONENT)
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    if failed:
        print(f"\n\nFAILED ❌ {len(failed)} grant(s) were not created and not written: {failed}")
        return 1
    print("\n\nDONE ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
