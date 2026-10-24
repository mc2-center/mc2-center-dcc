"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the grant manifest table to the Grant portal
table, by first truncating the table, then re-adding the rows.
"""

import sys

from synapseclient.models import EntityView
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


def main():
    """Main function. Returns the process exit code (1 if any grant failed)."""
    syn = utils.syn_login()
    args = utils.get_args("grant")
    
    # Info for table scope updates
    project_tables = "syn52750482"
    all_files_table = "syn27210848"
    table_list = [project_tables, all_files_table]

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    # Fail before any project is created if the live table drifted.
    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, COMPONENT)

    manifest = process_new_grants(
        args.manifest_id, args.portal_table_id, args.dryrun, synapse_client=syn,
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
