"""Add Publications to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the publication manifest table to the Publications
portal table, by first truncating the table, then re-adding the rows.
"""

import re
from typing import List

import pandas as pd
from synapseclient import operations
from synapseclient.models import Table
import crosswalk
import model_contract
import utils

COMPONENT = "PublicationView"


def add_missing_info(
    pubs: pd.DataFrame, grants: pd.DataFrame, new_cols: List[str]
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    pubs["link"] = [
        "".join(["[PMID:", str(pmid), "](", url, ")"])
        for pmid, url in zip(pubs["PubmedId"], pubs["PubmedUrl"])
    ]

    pattern = re.compile(r"(')([\s\w/-]+)(')")

    for _, row in pubs.iterrows():
        datasets = row["PublicationDatasetAlias"]
        final_datasets = [d for d in datasets.split(", ") if d.startswith("SRX") is False]
        pubs.at[_, "PublicationDatasetAlias"] = ", ".join(final_datasets)

    for col in new_cols:
        pubs[col] = ""
        for i, row in pubs.iterrows():
            n = row["GrantViewKey"].split(",")
            extracted = []
            for g in n:
                if len(grants[grants.grantNumber == g][col].values) > 0:
                    values = str(grants[grants.grantNumber == g][col].values[0])

                    if col == "grantName":
                        extracted.append(values)
                    else:
                        matches = pattern.findall(values)
                        for m in matches:
                            extracted.append(m[1])
                else:
                    print(f"No match found for grant number: {g}")
                    continue

            clean_values = list(dict.fromkeys(extracted))
            pubs.at[i, col] = clean_values
    return pubs


def clean_table(df: pd.DataFrame, live_types: dict[str, str]) -> pd.DataFrame:
    """Sort and convert the table into the portal table's shape."""
    # Show most recent publications at the top of the table
    df = df.sort_values(by=["PublicationYear", "PubmedId"], ascending=False)
    return crosswalk.to_portal(df, COMPONENT, live_types)


def read_manifest(path: str) -> pd.DataFrame:
    """Read the manifest CSV with class-label headers (13.1.0 or 14.0.0)."""
    manifest = pd.read_csv(path, header=0).fillna("")
    manifest = model_contract.rename_legacy_headers(manifest, COMPONENT)
    # iconTags is an enrichment column that comes with the manifest.
    return model_contract.normalize_headers(manifest, to="keys", passthrough=["iconTags"])


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("publication")

    new_cols = ["theme", "consortium", "grantName"]

    if args.dryrun:
        print(
            "Inputs will be processed and provided for review",
            "\n\nDatabase will NOT be updated.",
        )

    manifest = read_manifest(operations.get(args.manifest_id, synapse_client=syn).path)
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("\nProcessing publications staging database...")
    grants = Table(id="syn21918972").query(
        query=f"SELECT grantNumber, {','.join(new_cols)} FROM syn21918972",
        synapse_client=syn,
    )

    database = add_missing_info(manifest, grants, new_cols)
    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, COMPONENT)
    final_database = clean_table(database, live_types)
    if args.verbose:
        print("\n🔍 Publication(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(syn, args.portal_table_id, final_database, component=COMPONENT)
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
