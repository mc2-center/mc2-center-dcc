"""Add Educational Resources to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the Educational Resource manifest table to the Educational Resource portal
table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import re
import crosswalk
import model_contract
import utils

def add_missing_info(
    education: pd.DataFrame) -> pd.DataFrame:
    """Add missing information into table before syncing."""

    url_pattern = re.compile(r".*(synapse\.org).*")
    alias_pattern = re.compile(r"^syn\d{7,8}$")
    education["synapseLink"] = ""
    if "iconTags" not in education.columns:
        education["iconTags"] = ""
    
    for _, row in education.iterrows():
        alias_list = [alias for alias in row["ResourceAlias"].split(",") if alias_pattern.match(alias)]
        if len(alias_list) < 1:
            url_list = [url for url in row["ResourceLink"].split(",") if url_pattern.match(url)]
            formatted_syn_link_list = ["".join(["[Link](", url, ")"]) for url in url_list]
        else:
            syn_link_tuples = [("".join(["https://www.synapse.org/Synapse:", alias]), alias) for alias in alias_list]
            formatted_syn_link_list = ["".join(["[", alias, "](", syn_link, ")"]) for syn_link, alias in syn_link_tuples]
        
        syn_links = ", ".join(formatted_syn_link_list)
        education.at[_, "synapseLink"] = syn_links

        education.at[_, "ResourceTopic"] = ", ".join(dict.fromkeys(topic for topic in row["ResourceTopic"].split(", ") if topic != "Diversity/Equity/Inclusion"))
        
        for key_col, external_link_col in zip(["DatasetViewKey", "ToolViewKey"], ["ResourceDatasetAlias", "ResourceToolLink"]):
            key_links = [f"[{key}](https://www.synapse.org/#!Synapse:{key})" for key in row[key_col].split(", ") if row[key_col]]
            external_links = [external_link for external_link in row[external_link_col].split(", ") if row[external_link_col]]
            education.at[_, external_link_col] = ", ".join(external_links + key_links)
    
    return education

def clean_table(df: pd.DataFrame, live_types: dict[str, str]) -> pd.DataFrame:
    """Convert to the portal table's shape (names, order and list columns)."""

    if "iconTags" not in df.columns:
        df["iconTags"] = ""

    return crosswalk.to_portal(df, "EducationalResource", live_types)


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("education")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = utils.fill_blank(utils.read_sync_manifest(syn, args))
    manifest = model_contract.rename_legacy_headers(manifest, "EducationalResource")
    manifest = model_contract.normalize_headers(manifest, to="keys")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing educational resource staging database...")
    database = add_missing_info(manifest)
    live_types = crosswalk.check_crosswalk(
        syn, args.portal_table_id, "EducationalResource"
    )
    final_database = clean_table(database, live_types)
    if args.verbose:
        print("\n🔍 Educational resource(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(
            syn,
            args.portal_table_id,
            final_database,
            component="EducationalResource",
        )
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
