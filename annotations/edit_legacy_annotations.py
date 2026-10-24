import argparse
import os
import sys

import pandas as pd
from synapseclient.models import Table

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import header_maps
import model_contract
import utils


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update legacy annotations to latest standard terms."
    )
    parser.add_argument(
        "-u",
        "--union_table_id",
        type=str,
        help="Table synID with annotations to update.",
    )
    parser.add_argument(
        "-cv",
        "--cv_list",
        type=str,
        default=model_contract.ALL_VALID_VALUES_URL,
        help="CSV of controlled terms and their non-preferred terms",
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def get_standard_terms(dictionary: dict, terms: list) -> set:
    """Map list of terms to their standard term, removing all duplicates."""
    return {dictionary.get(term, term) for term in terms}


def map_legacy_terms_to_standard(vocab_csv: str) -> dict:
    """Generate a dictionary of legacy terms to their standard term.

    Standard terms that do not have legacy terms will NOT be added
    to the dictionary.
    """
    current_cv = pd.read_csv(vocab_csv)

    # Only consider terms with legacy terms, then explode the list.
    filtered_cv = current_cv[current_cv["nonpreferred_values"].notna()]
    filtered_cv.loc[:, "nonpreferred_values"] = (
        filtered_cv["nonpreferred_values"].str.replace(", ", ",").str.split(",")
    )
    filtered_cv = filtered_cv.explode("nonpreferred_values")

    # Create a nested dictionary of
    #   { category -> { non-preferred-term -> standard term } }
    cv_dict = filtered_cv.groupby("category").apply(
        lambda x: dict(zip(x["nonpreferred_values"], x["valid_value"])),
        include_groups=False,
    )
    return cv_dict


def update_nonpreferred_terms(
        table: pd.DataFrame,
        cv_dict: dict
) -> pd.DataFrame:
    """Update legacy annotations found to current standard terms."""

    # Manifest may use colnames that are variations of what is listed
    # in the CV, e.g. "Assay" or its 13.1.0 per-entity name instead of
    # "assay". Re-map colnames to CV categories (derived from the crosswalk),
    # keeping record of the original manifest colnames.
    og_colnames = table.columns
    table = table.rename(columns=header_maps.cv_category_columns())
    for category in cv_dict.index:
        if category in table.columns:
            print(f"\tChecking {category}...")
            table.loc[:, category] = (
                table[category]
                .str.replace(", ", ",")
                .str.split(",")
                .apply(lambda annots: ", ".join(
                    get_standard_terms(cv_dict[category], annots)
                ))
            )

    # Rename colnames to the original names.
    table.columns = og_colnames
    return table


def update_manifest_tables(syn, scope_ids: list, cv_dict: dict, dryrun: bool):
    """Update each parent table found in union table."""
    for table_id in scope_ids:
        print(f"Updating annotations found in table ID: {table_id}")
        table = Table(id=table_id)
        updated_table = update_nonpreferred_terms(
            table.query(query=f"SELECT * FROM {table_id}", synapse_client=syn).fillna(""),
            cv_dict
        )
        if dryrun:
            updated_table.to_csv(table_id + "-updated.csv", index=False)
        else:
            table.store_rows(values=updated_table, synapse_client=syn)


def main():
    """Main function."""
    syn = utils.syn_login()
    args = get_args()

    union_table_scope_ids = (
        Table(id=args.union_table_id)
        .query(query=f"SELECT entityId FROM {args.union_table_id}", synapse_client=syn)["entityId"]
        .unique()
    )
    cv_dict = map_legacy_terms_to_standard(args.cv_list)

    if args.dryrun:
        print("\n❗❗❗ WARNING: dryrun is enabled. Results will be "
              "saved to CSV instead.\n" + "=" * 80 + "\n")
    update_manifest_tables(syn, union_table_scope_ids, cv_dict, args.dryrun)


if __name__ == "__main__":
    main()
