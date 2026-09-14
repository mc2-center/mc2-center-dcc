from synapseclient import Synapse
from synapseclient.models import Table
import argparse
import requests
from time import sleep


### Login to Synapse ###
def login():

    syn = Synapse()
    syn.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(description="Get synapse publications table id")
    parser.add_argument(
        "table_id", type=str, help="Synapse table id to upload manifest to."
    )

    return parser.parse_args()


def get_df(syn, publications_table_id):

    # NOTE: selecting all columns (not just pubMedId/abstract) is deliberate.
    # Table.store_rows() does a full-row replacement -- any column not present
    # in the DataFrame gets nulled out on the updated rows. The legacy
    # `syn.store(Table(table_id, df))` this replaced tolerated a partial
    # column set; store_rows() does not.
    pubs_query = f"SELECT * FROM {publications_table_id}"
    pubs_df = Table(id=publications_table_id).query(query=pubs_query, synapse_client=syn).fillna("")

    return pubs_df


def get_pmids(pubs_df):

    # Convert column of pubmedIds to a list
    pmid_list = pubs_df["pubMedId"].tolist()

    return pmid_list


def get_abstracts(pmid_list, pubs_df):

    total_count = len(pmid_list)
    counter = 0

    for pmid in pmid_list:

        endpoint = f"https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/pubtator?pmids={pmid}&concepts=none"
        response = requests.get(endpoint)
        response_string = response.text
        # Parse text to extract only the abstract
        index = response_string.find("|a|") + 3
        abstract = response_string[index:]

        # Add abstracts to publications data frame
        pubs_df.loc[pubs_df["pubMedId"] == pmid, "abstract"] = abstract
        counter += 1
        print(f"Getting {counter} of {total_count} total abstracts...")

        sleep(0.34)

    return pubs_df


def store_edited_publications(syn, table_id, pubs_df):

    Table(id=table_id).store_rows(values=pubs_df, synapse_client=syn)

    print("\n\nPublications have been updated with Abstracts!")


def main():

    syn = login()
    args = get_args()
    pub_df = get_df(syn, args.table_id)
    pmids = get_pmids(pub_df)
    pub_abstracts = get_abstracts(pmids, pub_df)

    # Uncomment when ready
    store_edited_publications(syn, args.table_id, pub_abstracts)


if __name__ == "__main__":
    main()
