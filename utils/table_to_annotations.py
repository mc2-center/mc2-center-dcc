"""table_to_annotations.py

This script will query a Synapse table for metadata and apply it to an entity as annotations.

Usage:

author: orion.banks
"""

import synapseclient
from synapseclient import Annotations
import argparse
import pandas as pd


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        type=str,
        help="Synapse Id of an dataset with files to annotate",
        required=True
    )
    parser.add_argument(
        "-f",
        type=str,
        help="Synapse Id of a table containing File View metadata.",
        required=True
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Synapse Id of a table containing Biospecimen metadata.",
        required=True
    )
    parser.add_argument(
        "-i",
        type=str,
        help="Synapse Id of a table containing Model metadata.",
        required=False
    )
    parser.add_argument(
        "-m",
        type=str,
        help="Synapse Id of a table containing Model metadata.",
        required=False
    )
    return parser.parse_args()


def get_dataset_file_ids(syn, dataset_id):
    
    query = f"SELECT id FROM {dataset_id}"
    file_table = syn.tableQuery(query).asDataFrame().fillna("")
    files = list(file_table["id"])
    print(f"Synapse Ids acquired from Dataset {dataset_id}")

    return files


def get_annotations_table(syn, source_id):
    
    query = f"SELECT * FROM {source_id}"
    annotations_table = syn.tableQuery(query).asDataFrame().fillna("")
    print(f"Annotations acquired from Synapse table {source_id}")

    return annotations_table


def build_annotations(syn, files, fileview_table, biospecimen_table, individual_table, model_table, mapping, specimen_columns):

    data_types = [fileview_table, biospecimen_table, individual_table, model_table]

    data_tables = [get_annotations_table(syn, data) for data in data_types if data is not None]
    
    id_key_tuples = []

    for _, row in data_tables[0].iterrows():
        id_key_tuple = (row["Biospecimen Key"], row["FileView_id"])
        id_key_tuples.append(id_key_tuple)
    
    file_tuples = dict([tup for tup in id_key_tuples if tup[1] in files])

    for row in data_tables[1].itertuples(index=False):
        biospecimen_id = row[1]
        if biospecimen_id in file_tuples.keys():
            file_id = file_tuples[biospecimen_id]
            individual_id = row[3]
            model_id = row[4]
            new_annotations = list(zip(specimen_columns, list(row)))
            print(new_annotations)
            file_annotations = syn.get_annotations(file_id)
            for annot in new_annotations:
                file_annotations[annot[0]] = annot[1]
            print(file_annotations)
            #entity = syn.set_anotations(new_annotations)
            #print(f"Annotations applied to Synapse entity: {file_id}")
    

def map_annotations(df: pd.DataFrame, column_map: list[tuple]) -> pd.DataFrame:
    """Map table columns names to different schema"""
    #define mapping between table column names and desired schema for entity, if different
    #mostly applicable for datasets or converting between MC2 and CRDC table headers

    for start, end in column_map:

        df[f"{end}"] = [
            x for x in df[f"{start}"]
        ]

        df = df.drop([f"{start}"])

    return df


def main():

    syn = (
        synapseclient.login()
    ) 

    args = get_args()

    target, file_table, specimen_table, individual_table, model_table = args.t, args.f, args.s, args.i, args.m

    biospecimen_columns = ["Component",
                           "Biospecimen_id",
                           "Study Key",
                           "Individual Key",
                           "Model Key",
                           "Parent Biospecimen Key",
                           "Biospecimen Type Category",
                           "Biospecimen Type",
                           "Biospecimen Tumor Status",
                           "Biospecimen Acquisition Method",
                           "Biospecimen Incidence Type",
                           "Biospecimen Stain",
                           "Biospecimen Species",
                           "Biospecimen Sex",
                           "Biospecimen Age at Collection",
                           "Biospecimen Age at Collection Unit",
                           "Biospecimen Disease Type",
                           "Biospecimen Primary Site",
                           "Biospecimen Primary Diagnosis",
                           "Biospecimen Site of Origin",
                           "Biospecimen Tumor Subtype",
                           "Biospecimen Tumor Grade",
                           "Biospecimen Known Metastasis Sites",
                           "Biospecimen Tumor Morphology",
                           "Biospecimen Composition",
                           "Biospecimen Preservation Method",
                           "Biospecimen Fixative",
                           "Biospecimen Embedding Medium",
                           "Biospecimen Anatomic Site",
                           "Biospecimen Site of Resection or Biopsy",
                           "Biospecimen Timepoint Type",
                           "Biospecimen Timepoint Offset",
                           "Biospecimen Collection Site",
                           "Biospecimen Treatment Type",
                           "Biospecimen Therapeutic Agent",
                           "Biospecimen Treatment Response",
                           "Biospecimen Last Known Disease Status",
                           "Biospecimen BioSample Identifier",
                           "Biospecimen Description"]

    files = get_dataset_file_ids(syn, target)
    
    annotations = build_annotations(syn, files, file_table, specimen_table, individual_table, model_table, None, biospecimen_columns)

if __name__ == "__main__":
    main()
