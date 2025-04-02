"""table_to_annotations.py

This script will query a Synapse table for metadata and apply it to an entity as annotations.

Usage:
python table_to_annotations.py -t [Dataset Synapse Id] -f [File View metadata table Synapse Id] -s [Biospecimen metadata table Synapse Id] -i [Individual metadata table Synapse Id] -m [Model metadata table Synapse Id]

author: orion.banks
"""

import synapseclient
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


def get_dataset_file_ids(syn, dataset_id:str) -> list:
    """Collect file Synapse Ids from a Synapse Dataset entity"""
    
    query = f"SELECT id FROM {dataset_id}"
    file_table = syn.tableQuery(query).asDataFrame().fillna("")
    files = list(file_table["id"])
    print(f"Synapse Ids acquired from Dataset {dataset_id}")

    return files


def get_annotations_table(syn, source_id:str) -> pd.DataFrame:
    """Collect all entries from a Synapse Table entity"""
    
    query = f"SELECT * FROM {source_id}"
    annotations_table = syn.tableQuery(query).asDataFrame().fillna("")
    print(f"Annotations acquired from Synapse table {source_id}")

    return annotations_table


def collect_fileview_annotations(syn, files: list, fileview_id: str) -> dict:
    """Collect all entries from a File View metadata table,
    extract Biospecimen and File identifiers,
    return a Biospecimen Key: File Synapse Id dictionary"""
    
    id_key_tuples = []

    fileview_table = get_annotations_table(syn, fileview_id)

    for _, row in fileview_table.iterrows():
        id_key_tuple = (row["Biospecimen Key"], row["FileView_id"])
        id_key_tuples.append(id_key_tuple)
    
    file_tuples = dict([tup for tup in id_key_tuples if tup[1] in files])

    return file_tuples
            

def collect_biospecimen_annotations(syn, file_tuples: dict, specimen_info_tuple: tuple[str, str, str]) -> tuple[dict, dict]:
    """Collect all entries from a Biospecimen metadata table,
    select entries where Biospecimen_id is present in Biospecimen Keys associated with files,
    match Biospecimen metadata with column names to create key:value pairs,
    apply annotations to each file,
    return Individual Key:File Id and Model Key: File Id dictionaries
    """
    
    individual_tup_list = []
    model_tup_list = []
    
    component, table_id, column_list = specimen_info_tuple
    data_table = get_annotations_table(syn, table_id)
    count = 0
    for _, row in data_table.iterrows():
        id = row["Biospecimen_id"]
        if id in file_tuples.keys():
            file_id = file_tuples[id]
            individual_id = row["Individual Key"]
            model_id = row["Model Key"]
            individual_tup = (individual_id, file_id)
            individual_tup_list.append(individual_tup)
            model_tup = (model_id, file_id)
            model_tup_list.append(model_tup)
            biospecimen_annotations = list(zip(column_list, list(row)))
            biospecimen_annotations = apply_annotations_to_entity(syn, component, file_id, biospecimen_annotations)
            count += 1
    
    print(f"Biospecimen annotations applied to {count} entities")
    return dict(individual_tup_list), dict(model_tup_list)


def collect_record_annotations(syn, info_tuple: tuple[str, str, str], tuple_dict: dict):
    """Collect all entries from a Synapse table,
    select entries where primary key (e.g. Individual_id) matches foreign key (e.g. Individual Key),
    based on dictionary output from collect_biospecimen_annotations,
    apply annotations to each file"""
    
    component, table_id, column_list = info_tuple
    data_table = get_annotations_table(syn, table_id)
    count = 0
    for _, row in data_table.iterrows():
        id = row[f"{component}_id"]
        if id in tuple_dict.keys():
            file_id = tuple_dict[id]
            annotations = list(zip(column_list, list(row)))
            annotations = apply_annotations_to_entity(syn, component, file_id, annotations)
            count += 1
    
    print(f"{component} annotations applied to {count} entities")


def apply_annotations_to_entity(syn, component: str, entity_id: str, new_annotations: list[tuple[str, str]]):
    """Apply annotations to a Synapse entity by:
    retrieving current annotations,
    converting new_annotations tuple to key:value pairs within the retrieved annotation object,
    storing modified annotation object in Synapse.
    Note that only keys with non-empty values will be applied."""
    
    entity_annotations = syn.get_annotations(entity_id)
    filtered_annotations = [tup for tup in new_annotations if len(tup[1]) > 0]
    for annot in filtered_annotations:
        entity_annotations[annot[0].replace(" ", "")] = annot[1]
    print(entity_annotations)
    #entity = syn.set_annotations(entity_annotations)
    print(f"\n{component} annotations applied to Synapse entity: {entity_id}")


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
    
    individual_columns = ['Component',
                          'Individual_id',
                          'Study Key',
                          'Individual dbGaP Subject Id',
                          'Individual Sex',
                          'Individual Gender',
                          'Individual Age at Diagnosis',
                          'Individual Disease Type',
                          'Individual Primary Diagnosis',
                          'Individual Primary Site',
                          'Individual Primary Tumor Stage',
                          'Individual Site of Origin',
                          'Individual Tumor Subtype',
                          'Individual Tumor Grade',
                          'Individual Tumor Lymph Node Stage',
                          'Individual Known Metastasis Sites',
                          'Individual Metastasis Stage',
                          'Individual Treatment Type',
                          'Individual Therapeutic Agent',
                          'Individual Days to Treatment',
                          'Individual Treatment Response',
                          'Individual Days to Last Followup',
                          'Individual Recurrence Status',
                          'Individual Days To Recurrence',
                          'Individual Days to Last Known Disease Status',
                          'Individual Last Known Disease Status',
                          'Individual Vital Status']
    
    model_columns = ['Component',
                     'Model_id',
                     'Study Key',
                     'Individual Key',
                     'Model Age',
                     'Model Age Unit',
                     'Model Sex',
                     'Model Disease Type',
                     'Model Primary Diagnosis',
                     'Model Primary Site',
                     'Model Site of Origin',
                     'Model Tumor Subtype',
                     'Model Species',
                     'Model Type',
                     'Model Method',
                     'Model Source',
                     'Model Acquisition Type',
                     'Model Graft Source',
                     'Model Genotype',
                     'Model Treatment Type',
                     'Model Therapeutic Agent',
                     'Model Days to Treatment',
                     'Model Treatment Response']

    specimen_info_tuple = ("Biospecimen", specimen_table, biospecimen_columns)
    individual_info_tuple = ("Individual", individual_table, individual_columns)
    model_info_tuple = ("Model", model_table, model_columns)
    
    files = get_dataset_file_ids(syn, target)

    file_view_out = collect_fileview_annotations(syn, files, file_table)

    ind_dict, model_dict = collect_biospecimen_annotations(syn, file_view_out, specimen_info_tuple)

    if individual_table is not None:
        individual_out = collect_record_annotations(syn, individual_info_tuple, ind_dict)

    if model_table is not None:
        model_out = collect_record_annotations(syn, model_info_tuple, model_dict)

if __name__ == "__main__":
    main()
