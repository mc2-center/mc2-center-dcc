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
        help="Synapse Id of a dataset with files to annotate",
        required=True,
    )
    parser.add_argument(
        "-v",
        type=str,
        help="Synapse Id of a table containing DatasetView metadata",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-f",
        type=str,
        help="Synapse Id of a table containing File View metadata.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Synapse Id of a table containing Biospecimen metadata.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-i",
        type=str,
        help="Synapse Id of a table containing Individual metadata.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-m",
        type=str,
        help="Synapse Id of a table containing Model metadata.",
        required=False,
        default=None,
    )
    return parser.parse_args()


def get_table(syn, source_id: str, cols: str | list = "*") -> pd.DataFrame:
    """Collect columns from a Synapse table entity and return as a Dataframe."""

    if type(cols) == list:
        cols = ", ".join(["".join(['"', col, '"']) for col in cols])

    query = f"SELECT {cols} FROM {source_id}"
    table = syn.tableQuery(query).asDataFrame().fillna("")
    print(f"Data acquired from Synapse table {source_id}")

    return table


def collect_fileview_annotations(syn, files: list, fileview_id: str) -> dict:
    """Collect all Biospecimen and File identifiers from a File View metadata table,
    return a File Synapse Id: Biospecimen Key dictionary"""

    fileview_columns = ["FileView_id", "Biospecimen Key"]

    fileview_table = get_table(syn, fileview_id, fileview_columns)

    file_biospecimen_mapping = {
        row["FileView_id"]: row["Biospecimen Key"]
        for _, row in fileview_table.iterrows()
        if row["FileView_id"] in files
    }

    return file_biospecimen_mapping


def collect_biospecimen_annotations(
    syn,
    file_biospecimen_dict: dict,
    specimen_info_tuple: tuple[str, str, list[str]],
    keys_to_drop: list[str],
) -> tuple[dict, dict, dict]:
    """Collect all entries from a Biospecimen metadata table,
    select entries where Biospecimen_id is present in Biospecimen Keys associated with files,
    match Biospecimen metadata with column names to create key:value pairs,
    apply annotations to each file,
    return File Id: Individual Key and File Id: Model Key dictionaries
    """

    individual_dict = {}
    model_dict = {}

    component, table_id, column_list = specimen_info_tuple
    data_table = get_table(syn, table_id, column_list).set_index("Biospecimen_id")
    column_list.pop(
        0
    )  # remove Biospecimen_id from list of columns, since it is now the index
    biospecimen_ids = set(file_biospecimen_dict.values())
    filtered_metadata = data_table[data_table.index.isin(biospecimen_ids)]
    count = 0
    for file_id, biospecimen_key in file_biospecimen_dict.items():
        if biospecimen_key in filtered_metadata.index:
            metadata = filtered_metadata.loc[biospecimen_key]
            individual_id = metadata["Individual Key"]
            model_id = metadata["Model Key"]
            individual_dict[file_id] = individual_id
            model_dict[file_id] = model_id
            biospecimen_annotations = list(zip(column_list, metadata.tolist()))
            apply_annotations_to_entity(
                syn, component, file_id, biospecimen_annotations, keys_to_drop
            )
            count += 1
        else:
            print(f"Metadata not found for: {biospecimen_key}")

    print(f"Biospecimen annotations applied to {count} entities")
    return individual_dict, model_dict


def collect_record_annotations(
    syn,
    info_tuple: tuple[str, str, list[str]],
    tuple_dict: dict,
    keys_to_drop: list[str],
):
    """Collect all entries from a Synapse table,
    select entries where primary key (e.g. Individual_id) matches foreign key (e.g. Individual Key),
    based on dictionary output from collect_biospecimen_annotations,
    apply annotations to each file"""

    component, table_id, column_list = info_tuple
    key_column = f"{component}_id"
    data_table = get_table(syn, table_id, column_list).set_index(key_column)
    column_list.pop(
        0
    )  # remove Component_id from list of columns, since it is now the index
    table_keys = set(tuple_dict.values())
    filtered_metadata = data_table[data_table.index.isin(table_keys)]
    count = 0
    for file_id, table_key in tuple_dict.items():
        if table_key in filtered_metadata.index:
            metadata = filtered_metadata.loc[table_key]
            annotations = list(zip(column_list, metadata.tolist()))
            apply_annotations_to_entity(
                syn, component, file_id, annotations, keys_to_drop
            )
            count += 1

    print(f"{component} annotations applied to {count} entities")


def collect_dataset_annotations(
    syn, dataset_id: str, info_tuple: tuple[str, str, list[str]], keys_to_drop: list[str]
):
    """Collect all entries from a DatasetView Synapse table,
    select entry where input table Synapse Id matches DatasetView_id,
    apply annotations to the Dataset"""

    component, table_id, column_list = info_tuple
    key_column = f"{component}_id"
    data_table = get_table(syn, table_id, column_list).set_index(key_column)
    column_list.pop(
        0
    )  # remove DatasetView_id from list of columns, since it is now the index
    if dataset_id in data_table.index:
        metadata = data_table.loc[dataset_id]
        annotations = list(zip(column_list, metadata.tolist()))
        apply_annotations_to_entity(syn, component, dataset_id, annotations, keys_to_drop)


def apply_annotations_to_entity(
    syn,
    component: str,
    entity_id: str,
    new_annotations: list[tuple[str, str]],
    keys_to_drop: list,
):
    """Apply annotations to a Synapse entity by:
    retrieving current annotations,
    filtering to remove empty annotations,
    filtering to remove keys in keys_to_drop,
    converting new_annotations tuple to key:value pairs within the retrieved annotation object,
    storing modified annotation object in Synapse."""

    entity_annotations = syn.get_annotations(entity_id)
    filtered_annotations = [tup for tup in new_annotations if len(tup[1]) > 0]
    for key, annot in filtered_annotations:
        if key not in keys_to_drop:
            entity_annotations[key.replace(" ", "")] = annot
    syn.set_annotations(entity_annotations)
    print(f"{component} annotations applied to Synapse entity: {entity_id}\n")


def main():

    syn = synapseclient.login()

    args = get_args()

    (
        target,
        datasetview_table,
        file_table,
        specimen_table,
        individual_table,
        model_table,
    ) = (args.t, args.v, args.f, args.s, args.i, args.m)

    biospecimen_columns = [
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
        "Biospecimen Description",
    ]

    individual_columns = [
        "Individual_id",
        "Study Key",
        "Individual dbGaP Subject Id",
        "Individual Sex",
        "Individual Gender",
        "Individual Age at Diagnosis",
        "Individual Disease Type",
        "Individual Primary Diagnosis",
        "Individual Primary Site",
        "Individual Primary Tumor Stage",
        "Individual Site of Origin",
        "Individual Tumor Subtype",
        "Individual Tumor Grade",
        "Individual Tumor Lymph Node Stage",
        "Individual Known Metastasis Sites",
        "Individual Metastasis Stage",
        "Individual Treatment Type",
        "Individual Therapeutic Agent",
        "Individual Days to Treatment",
        "Individual Treatment Response",
        "Individual Days to Last Followup",
        "Individual Recurrence Status",
        "Individual Days To Recurrence",
        "Individual Days to Last Known Disease Status",
        "Individual Last Known Disease Status",
        "Individual Vital Status",
    ]

    model_columns = [
        "Model_id",
        "Study Key",
        "Individual Key",
        "Model Age",
        "Model Age Unit",
        "Model Sex",
        "Model Disease Type",
        "Model Primary Diagnosis",
        "Model Primary Site",
        "Model Site of Origin",
        "Model Tumor Subtype",
        "Model Species",
        "Model Type",
        "Model Method",
        "Model Source",
        "Model Acquisition Type",
        "Model Graft Source",
        "Model Genotype",
        "Model Treatment Type",
        "Model Therapeutic Agent",
        "Model Days to Treatment",
        "Model Treatment Response",
    ]

    datasetview_columns = [
        "DatasetView_id",
        "GrantView Key",
        "Study Key",
        "PublicationView Key",
        "Dataset Name",
        "Dataset Alias",
        "Dataset Description",
        "Dataset Design",
        "Dataset Assay",
        "Dataset Species",
        "Dataset Tumor Type",
        "Dataset Tissue",
        "Dataset Url",
        "Dataset Doi",
        "Dataset File Formats",
        "Data Use Codes",
    ]

    specimen_info_tuple = ("Biospecimen", specimen_table, biospecimen_columns)
    individual_info_tuple = ("Individual", individual_table, individual_columns)
    model_info_tuple = ("Model", model_table, model_columns)
    dataset_info_tuple = ("DatasetView", datasetview_table, datasetview_columns)
    keys_to_drop = ["Study Key"]

    if file_table is not None:
        files = get_table(syn, target, cols="id")["id"].tolist()
        file_view_out = collect_fileview_annotations(syn, files, file_table)

        if specimen_table is not None:
            ind_dict, model_dict = collect_biospecimen_annotations(
                syn, file_view_out, specimen_info_tuple, keys_to_drop
            )

        if individual_table is not None:
            collect_record_annotations(
                syn, individual_info_tuple, ind_dict, keys_to_drop
            )

        if model_table is not None:
            collect_record_annotations(syn, model_info_tuple, model_dict, keys_to_drop)

    if datasetview_table is not None:
        collect_dataset_annotations(syn, target, dataset_info_tuple, keys_to_drop)


if __name__ == "__main__":
    main()
