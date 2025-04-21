"""create_entity_links.py

Uses link in a resource manifest to generate a Synapse Link Entity
Links are created in the MC2 Center reference folders, also used to store metadata
Returns the entity ID and adds it to the primary key column of the input manifest

author: orion.banks
"""

import argparse
import numpy as np
import pandas as pd
import synapseclient
from synapseclient import File


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        type=str,
        help="path to manifest listing file paths and target folders in csv format",
    )
    parser.add_argument(
        "-t",
        type=str,
        choices=["DatasetView", "EducationalResource", "PublicationView", "ToolView"],
        help="Type of manifest being submitted",
    )
    return parser.parse_args()


def get_names(manifest: str, name_column: str, target_column: str, link_column: str) -> list[tuple[str, str, str, str]]:

    path_name_link_target = []

    paths_sheet = pd.read_csv(manifest)

    paths = paths_sheet["File Paths"].tolist()

    targets = paths_sheet[f"{target_column}"].tolist() 

    for path, target in zip(paths, targets):

        df = pd.read_csv(path)
        names = df[f"{name_column}"].tolist()
        links = df[f"{link_column}"].tolist()

        for name, link in zip(names, links):

            folder_info = (path, name, link, target)
            path_name_link_target.append(folder_info)

    return path_name_link_target


def create_links(syn, path_name_link_target: list[tuple[str, str, str, str]]) -> list[tuple[str, str, str]]: 

    paths, names, links, targets = zip(*path_name_link_target)

    path_name_id = []

    for p, n, l, t in zip(paths, names, links, targets):

        n = n.translate(str.maketrans("", "", "[]:/!@#$<>"))

        entity = File(path=l, name=n, parent=t, synapseStore=False)
        entity = syn.store(entity)
        id = entity.id
        info = (p, n, id)
        path_name_id.append(info)

    return path_name_id


def add_ids_to_manifests(path_name_id: list[tuple[str, str, str]], name_column: str, primary_key: str) -> None:

    df_to_merge = pd.DataFrame.from_records(
        path_name_id, columns=["File Paths", f"{name_column}", f"{primary_key}"]
    )
    

    path_groups = df_to_merge.groupby(["File Paths"], sort=False)
    

    for name, group in path_groups:
        
        name_path = name[0]
        print(name_path)
        base_df = pd.read_csv(name_path, index_col=False, dtype=str)
        info_df = group[[f"{name_column}", f"{primary_key}"]]
        info_df = info_df.set_index(keys=np.arange(stop=len(info_df)))
        base_df[f"{primary_key}"] = info_df[f"{primary_key}"]
        base_df.to_csv(path_or_buf=name_path, index=False)


def main():

    syn = synapseclient.login()  

    args = get_args()

    manifest, data_type = args.m, args.t

    if data_type == "DatasetView":

        name_column = "Dataset Alias"
        primary_key = "DatasetView_id"
        link_column = "Dataset Url"
        target_column = "folderIdDatasets"

    elif data_type == "ToolView":

        name_column = "Tool Name"
        primary_key = "ToolView_id"
        link_column = "Tool Homepage"
        target_column = "folderIdTools"

    elif data_type == "EducationalResource":

        name_column = "Resource Title"
        primary_key = "EducationalResource_id"
        link_column = "Resource Link"
        target_column = "folderIdEducation"


    print("Capturing information from " + data_type + " manifests...")
    pnt = get_names(manifest, name_column, target_column, link_column)

    print("Generating Synapse Link Entities for each set of " + data_type + " entries...")
    pni = create_links(syn, pnt)

    print(
        "Adding Synapse IDs to "
        + primary_key
        + " column of "
        + data_type
        + " manifests"
    )
    
    add_ids_to_manifests(pni, name_column, primary_key)

    print("Manifests have been populated with Synapse IDs!")


if __name__ == "__main__":
    main()
