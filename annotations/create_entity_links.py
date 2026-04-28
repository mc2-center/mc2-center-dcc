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
        required=False,
        default=None
    )
    parser.add_argument(
        "-t",
        type=str,
        choices=["DatasetView", "EducationalResource", "PublicationView", "ToolView"],
        help="Type of manifest being submitted",
    )
    parser.add_argument(
        "-n",
        type=str,
        help="Name of the entity link that will be created.",
    )
    parser.add_argument(
        "-l",
        type=str,
        help="URL for the resource to be stored as a link entity.",
    )
    parser.add_argument(
        "-p",
        type=str,
        help="Synapse ID for the parent entity (Project or Folder) in which the link entity will be stored.",
    )
    parser.add_argument(
        "-d",
        type=str,
        help="The path to a csv containing metadata of the type indicated.",
    )
    return parser.parse_args()


def get_names(name_column: str, target: str, link_column: str, manifest: str = None, data: str = None,) -> list[tuple[str, str, str, str]]:

    path_name_link_target = []

    if manifest is not None:
        paths_sheet = pd.read_csv(manifest)
        paths = paths_sheet["File Paths"].tolist()
        targets = paths_sheet[f"{target}"].tolist()
        df_path_target_list = [(pd.read_csv(p), p, t) for p, t in zip(paths, targets)]

    elif data is not None:
        df_path_target_list = [(pd.read_csv(data), data, target)]

    for df, path, target in zip(df_path_target_list):
        names = df[f"{name_column}"].tolist()
        links = df[f"{link_column}"].tolist()
        path_name_link_target = path_name_link_target + [(path, name, link, target) for name, link in zip(names, links)]

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

    manifest, data, data_type, name, link, target = args.m, args.d, args.t, args.n, args.l, args.p, 

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

    if manifest is not None:
        print("Capturing information from " + data_type + " manifests...")
        pnt = get_names(name_column, target_column, link_column, manifest=manifest)
    elif data is not None:
        pnt = get_names(name_column, target_column, link_column, data=data)
    
    else:
        pnt = list((None, name, link, target))

    print("Generating Synapse Link Entities for each set of " + data_type + " entries...")
    pni = create_links(syn, pnt)
    print(f"The following link entities were created:\n{[i for tup in pni for p,n,i in tup]}")

    if manifest is not None or data is not None:
        print(f"Adding Synapse IDs to {primary_key} column of {data_type} manifests")
        add_ids_to_manifests(pni, name_column, primary_key)
        print("Manifest(s) have been populated with Synapse IDs for link entities!")

if __name__ == "__main__":
    main()
