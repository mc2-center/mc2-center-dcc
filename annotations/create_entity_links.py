"""create_entity_links.py

Uses link in a resource manifest to generate a Synapse Link Entity
Links are created in the MC2 Center reference folders, also used to store metadata
Returns the entity ID and adds it to the primary key column of the input manifest

For DatasetView entries whose Dataset Url contains a GEO or SRA accession, a full
Synapse Dataset entity with GEO FTP file links is created instead of a plain File
link, using the geo-synapse pipeline (https://github.com/sagebio-ada/geo_dataset_creation).

author: orion.banks
"""

import argparse
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import synapseclient
from synapseclient import File

from geo_synapse.mc2_table import parse_accession, find_project_for_grant
from geo_synapse.synapse_ops import (
    find_or_create_folder,
    create_synapse_dataset,
    add_to_dataset_collection,
)
from geo_synapse.pipeline import run_one

# Central public project where GEO Dataset entities and link folders are created.
# Must have PUBLIC: READ + DOWNLOAD so files are accessible without touching PI projects.
_CENTRAL_PROJECT_SYNID = "syn76986881"   # CCKP Indexed Datasets
_COLLECTION_SYNID = "syn77025587"        # DatasetCollection in _CENTRAL_PROJECT_SYNID
_DATASETS_FOLDER = "datasets"            # subfolder for {accession}_links folders


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

    for df, path, target in df_path_target_list:
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


def _index_geo_dataset(
    syn: synapseclient.Synapse,
    accession: str,
    grant_number: str,
    outdir: Path,
) -> tuple[str | None, str | None]:
    """Create a GEO/SRA Dataset entity with FTP file links in the central project.

    Returns (dataset_synid, None) on success or (None, error_message) on failure.
    """
    try:
        study_synid = find_project_for_grant(syn, grant_number) or ""
        datasets_folder_id = find_or_create_folder(syn, _DATASETS_FOLDER, _CENTRAL_PROJECT_SYNID)
        links_folder_synid = find_or_create_folder(syn, f"{accession}_links", datasets_folder_id)
        dataset_synid = create_synapse_dataset(syn, accession, _CENTRAL_PROJECT_SYNID)

        result = run_one(
            syn=syn,
            accession=accession,
            folder_synid=links_folder_synid,
            dataset_synid=dataset_synid,
            outdir=outdir / accession,
            grant_number=grant_number,
            study_synid=study_synid,
            skip_make_public=True,
        )

        if result.get("error"):
            return None, result["error"]
        if result.get("no_raw_data"):
            return None, "no_raw_data"

        add_to_dataset_collection(syn, _COLLECTION_SYNID, dataset_synid)
        return dataset_synid, None

    except Exception as e:
        return None, str(e)


def _build_grant_lookup(manifest: str) -> dict[tuple[str, str], str]:
    """Build a (manifest_path, dataset_alias) → grant_number lookup from split manifests."""
    lookup = {}
    paths_sheet = pd.read_csv(manifest)
    for _, row in paths_sheet.iterrows():
        path = row["File Paths"]
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
            for _, mrow in df.iterrows():
                alias = mrow.get("Dataset Alias", "")
                grant = mrow.get("GrantView Key", "")
                if alias:
                    lookup[(path, alias)] = grant
        except Exception:
            pass
    return lookup


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

    # For DatasetView, GEO/SRA accessions get a full Dataset entity with FTP file
    # links instead of a plain File link entity.
    if data_type == "DatasetView" and manifest is not None:
        print("Checking for GEO/SRA accessions to index via geo-synapse pipeline...")
        grant_lookup = _build_grant_lookup(manifest)
        geo_outdir = Path(tempfile.mkdtemp()) / "geo_indexing"

        path_name_id = []
        for p, n, l, t in pnt:
            accession = parse_accession(f"[{n}]({l})")
            if accession:
                grant_number = grant_lookup.get((p, n), "")
                print(f"  GEO/SRA accession: {accession} (grant: {grant_number}) — indexing...")
                dataset_synid, err = _index_geo_dataset(syn, accession, grant_number, geo_outdir)
                if dataset_synid:
                    print(f"    → {dataset_synid}")
                    path_name_id.append((p, n, dataset_synid))
                    continue
                else:
                    print(f"    Indexing failed ({err}), falling back to File link")

            # Non-GEO or failed GEO: create plain File link entity
            n_clean = n.translate(str.maketrans("", "", "[]:/!@#$<>"))
            entity = File(path=l, name=n_clean, parent=t, synapseStore=False)
            entity = syn.store(entity)
            path_name_id.append((p, n, entity.id))

    else:
        print("Generating Synapse Link Entities for each set of " + data_type + " entries...")
        path_name_id = create_links(syn, pnt)

    print(f"The following link entities were created:\n{[i for p,n,i in path_name_id]}")

    if manifest is not None or data is not None:
        print(f"Adding Synapse IDs to {primary_key} column of {data_type} manifests")
        add_ids_to_manifests(path_name_id, name_column, primary_key)
        print("Manifest(s) have been populated with Synapse IDs for link entities!")

if __name__ == "__main__":
    main()
