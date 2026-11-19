"""create_entity_links.py

Uses link in a resource manifest to generate a Synapse Link Entity
Links are created in the MC2 Center reference folders, also used to store metadata
(ToolView: one shared entity per tool, in the central project's "tools" folder; a tool
without a homepage gets an empty Folder)
Returns the entity ID and adds it to the primary key column of the input manifest

For DatasetView entries whose Dataset Url contains a GEO or SRA accession, a full
Synapse Dataset entity with GEO FTP file links is created instead of a plain File
link, using the geo-synapse pipeline (https://github.com/sagebio-ada/geo_dataset_creation).
Other DatasetView entries get an empty placeholder Folder named {alias}_links in the
central project's "datasets" folder (the folder that also holds the {accession}_links
folders). The entity holds no URL: the repository URL stays in the row's Dataset Url.
A row whose Dataset Url names a Synapse entity uses that Synapse ID and gets no new entity.
An alias with different Dataset Url values across the split files is skipped in all its rows.

Run it through annotations/upload_workflow.py (its links stage), not on its own: this script
only writes the new IDs into the local manifest CSVs, and the workflow's upload stage is
what puts them in the grant's RecordSet.

author: orion.banks
"""

import argparse
import csv
import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import re
from synapseclient import Synapse
from synapseclient.models import File, Folder

from geo_synapse.mc2_table import parse_accession, find_project_for_grant
from geo_synapse.synapse_ops import (
    find_or_create_folder,
    create_synapse_dataset,
    add_to_dataset_collection,
)
from geo_synapse.pipeline import run_one

import os
import sys
# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import utils

logger = logging.getLogger(__name__)

# Central public project where GEO Dataset entities and link folders are created.
# Must have PUBLIC: READ + DOWNLOAD so files are accessible without touching PI projects.
_CENTRAL_PROJECT_SYNID = "syn76986881"   # CCKP Indexed Datasets
_COLLECTION_SYNID = "syn77025587"        # DatasetCollection in _CENTRAL_PROJECT_SYNID
_DATASETS_FOLDER = "datasets"            # subfolder for {accession}_links folders and non-GEO placeholder folders
_TOOLS_FOLDER = "tools"                  # subfolder holding one shared entity per tool


def get_args(argv=None):
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
    parser.add_argument(
        "--created-ids",
        type=str,
        default=None,
        help="CSV that records each created entity ID as it is made (row key -> synID). "
        "Rows already listed are skipped on a re-run, so an interrupted run can be resumed. "
        "Opt-in: without this flag no resume file is kept.",
    )
    return parser.parse_args(argv)


def clean_link_name(name: str) -> str:
    return name.translate(str.maketrans("", "", "[]:/!@#$<>"))


def row_key(path, name, link) -> str:
    """Stable key for a manifest row: split-file name, alias and link (not the row
    position, which shifts when other rows are removed between runs)."""
    return f"{Path(str(path)).name}|{name}|{link}"


def load_created_ids(path) -> dict[str, str]:
    if not path or not Path(path).exists():
        return {}
    with open(path, newline="") as fh:
        return {r["key"]: r["synapse_id"] for r in csv.DictReader(fh)}


def record_created_id(path, key: str, synapse_id: str) -> None:
    """Append one row and flush, so a mid-run failure loses nothing already created."""
    if not path:
        return
    new = not Path(path).exists()
    with open(path, "a", newline="") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["key", "synapse_id"])
        w.writerow([key, synapse_id])
        fh.flush()
        os.fsync(fh.fileno())


FILE_ENTITY_TYPE = "org.sagebionetworks.repo.model.FileEntity"
FOLDER_ENTITY_TYPE = "org.sagebionetworks.repo.model.Folder"


def store_link(syn, name: str, link: str, parent_id: str) -> str:
    """Create the plain File link, or reuse a same-named File under the parent.

    A same-named entity is reused only if it is a File (concreteType FileEntity)
    whose external URL equals ``link``. A File with another URL gets its URL
    updated (a new version pointing at ``link``); an entity that is not a File
    raises ValueError so the row fails with a clear message instead of an
    unrelated entity's ID going into the manifest.

    Uses the synapseclient.models File: ``File(id=..., download_file=False).get()``
    loads the file handle (``external_url``); setting ``external_url`` with
    ``synapse_store=False`` and calling ``store()`` swaps in a new
    ExternalFileHandle when the URL changed.
    """
    existing = syn.findEntityId(name, parent=parent_id)
    if not existing:
        return File(external_url=link, name=name, parent_id=parent_id, synapse_store=False).store(synapse_client=syn).id
    concrete = syn.restGET(f"/entity/{existing}").get("concreteType")
    if concrete != FILE_ENTITY_TYPE:
        raise ValueError(
            f"Cannot link {name!r}: {existing} under {parent_id} already exists and is not a File ({concrete})"
        )
    file = File(id=existing, download_file=False).get(synapse_client=syn)
    if file.external_url != link:
        logger.info("Updating external URL of %s (%s): %s -> %s", existing, name, file.external_url, link)
        file.external_url = link
        file.synapse_store = False
        file = file.store(synapse_client=syn)
    return existing


class ToolConflict(ValueError):
    """A same-named entity in the tools folder that cannot be reused for this tool row."""

    def __init__(self, message, name, homepage, existing_id, existing_type, existing_url):
        super().__init__(message)
        self.record = {"name": name, "homepage": homepage, "existing_id": existing_id,
                       "existing_type": existing_type, "existing_url": existing_url, "message": message}


def store_tool_entity(syn, name: str, link: str, parent_id: str) -> str:
    """Create or reuse the one shared entity for a tool in the central tools folder.

    With a homepage the entity is a link File; without one it is an empty Folder.
    A same-named entity is reused only if it is the same kind (and, for a File, has
    the same external URL). Anything else raises ToolConflict (a ValueError) naming the
    tool, the existing ID and both URLs; nothing is overwritten.
    """
    link = (link or "").strip()
    existing = syn.findEntityId(name, parent=parent_id)
    if not existing:
        if link:
            return File(external_url=link, name=name, parent_id=parent_id, synapse_store=False).store(synapse_client=syn).id
        return Folder(name=name, parent_id=parent_id).store(synapse_client=syn).id
    concrete = syn.restGET(f"/entity/{existing}").get("concreteType")
    if concrete == FILE_ENTITY_TYPE:
        url = File(id=existing, download_file=False).get(synapse_client=syn).external_url
        if link and url == link:
            return existing
    elif concrete == FOLDER_ENTITY_TYPE and not link:
        return existing
    else:
        url = None
    raise ToolConflict(
        f"Cannot create tool {name!r}: {existing} ({concrete}) already exists in {parent_id} "
        f"with URL {url!r}, but this row has homepage {link!r}",
        name, link, existing, concrete, url,
    )


def store_dataset_placeholder(syn, name: str, parent_id: str) -> str:
    """Create or reuse the placeholder Folder for a dataset that has no indexed files.

    A same-named Folder is reused. A same-named entity of another type raises
    ToolConflict (a ValueError); nothing is overwritten.
    """
    existing = syn.findEntityId(name, parent=parent_id)
    if not existing:
        return Folder(name=name, parent_id=parent_id).store(synapse_client=syn).id
    concrete = syn.restGET(f"/entity/{existing}").get("concreteType")
    if concrete == FOLDER_ENTITY_TYPE:
        return existing
    raise ToolConflict(
        f"Cannot create placeholder folder {name!r}: {existing} ({concrete}) already exists in {parent_id} "
        "and is not a Folder",
        name, "", existing, concrete, None,
    )


def synapse_id_of(url: str):
    """None if url is not a synapse.org URL; else the first Synapse ID in it ("" if it has none)."""
    if not re.search(r"https?://(?:[\w-]+\.)*synapse\.org\b", url or "", re.IGNORECASE):
        return None
    m = re.search(r"\bsyn\d+\b", url, re.IGNORECASE)
    return m.group(0).lower() if m else ""


def alias_url_mismatches(pnt) -> dict[str, list[tuple[str, str]]]:
    """Aliases that have more than one Dataset Url (compared after strip) in a run:
    alias -> [(file, url), ...] for each of its rows."""
    seen = {}
    for p, n, l, _ in pnt:
        seen.setdefault(n.strip(), []).append((p, l.strip()))
    return {a: rows for a, rows in seen.items() if len({u for _, u in rows}) > 1}


def get_names(name_column: str, target: str, link_column: str, manifest: str = None, data: str = None,) -> list[tuple[str, str, str, str]]:

    path_name_link_target = []

    if manifest is not None:
        paths_sheet = pd.read_csv(manifest, dtype=str, keep_default_na=False)
        paths = paths_sheet["File Paths"].tolist()
        targets = paths_sheet[f"{target}"].tolist()
        df_path_target_list = [(pd.read_csv(p, dtype=str, keep_default_na=False), p, t) for p, t in zip(paths, targets)]

    elif data is not None:
        df_path_target_list = [(pd.read_csv(data, dtype=str, keep_default_na=False), data, target)]

    for df, path, target in df_path_target_list:
        names = df[f"{name_column}"].tolist()
        links = df[f"{link_column}"].tolist()
        path_name_link_target = path_name_link_target + [(path, name, link, target) for name, link in zip(names, links)]

    return path_name_link_target


def create_links(
    syn, path_name_link_target: list[tuple[str, str, str, str]], created_ids_path=None, store=store_link,
    conflicts=None,
) -> list[tuple[str, str, str]]:
    """Store each row's link entity. With a ``conflicts`` list, a ToolConflict skips
    that row (warning, appended to the list, blank ID, not recorded in created_ids so
    a rerun retries it); any other error still stops the run."""

    done = load_created_ids(created_ids_path)
    path_name_id = []

    for p, n, l, t in path_name_link_target:

        key = row_key(p, n, l)
        n = clean_link_name(n)
        if key in done:
            path_name_id.append((p, n, done[key]))
            continue

        try:
            id = store(syn, n, l, t)
        except ToolConflict as e:
            if conflicts is None:
                raise
            logger.warning("%s", e)
            conflicts.append({"file": p, **e.record})
            path_name_id.append((p, n, ""))  # blank, so later rows keep their position
            continue
        record_created_id(created_ids_path, key, id)
        path_name_id.append((p, n, id))

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
    syn: Synapse,
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


def _file_grant(path: str) -> str:
    """Grant number a split file is named for (``CA123456_dataset.csv``), or ""."""
    match = re.match(r"(CA\d{6})_", os.path.basename(str(path)))
    return match.group(1) if match else ""


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
                # The split file is per grant; GrantView Key can list several.
                grant = _file_grant(path) or mrow.get("GrantView Key", "").split(",")[0].strip()
                if alias:
                    lookup[(path, alias)] = grant
        except Exception:
            pass
    return lookup


def main(argv=None):
    """Returns the ToolView/DatasetView rows skipped for a ToolConflict (blank ID in their manifest)."""

    syn = utils.syn_login()

    args = get_args(argv)

    manifest, data, data_type, name, link, target = args.m, args.d, args.t, args.n, args.l, args.p,
    created_ids_path = args.created_ids
    conflicts = []

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
        pnt = [(None, name, link, target)]

    # For DatasetView, GEO/SRA accessions get a full Dataset entity with FTP file
    # links instead of a plain File link entity.
    if data_type == "DatasetView" and manifest is not None:
        print("Checking for GEO/SRA accessions to index via geo-synapse pipeline...")
        grant_lookup = _build_grant_lookup(manifest)
        geo_outdir = Path(tempfile.mkdtemp()) / "geo_indexing"

        done = load_created_ids(created_ids_path)
        path_name_id = []
        datasets_folder = None  # central datasets folder, looked up on first use
        mismatched = alias_url_mismatches(pnt)
        for p, n, l, t in pnt:
            if n.strip() in mismatched:
                details = "; ".join(f"{Path(f).name}: {u!r}" for f, u in mismatched[n.strip()])
                msg = f"Dataset Alias {n.strip()!r} has different Dataset Url values across files ({details})"
                logger.warning("%s", msg)
                conflicts.append({"file": p, "name": n, "homepage": l, "existing_id": "",
                                  "existing_type": "", "existing_url": None, "message": msg})
                path_name_id.append((p, n, ""))
                continue
            key = row_key(p, n, l)
            if key in done:
                print(f"  Already created ({done[key]}): {n}")
                path_name_id.append((p, n, done[key]))
                continue

            # Data stored in Synapse: use that entity directly, no new entity
            syn_id = synapse_id_of(l)
            if syn_id is not None:
                if not syn_id:
                    msg = f"Dataset Url {l!r} of {n!r} is a Synapse URL with no Synapse ID"
                    logger.warning("%s", msg)
                    conflicts.append({"file": p, "name": n, "homepage": l, "existing_id": "",
                                      "existing_type": "", "existing_url": None, "message": msg})
                    path_name_id.append((p, n, ""))
                    continue
                print(f"  Synapse entity {syn_id}: {n}")
                record_created_id(created_ids_path, key, syn_id)
                path_name_id.append((p, n, syn_id))
                continue

            accession = parse_accession(f"[{n}]({l})")
            if accession:
                grant_number = grant_lookup.get((p, n), "")
                print(f"  GEO/SRA accession: {accession} (grant: {grant_number}) — indexing...")
                dataset_synid, err = _index_geo_dataset(syn, accession, grant_number, geo_outdir)
                if dataset_synid:
                    print(f"    → {dataset_synid}")
                    record_created_id(created_ids_path, key, dataset_synid)
                    path_name_id.append((p, n, dataset_synid))
                    continue
                else:
                    print(f"    Indexing failed ({err}), using a placeholder folder")

            # Non-GEO or failed GEO: empty placeholder Folder in the central datasets folder
            if datasets_folder is None:
                datasets_folder = find_or_create_folder(syn, _DATASETS_FOLDER, _CENTRAL_PROJECT_SYNID)
            try:
                entity_id = store_dataset_placeholder(syn, f"{clean_link_name(n)}_links", datasets_folder)
            except ToolConflict as e:
                logger.warning("%s", e)
                conflicts.append({"file": p, **e.record})
                path_name_id.append((p, n, ""))  # blank, so later rows keep their position
                continue
            record_created_id(created_ids_path, key, entity_id)
            path_name_id.append((p, n, entity_id))

    else:
        print("Generating Synapse Link Entities for each set of " + data_type + " entries...")
        store = store_link
        if data_type == "ToolView":
            # One shared entity per tool in the central tools folder, not the grant's folder.
            tools_folder = find_or_create_folder(syn, _TOOLS_FOLDER, _CENTRAL_PROJECT_SYNID)
            pnt = [(p, n, l, tools_folder) for p, n, l, _ in pnt]
            store = store_tool_entity
        path_name_id = create_links(syn, pnt, created_ids_path, store=store,
                                conflicts=conflicts if data_type == "ToolView" else None)

    for c in conflicts:
        print(f"Skipped {data_type} row: {c['message']}")
    print(f"The following link entities were created:\n{[i for p,n,i in path_name_id]}")

    if manifest is not None or data is not None:
        print(f"Adding Synapse IDs to {primary_key} column of {data_type} manifests")
        add_ids_to_manifests(path_name_id, name_column, primary_key)
        print("Manifest(s) have been populated with Synapse IDs for link entities!")
    return conflicts

if __name__ == "__main__":
    main()
