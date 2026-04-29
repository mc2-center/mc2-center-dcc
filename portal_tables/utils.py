"""Shared functions used by syncing scripts"""

import os
import argparse
from getpass import getpass
from datetime import datetime

import synapseclient
import pandas as pd
import re


# Manifest and portal table synIDs of each resource type.
CONFIG = {
    "publication": {"manifest": "syn53478776", "portal_table": "syn21868591"},
    "dataset": {"manifest": "syn53478774", "portal_table": "syn21897968"},
    "tool": {"manifest": "syn53479671", "portal_table": "syn26127427"},
    "people": {"manifest": "syn38301033", "portal_table": "syn28073190"},
    "grant": {"manifest": "syn53259587", "portal_table": "syn21918972"},
    "education": {"manifest": "syn53651540", "portal_table": "syn51497305"},
    "project": {"manifest": "syn59074382", "portal_table": "syn21868602"}
}

DUO_DICT = {
    "GRU" : "Data access is allowed for any research purpose",
    "IRB" : "Requestor must provide documentation of local IRB/ERB approval to access data",
    "NCU" : "Data access is limited to not-for-profit use",
    "NPU" : "Data access is limited to not-for-profit organizations",
    "NPUNCU" : "Data access is limited to not-for-profit organizations and not-for-profit/non-commercial use",
    "NRES" : "There are no restrictions on the use of this data",
    "RS" : "Data access is limited to use in specific types of research",
    "RTN" : "Derived/enriched data must be returned to the database/resource repository",
    "Pending Annotation" : "Access information was not provided for this dataset"
}

REPO_DICT = {
    "CBioPortal" : [("cbioportal"), (None)],
    "Dryad" : [("dryad"), ("dryad")],
    "Harvard Dataverse" : [("dataverse"), (None)], 
    "Mendeley" : [("mendeley"), ("10.17632")],
    "EBI ArrayExpress" : [("arrayexpress"), ("E-MTAB")],
    "EBI Electron Microscopy Data Bank (EMDB)" : [("emdb"), ("EMD")],
    "EBI European Nucleotide Archive (ENA)" : [("ena"), ("PRJE")],
    "EBI Proteomics Identifications Database (PRIDE) - ProteomeXchange member" : [("pride"), ("PXD")],
    "EBI BioImages" : [("BioImages"), ("S-B")],
    "European Genome-phenome Archive (EGA)" : [("ega"), ("EGAD", "EGAS")],
    "FigShare" : [("figshare"), (None)],
    "Flow Repository" : [("flowrepository"), ("FR-")],
    "GitHub" : [("github"), (None)],
    "UCSD MassIVE" : [("massive"), ("MSV")],
    "Metabolomics Workbench" : [("metabolomicsworkbench"), ("PR")],
    "NCBI Bioprojects" : [("bioproject"), ("GSE", "PRJNA")],
    "NCBI Gene Expression Omnibus (GEO)" : [("geo"), ("GSE", "PRJNA")],
    "NCBI Sequence Read Archive (SRA)" : [("sra", "trace"), ("SRP")],
    "NCBI Nucleotide database" : [("nuccore"), ("OK", "SAMN")],
    "NCBI Database of Genotypes and Phenotypes (dbGaP)" : [("gap"), ("phs", "PRJNA")],
    "Cytoscape Consortium Network Data Exchange (NDEx)" : [("ndexbio"), (None)],
    "Proteome Central - ProteomeXchange member" : [("proteomecentral"), ("PXD")],
    "Synapse" : [("synapse"), ("syn")],
    "Harvard Tissue Atlas" : [("tissue-atlas"), (None)],
    "Zenodo" : [("zenodo"), ("zenodo")],
    "None" : "No repository designated"
}

REPO_REGEX = r"(https|http)(:\/\/)(www\.|)(.*\/)(.*?)(\/|)|(Pending Annotation)"

def syn_login() -> synapseclient.Synapse:
    """Log into Synapse. If env variables not found, prompt user."""
    try:
        syn = synapseclient.login(silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print(
            ".synapseConfig not found; please manually provide your",
            "Synapse Personal Access Token (PAT). You can generate"
            "one at https://www.synapse.org/#!PersonalAccessTokens:0",
        )
        pat = getpass("Your Synapse PAT: ")
        syn = synapseclient.login(authToken=pat, silent=True)
    return syn


def get_args(resource: str) -> argparse.Namespace:
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description=f"Sync {resource} to the CCKP")
    parser.add_argument(
        "-m",
        "--manifest_id",
        type=str,
        default=CONFIG.get(resource).get("manifest"),
        help="Synapse ID of the manifest CSV file.",
    )
    parser.add_argument(
        "-t",
        "--portal_table_id",
        type=str,
        default=CONFIG.get(resource).get("portal_table"),
        help=(
            f"Sync to this specified table. (Default: "
            f"{CONFIG.get(resource).get('portal_table')})"
        ),
    )
    parser.add_argument(
        "-o",
        "--output_csv",
        type=str,
        default=f"./final_{resource}_table.csv",
        help="Filepath to output CSV.",
    )
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Output all logs and interim tables.",
    )
    parser.add_argument(
        "-np",
        "--noprint",
        action="store_true",
        help="Do not output CSV file.",
    )
    return parser.parse_args()


# TODO: check if we still need this function?
def sort_and_stringify_col(col: pd.Series) -> str:
    """Sort list col then join together as comma-separated string."""
    # Check column by looking at first row; if str, convert to list first.
    if isinstance(col.iloc[0], str):
        col = col.str.replace(", ", ",").str.split(",")
    return col.apply(lambda x: ",".join(map(str, sorted(x))))


def convert_to_stringlist(col: pd.Series) -> pd.Series:
    """Convert a string column to a list."""
    return col.str.replace(", ", ",").str.split(",")


def update_table(syn: synapseclient.Synapse, table_id: str, df: pd.DataFrame) -> None:
    """Update the portal table.

    Steps include:
        - creating a new table version
        - truncating the table
        - sync over rows from the latest manifest
    """

    today = datetime.today().strftime("%Y-%m-%d-%H-%M-%S")
    print(f"Creating new table version with label: {today}...")
    syn.create_snapshot_version(table_id, label=today)

    current_rows = syn.tableQuery(f"SELECT * FROM {table_id}")
    print(f"Syncing table with latest data (new_rows={len(df) - len(current_rows)})...\n")
    syn.delete(current_rows)
    new_rows = df.values.tolist()
    syn.store(synapseclient.Table(table_id, new_rows))


def get_manifest(resource: str) -> dict[str, dict[str, str]]:
    """Get the config dictionary for the portal tables."""
    return CONFIG.get(resource).get("manifest")

def translate_duo(code: str, dict: dict[str, str] = DUO_DICT) -> str:
    """Get the definition of a DUO code."""
    return dict[code]

def extract_map_repository(link: str, alias: str, dict: dict[str, str] = REPO_DICT, regex: str = REPO_REGEX):
    """Extract distinctive link elements and map to a repository name."""
    extracted_link = re.fullmatch(regex, link) if link != "Pending Annotation" else None
    core_link = "".join([g for g in extracted_link.groups()[3:] if g is not None]) if extracted_link is not None else "No extracted content"
    source_repo_link_list, source_repo_alias_list, source_repo = [], [], None
    
    for repo in dict.keys():
        if type(dict[repo]) == list:
            link_patterns, alias_patterns = dict[repo]
            
            link_patterns = link_patterns.split(",") if type(link_patterns) == str else link_patterns
            link_patterns = list(link_patterns) if type(link_patterns) == tuple else link_patterns
            link_patterns = [None] if link_patterns == None else link_patterns
            
            alias_patterns = alias_patterns.split(",") if type(alias_patterns) == str else alias_patterns
            alias_patterns = list(alias_patterns) if type(alias_patterns) == tuple else alias_patterns
            alias_patterns = [None] if alias_patterns == None else alias_patterns
            
            if link_patterns != [None]:
                for pattern in link_patterns:
                    if pattern is not None and pattern.strip() in core_link:
                        source_repo_link_list.append(repo)
            
            if alias_patterns != [None]:
                for pattern in alias_patterns:
                    if pattern is not None and pattern.strip() in alias:
                        source_repo_alias_list.append(repo)
    
    source_repo_link_set = set([s for s in source_repo_link_list if s is not None])
    source_repo_alias_set = set([s for s in source_repo_alias_list if s is not None])
    
    source_repo = "".join([r for r in source_repo_link_set if r is not None])
    if len(source_repo_alias_list) > 0:
        if source_repo not in source_repo_alias_set:
            print(f"\nRepository identification pattern mismatch:\nlink: {link}\nlink repository: {source_repo}\nalias: {alias}\nalias repositories: {source_repo_alias_set}\nUsing repo specified by link")
    else:
        print(f"\nRepository identification:\nlink: {link}\nalias: {alias}\nrepository: {source_repo}")

    if len(source_repo_link_set) == 0:
        source_repo = "No repository information provided"
        print(f"\nNo pattern match found for:\nlink: {link}\nalias: {alias}")
    
    return source_repo

def identify_download_type(syn: synapseclient.Synapse, row: pd.Series, source_repo: str):
    """Determine download type and return download id if Synapse hosted or indexed"""

    entity = syn.get(row["DatasetView_id"], downloadFile=False)

    if entity.entityType == "Dataset":
        indexed = True
        download_id = row["DatasetView_id"]
    else:
        indexed = False
        download_id = None
    
    if source_repo == "Synapse" and indexed is True:
        download_type = "Synapse Hosted"
    elif source_repo != "Synapse" and indexed is True:
        download_type = "Synapse Indexed"
    elif source_repo != "Synapse" and indexed is False:
        download_type = "Externally Hosted"
    
    return download_type, download_id
    