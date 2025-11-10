"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will create a new Synapse Project with pre-filled
Wikis and Folders for each new grant found, as well as a Synapse
team with "Can edit" permissions.
"""

import argparse
import pandas as pd
import re

import synapseclient
from synapseclient import Project, Wiki, Folder, Team

PERMISSIONS = {
    "view": ["READ"],
    "download": ["READ", "DOWNLOAD"],
    "edit": ["READ", "DOWNLOAD", "CREATE", "UPDATE"],
    "edit_delete": ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"],
    "admin": [
        "READ",
        "DOWNLOAD",
        "CREATE",
        "UPDATE",
        "DELETE",
        "MODERATE",
        "CHANGE_SETTINGS",
        "CHANGE_PERMISSIONS",
    ],
}


def _syn_prettify(name):
    """Prettify a name that will conform to Synapse naming rules.

    Names can only contain letters, numbers, spaces, underscores, hyphens,
    periods, plus signs, apostrophes, and parentheses.
    """
    valid = {38: "and", 58: "-", 59: "-", 47: "_"}
    return name.translate(valid)


def _join_listlike_col(col, join_by="_", delim=","):
    """Join list-like column values by specified value.

    Expects a list, but if string is given, then split (and strip
    whitespace) by delimiter first.
    """
    if isinstance(col, str):
        col = [el.strip() for el in col.split(delim)]
    return join_by.join(col).replace("'", "")


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new grants to the CCKP")
    parser.add_argument(
        "-m",
        "--manifest",
        type=str,
        default="syn53259587",
        help=("Synapse ID to the manifest table/fileview." "(Default: syn53259587)"),
    )
    parser.add_argument(
        "-t",
        "--portal_table",
        type=str,
        default="syn21918972",
        help=("Add grants to this specified table. " "(Default: syn21918972)"),
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def create_wiki_pages(syn, project_id, grant):
    """Create main Wiki page for the Project."""

    # Main Wiki page
    consortium = grant["Grant Consortium Name"]
    grant_type = grant["Grant Type"]
    title = grant["Grant Institution Alias"]
    institutions = grant["Grant Institution Name"]
    desc = grant["Grant Abstract"] or ""

    content = f"""### The {consortium} {grant_type} Research Project \@ {title}

#### List of Collaborating Institutions
{institutions}

#### Project Description
{desc}

"""
    content += (
        "->"
        "${buttonlink?text="
        "Back to Multi-Consortia Coordinating (MC2) Center"
        "&url=https%3A%2F%2Fwww%2Esynapse%2Eorg%2F%23%21Synapse%3Asyn7080714%2F}"
        "<-"
    )
    main_wiki = Wiki(title=grant["Grant Name"], owner=project_id, markdown=content)
    main_wiki = syn.store(main_wiki)

    # Sub-wiki page: Project Investigators
    pis = [pi.strip(" ") for pi in grant["Grant Investigator"].split(",")]
    pi_markdown = "* " + "\n* ".join(pis)
    pi_wiki = Wiki(
        title="Project Investigators",
        owner=project_id,
        markdown=pi_markdown,
        parentWikiId=main_wiki.id,
    )
    pi_wiki = syn.store(pi_wiki)


def create_folders(syn, project_id):
    """Create top-levels expected for resource and metadata management.

    Folders:
        - biospecimens
        - datasets
        - education
        - governance
        - individuals
        - models
        - publications
        - sharing_plans
        - studies
        - tools
    """
    for name in ["biospecimens", "datasets", "education", "governance", "individuals", "models", "publications", "sharing_plans", "studies", "tools"]:
        syn.store(Folder(name, parent=project_id))


def create_team(syn, project_id, grant, access_type="edit"):
    """Create team for new grant project."""
    consortia = _join_listlike_col(grant["Grant Consortium Name"])
    center = _join_listlike_col(grant["Grant Institution Alias"])
    team_name = f"{consortia} {center} {grant['Grant Type']} {grant['Grant Number']}"
    try:
        new_team = Team(name=team_name, canPublicJoin=False)
        new_team = syn.store(new_team)
        syn.setPermissions(
            project_id, principalId=new_team.id, accessType=PERMISSIONS.get(access_type)
        )
        return new_team.id
    except ValueError as err:
        if err.__context__.response.status_code == 409:
            print(f"Team already exists: {team_name}")
        else:
            print(f"Something went wrong! Team: {team_name}")
        return None


def create_grant_projects(syn, grants):
    """Create a new Synapse project for each grant and populate its Wiki."""

    grant_info_dict = {}
    for _, row in grants.iterrows():
        name = _syn_prettify(row["Grant Name"])
        try:
            project = Project(name)
            project = syn.store(project)
            syn.setPermissions(
                project.id, principalId=3450948, accessType=PERMISSIONS.get("admin")
            )

            create_wiki_pages(syn, project.id, row)
            create_folders(syn, project.id)
            team_id = create_team(syn, project.id, row)
            grant_info_dict[row["GrantView_id"]] = (project.id, team_id)
        except synapseclient.core.exceptions.SynapseHTTPError:
            print(f"Skipping: {name}")
            grant_info_dict[row["GrantView_id"]] = ("None", "None")
    
    return grant_info_dict


def process_new_grants(new = None, current = None, dryrun = None):
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    args = get_args()
    
    manifest = new if new is not None else args.manifest
    portal_table = current if current is not None else args.portal_table
    dryrun = dryrun if dryrun is not None else args.dryrun

    manifest = syn.tableQuery(f"SELECT * FROM {manifest}").asDataFrame()
    curr_manifest = syn.tableQuery(f"SELECT * FROM {portal_table}").asDataFrame()
    curr_grants = curr_manifest.grantNumber.to_list()

    # Generate manifest containing grants not currently on CCKP
    new_grants = manifest[~manifest["Grant Number"].isin(curr_grants)]
    grant_info_dict = {grant : ("None", "None") for grant in new_grants["Grant Number"].to_list() if grant}
    
    # Only add grants not currently in the Grants table.
    if new_grants.empty:
        print("No new grants found!")
    else:
        print(f"{len(new_grants)} new grants found!\n")
        if dryrun:
            print("\u26A0", "WARNING:", "dryrun is enabled (no updates will be done)\n")
            print(new_grants)
        else:
            print("Adding new grants...")
            grant_info_dict = create_grant_projects(syn, new_grants)
            
    manifest = manifest.rename(columns={
        "GrantView_id" : "grantViewId",
        "Grant Name" : "grantName",
        "Grant Number" : "grantNumber",
        "Grant Abstract" : "abstract",
        "Grant Type" : "grantType",
        "Grant Theme Name" : "theme",
        "Grant Institution Name" : "institutionAlias",
        "Grant Institution Alias" : "grantInstitution",
        "Grant Investigator" : "investigator",
        "Grant Consortium Name" : "consortium",
        "Grant Start Date" : "grantStartDate",
        "NIH RePORTER Link" : "nihReporterLink",
        "Duration of Funding" : "durationOfFunding",
        "Embargo End Date" : "embargoEndDate",
        "Grant Synapse Team" : "grantSynapseTeam",
        "Grant Synapse Project" : "grantSynapseProject"
        })
    
    manifest["grantId"] = ""

    # Add new Grant info to current manifest
    col_order = [
        "grantId",
        "grantViewId",
        "grantName",
        "grantNumber",
        "abstract",
        "grantType",
        "theme",
        "institutionAlias",
        "grantInstitution",
        "investigator",
        "consortium",
        "grantStartDate",
        "nihReporterLink",
        "durationOfFunding",
        "embargoEndDate",
        "grantSynapseTeam",
        "grantSynapseProject"
        ]
    manifest = manifest[col_order]
    
    new_manifest = pd.concat([curr_manifest, manifest]).drop_duplicates(subset=["grantViewId"]).reset_index()

    # Add Project and Team info to updated manifest
    for _, row in new_manifest.iterrows():
        for col in ["theme", "institutionAlias", "grantInstitution", "consortium"]:
            values = re.findall(r"\'(.*?)\'", "".join(row[col])) # extract values from string lists
            value_string = ", ".join(values) if values else ", ".join(row[col])
            new_manifest.at[_, col] = value_string
        if row["grantId"] == "":  # If row was added via new manifest
            grantId, teamId = grant_info_dict[row["grantViewId"]]
            new_manifest.at[_, "grantId"] = grantId
            new_manifest.at[_, "grantSynapseTeam"] = f"https://www.synapse.org/#!Team:{teamId}"
            new_manifest.at[_, "grantSynapseProject"] = f"https://www.synapse.org/Synapse:{grantId}"
            
    print("DONE ✓")
    
    return new_manifest


if __name__ == "__main__":
    process_new_grants()
