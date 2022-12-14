"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new grants and its annotations to the
Grants portal table. A Synapse Project with pre-filled Wikis and
Folders will also be created for each new grant found.
"""

import argparse

import synapseclient
from synapseclient import Table, Project, Wiki, Folder


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new grants to the CCKP")
    parser.add_argument("-m", "--manifest",
                        type=str, default="syn35242677",
                        help=("Synapse ID to the manifest table/fileview."
                              "(Default: syn35242677)"))
    parser.add_argument("-t", "--portal_table",
                        type=str, default="syn21918972",
                        help=("Add grants to this specified table. "
                              "(Default: syn21918972)"))
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def create_wiki_pages(syn, project_id, grant_info):
    """Create main Wiki page for the Project."""

    # Main Wiki page
    consortium = grant_info['grantConsortiumName']
    grant_type = grant_info['grantType']
    title = grant_info['grantInstitutionAlias']
    institutions = grant_info['grantInstitutionName']
    desc = grant_info['grantAbstract'] or ""

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
    main_wiki = Wiki(title=grant_info['grantName'],
                     owner=project_id, markdown=content)
    main_wiki = syn.store(main_wiki)

    # Sub-wiki page: Project Investigators
    pis = [pi.lstrip(" ").rstrip(" ")
           for pi
           in grant_info['grantInvestigator'].split(",")]
    pi_markdown = "* " + "\n* ".join(pis)
    pi_wiki = Wiki(title="Project Investigators", owner=project_id,
                   markdown=pi_markdown, parentWikiId=main_wiki.id)
    pi_wiki = syn.store(pi_wiki)


def create_folders(syn, project_id):
    """Create top-levels expected by the DCA.

    Folders:
        - projects
        - publications
        - datasets
        - tools
    """
    syn.store(Folder("projects", parent=project_id))
    syn.store(Folder("publications", parent=project_id))
    syn.store(Folder("datasets", parent=project_id))
    syn.store(Folder("tools", parent=project_id))


def syn_prettify(name):
    """Prettify a name that will conform to Synapse naming rules.

    Names can only contain letters, numbers, spaces, underscores, hyphens,
    periods, plus signs, apostrophes, and parentheses.
    """
    valid = {38: 'and', 58: '-', 59: '-', 47: '_'}
    return name.translate(valid)


def create_grant_projects(syn, grants):
    """Create a new Synapse project for each grant and populate its Wiki.

    Returns:
        df: grants information (including their new Project IDs)
    """
    for _, row in grants.iterrows():
        name = syn_prettify(row['grantName'])
        try:
            project = Project(name)
            project = syn.store(project)
            syn.setPermissions(
                project.id, principalId=3450948,
                accessType=['CREATE', 'READ', 'UPDATE', 'DELETE', 'DOWNLOAD',
                            'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS', 'MODERATE'],
            )

            # Update grants table with new synId
            grants.at[_, 'grantId'] = project.id

            create_wiki_pages(syn, project.id, row)
            create_folders(syn, project.id)
        except synapseclient.core.exceptions.SynapseHTTPError:
            print(f"Skipping: {name}")
            grants.at[_, 'grantId'] = ""

    return grants


def sync_table(syn, grants, table):
    """Add grants annotations to the Synapse table.

    Assumptions:
        `grants` matches the same schema as `table`
    """
    schema = syn.get(table)

    # Reorder columns to match the table order.
    col_order = [
        'grantId', 'grantName', 'grantNumber', 'grantAbstract', 'grantType',
        'grantThemeName', 'grantInstitutionAlias', 'grantInstitutionName',
        'grantInvestigator', 'grantConsortiumName'
    ]
    grants = grants[col_order]

    # Convert columns into STRINGLIST.
    grants.loc[:, 'grantThemeName'] = grants.grantThemeName.str.split(", ")
    grants.loc[:, 'grantInstitutionName'] = grants.grantInstitutionName.str.split(
        ", ")
    grants.loc[:, 'grantInstitutionAlias'] = grants.grantInstitutionAlias.str.split(
        ", ")

    new_rows = grants.values.tolist()
    syn.store(Table(schema, new_rows))


def main():
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    args = get_args()

    manifest = syn.tableQuery(f"SELECT * FROM {args.manifest}").asDataFrame()
    curr_grants = (
        syn.tableQuery(f"SELECT grantNumber FROM {args.portal_table}")
        .asDataFrame()
        .grantNumber
        .to_list()
    )

    # Only add grants not currently in the Grants table.
    new_grants = manifest[~manifest.grantNumber.isin(curr_grants)]
    if new_grants.empty:
        print("No new grants found!")
    else:
        print(f"{len(new_grants)} new grants found!\n")
        if args.dryrun:
            print(u"\u26A0", "WARNING:",
                  "dryrun is enabled (no updates will be done)\n")
        else:
            print("Adding new grants...")
            added_grants = create_grant_projects(syn, new_grants)
            sync_table(syn, added_grants, args.portal_table)
    print("DONE ???")


if __name__ == "__main__":
    main()
