"""Add Grants to the Cancer Complexity Knowledge Portal (CCKP).

This script will create a new Synapse Project with pre-filled
Wikis and Folders for each new grant found, as well as a Synapse
team with "Can edit" permissions.
"""

import argparse
import ast
import logging
import pandas as pd

import synapseclient
from synapseclient import Project, Wiki, Folder, Team
from synapseclient.models import Table

import crosswalk
import model_contract
import utils

logger = logging.getLogger(__name__)

COMPONENT = "GrantView"

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
    consortium = grant["consortium"]
    grant_type = grant["GrantType"]
    title = grant["GrantInstitutionAlias"]
    institutions = grant["GrantInstitutionName"]
    desc = grant["GrantAbstract"] or ""

    content = f"""### The {consortium} {grant_type} Research Project \\@{title}

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
    main_wiki = Wiki(title=grant["GrantName"], owner=project_id, markdown=content)
    main_wiki = syn.store(main_wiki)

    # Sub-wiki page: Project Investigators
    pis = [pi.strip(" ") for pi in grant["Investigator"].split(",")]
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
    consortia = _join_listlike_col(grant["consortium"])
    center = _join_listlike_col(grant["GrantInstitutionAlias"])
    team_name = f"{consortia} {center} {grant['GrantType']} {grant['GrantNumber']}"
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
        name = _syn_prettify(row["GrantName"])
        try:
            project = Project(name)
            project = syn.store(project)
            syn.setPermissions(
                project.id, principalId=3450948, accessType=PERMISSIONS.get("admin")
            )

            create_wiki_pages(syn, project.id, row)
            create_folders(syn, project.id)
            team_id = create_team(syn, project.id, row)
            grant_info_dict[row["GrantViewId"]] = (project.id, team_id)
        except synapseclient.core.exceptions.SynapseHTTPError:
            print(f"Skipping: {name}")
            grant_info_dict[row["GrantViewId"]] = ("None", "None")
    
    return grant_info_dict


def read_manifest(syn, manifest_id):
    """Read the grant manifest table into class-label (key) columns.

    Accepts both 13.1.0 and 14.0.0 headers: legacy headers are renamed
    (with a warning) before normalizing display names to keys.
    """
    manifest = Table(id=manifest_id).query(
        query=f"SELECT * FROM {manifest_id}",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    )
    manifest = model_contract.rename_legacy_headers(manifest, COMPONENT)
    return model_contract.normalize_headers(manifest, to="keys")


def _split_values(value):
    """Comma-separated string or list to a list of stripped, non-empty values."""
    if isinstance(value, str):
        value = value.split(",")
    elif value is None or (not hasattr(value, "__iter__") and pd.isna(value)):
        return []
    return [v.strip() for v in value if isinstance(v, str) and v.strip()]


def resolve_consortium(syn, values):
    """Resolve ConsortiumKey values to Consortium display names.

    Each cell may hold several comma-separated values. ``program.*`` IDs are
    resolved with ``utils.consortium_display_names``; a value that is already
    a display name known to the Consortium table (the live manifest today)
    passes through unchanged. Any other value raises ``ValueError``, listing
    every unknown value. Returns a Series of ``", "``-joined display names.
    """
    cells = [_split_values(v) for v in values]
    tokens = {t for cell in cells for t in cell}
    if not tokens:
        return pd.Series([""] * len(cells), index=values.index, dtype=object)

    table_id = utils.CONFIG["consortium"]["table"]
    known = set(
        Table(id=table_id)
        .query(
            query=f"SELECT consortiumDisplayName FROM {table_id}",
            include_row_id_and_row_version=False,
            synapse_client=syn,
        )["consortiumDisplayName"]
    )
    ids = sorted(t for t in tokens if t.startswith("program."))
    unknown = sorted(t for t in tokens if not t.startswith("program.") and t not in known)
    resolved = {}
    try:
        resolved = dict(zip(ids, utils.consortium_display_names(syn, ids)))
    except ValueError as err:
        unknown_ids = [i for i in ids if f"'{i}'" in str(err)]
        raise ValueError(
            f"Unknown ConsortiumKey value(s): names {unknown}, IDs {unknown_ids}. {err}"
        ) from err
    if unknown:
        raise ValueError(
            f"Unknown ConsortiumKey value(s) {unknown}: neither a program.* ID nor "
            f"a display name in {table_id} ({sorted(known)})"
        )
    mapping = {t: resolved.get(t, t) for t in tokens}
    return pd.Series(
        [", ".join(mapping[t] for t in cell) for cell in cells],
        index=values.index,
        dtype=object,
    )


def _list_to_string(value):
    """Portal list cell (list or its string form) to a ``", "``-joined string."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                value = ast.literal_eval(stripped)
            except (ValueError, SyntaxError):
                return value
        else:
            return value
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(v) for v in value)
    return value


def portal_to_sources(curr_portal):
    """Rename portal-table columns back to crosswalk source columns."""
    renames = {
        portal_col: crosswalk.source_column(source)
        for source, portal_col, _ in crosswalk.CROSSWALK[COMPONENT]
        if portal_col in curr_portal.columns
    }
    return curr_portal.rename(columns=renames)


def process_new_grants(new = None, current = None, dryrun = None, synapse_client = None):
    """Create projects for new grants; return all grants in staging shape.

    The result has class-label (key) columns plus the crosswalk's derived
    columns (``grantId``, ``consortium``, ``grantSynapseTeam``,
    ``grantSynapseProject``), ready for ``crosswalk.to_portal``.
    """
    syn = synapse_client
    if syn is None:
        syn = synapseclient.Synapse()
        syn.login(silent=True)

    # Only parse CLI args when the caller (e.g. sync_grants.py) hasn't
    # already supplied all three values -- avoids re-parsing the caller's
    # own CLI flags when this is used as a library function.
    if new is None or current is None or dryrun is None:
        args = get_args()
        manifest_id = new if new is not None else args.manifest
        portal_table = current if current is not None else args.portal_table
        dryrun = dryrun if dryrun is not None else args.dryrun
    else:
        manifest_id = new
        portal_table = current

    manifest = read_manifest(syn, manifest_id)
    curr_portal = Table(id=portal_table).query(
        query=f"SELECT * FROM {portal_table}",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    )
    is_new = ~manifest["GrantNumber"].isin(curr_portal.grantNumber.to_list())

    # Grants already on the portal keep their portal values, so an unknown
    # consortium there is only worth a warning. New grants must resolve.
    manifest["consortium"] = ""
    if (~is_new).any():
        try:
            manifest.loc[~is_new, "consortium"] = resolve_consortium(
                syn, manifest.loc[~is_new, "ConsortiumKey"]
            )
        except ValueError as err:
            logger.warning("Existing grants have unresolved ConsortiumKey (unused): %s", err)
    if is_new.any():
        manifest.loc[is_new, "consortium"] = resolve_consortium(
            syn, manifest.loc[is_new, "ConsortiumKey"]
        )
    new_grants = manifest[is_new]
    grant_info_dict = {
        grant: ("None", "None") for grant in new_grants["GrantViewId"].to_list() if grant
    }

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

    manifest["grantId"] = ""
    manifest["grantSynapseTeam"] = manifest["GrantSynapseTeam"]
    manifest["grantSynapseProject"] = manifest["GrantSynapseProject"]

    # Add new Grant info to the current portal rows (which win on conflict).
    curr = portal_to_sources(curr_portal)
    new_manifest = (
        pd.concat([curr, manifest])
        .drop_duplicates(subset=["GrantViewId"])
        .reset_index(drop=True)
    )

    # Flatten list-like cells; add Project and Team info to new rows.
    for col in ["GrantThemeName", "GrantInstitutionAlias", "GrantInstitutionName", "consortium"]:
        new_manifest[col] = new_manifest[col].map(_list_to_string)
    for idx, row in new_manifest.iterrows():
        if row["grantId"] == "":  # If row was added via new manifest
            grantId, teamId = grant_info_dict[row["GrantViewId"]]
            new_manifest.at[idx, "grantId"] = grantId
            new_manifest.at[idx, "grantSynapseTeam"] = f"https://www.synapse.org/#!Team:{teamId}"
            new_manifest.at[idx, "grantSynapseProject"] = f"https://www.synapse.org/Synapse:{grantId}"

    print("DONE ✓")

    return new_manifest


if __name__ == "__main__":
    process_new_grants()
