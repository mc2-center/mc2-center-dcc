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
from synapseclient.models import Table, Team as TeamModel
from synapseclient.operations import find_entity_id

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


def _text(value):
    """Cell value as text: ``None`` and NaN become the empty string."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    return str(value)


def _split_people(value):
    """Comma-separated people cell to a list; missing or empty gives ``[]``."""
    if isinstance(value, (list, tuple, set)):
        names = [_text(v) for v in value]
    else:
        names = _text(value).split(",")
    return [n.strip() for n in names if n.strip()]


def create_wiki_pages(syn, project_id, grant):
    """Create main Wiki page for the Project."""

    # Main Wiki page
    consortium = _text(grant["consortium"])
    grant_type = _text(grant["GrantType"])
    title = _text(grant["GrantInstitutionAlias"])
    institutions = _text(grant["GrantInstitutionName"])
    desc = _text(grant["GrantAbstract"])

    content = f"""### The {consortium} {grant_type} Research Project \\@ {title}

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
    pis = _split_people(grant["Investigator"])
    if pis:
        pi_markdown = "* " + "\n* ".join(pis)
        syn.store(
            Wiki(
                title="Project Investigators",
                owner=project_id,
                markdown=pi_markdown,
                parentWikiId=main_wiki.id,
            )
        )


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


def find_team_id(syn, team_name):
    """ID of the team named exactly ``team_name``, or None if there isn't one."""
    try:
        return TeamModel.from_name(team_name, synapse_client=syn).id
    except ValueError:  # "Can't find team"
        return None


def create_team(syn, project_id, grant, access_type="edit"):
    """Create (or reuse, if one already has the name) the team for a grant."""
    consortia = _join_listlike_col(grant["consortium"])
    center = _join_listlike_col(grant["GrantInstitutionAlias"])
    team_name = f"{consortia} {center} {grant['GrantType']} {grant['GrantNumber']}"
    team_id = find_team_id(syn, team_name)
    if team_id is not None:
        print(f"Reusing existing team: {team_name}")
    else:
        try:
            team_id = syn.store(Team(name=team_name, canPublicJoin=False)).id
        except ValueError as err:
            if err.__context__ is not None and err.__context__.response.status_code == 409:
                print(f"Team already exists: {team_name}")
            else:
                print(f"Something went wrong! Team: {team_name}")
            return None
    syn.setPermissions(
        project_id, principalId=team_id, accessType=PERMISSIONS.get(access_type)
    )
    return team_id


def _has_wiki(syn, project_id):
    """True if the project already has a root Wiki page."""
    try:
        syn.getWiki(project_id)
        return True
    except synapseclient.core.exceptions.SynapseHTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            return False
        raise


def create_grant_projects(syn, grants):
    """Create (or reuse) a Synapse project for each grant and populate it.

    A project that already exists under the grant's name (left by an earlier
    run that failed midway) is reused, and only the missing parts are added.
    Returns ``{GrantViewId: (project_id, team_id)}``; grants that could not
    be created map to ``("None", "None")``.
    """

    grants = grants.fillna("")
    grant_info_dict = {}
    for _, row in grants.iterrows():
        name = _syn_prettify(row["GrantName"])
        try:
            project_id = find_entity_id(name=name, parent=None, synapse_client=syn)
            if project_id is not None:
                print(f"Reusing existing project: {name} ({project_id})")
            else:
                project_id = syn.store(Project(name)).id
            syn.setPermissions(
                project_id, principalId=3450948, accessType=PERMISSIONS.get("admin")
            )

            if not _has_wiki(syn, project_id):
                create_wiki_pages(syn, project_id, row)
            create_folders(syn, project_id)
            team_id = create_team(syn, project_id, row)
            if team_id is None:
                raise synapseclient.core.exceptions.SynapseHTTPError(
                    f"No team for {name}"
                )
            grant_info_dict[row["GrantViewId"]] = (project_id, team_id)
        except synapseclient.core.exceptions.SynapseHTTPError as err:
            logger.error("Grant creation failed, skipping: %s (%s)", name, err)
            print(f"Skipping: {name}")
            grant_info_dict[row["GrantViewId"]] = ("None", "None")

    return grant_info_dict


def read_manifest(syn, manifest_id):
    """Read the grant manifest table into class-label (key) columns.

    Accepts both 13.1.0 and 14.0.0 headers: legacy headers are renamed
    (with a warning) before normalizing display names to keys.
    """
    manifest = model_contract.read_manifest(syn, manifest_id)
    manifest = model_contract.rename_legacy_headers(manifest, COMPONENT)
    return model_contract.normalize_headers(manifest, to="keys")


def resolve_consortium(syn, values):
    """Resolve ConsortiumKey values to ``", "``-joined display names.

    See ``utils.resolve_consortium_keys``; unknown values raise ``ValueError``.
    """
    cells = list(values)
    if not any(utils.split_cell(c) for c in cells):
        return pd.Series([""] * len(cells), index=values.index, dtype=object)
    names = utils.resolve_consortium_keys(syn, cells)
    return pd.Series([", ".join(n) for n in names], index=values.index, dtype=object)


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


def process_new_grants(new = None, current = None, dryrun = None, synapse_client = None, live_types = None):
    """Create projects for new grants; return all grants in staging shape.

    The result has class-label (key) columns plus the crosswalk's derived
    columns (``grantId``, ``consortium``, ``grantSynapseTeam``,
    ``grantSynapseProject``), ready for ``crosswalk.to_portal``.

    With ``live_types`` (see ``crosswalk.check_crosswalk``), the full frame is
    converted with placeholder project/team IDs *before* anything is created,
    so a conversion error aborts the run with nothing created. Grants whose
    project creation failed are left out of the result and listed in
    ``result.attrs["failed_grants"]``.
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
    failed_grants = []
    grant_info_dict = {
        grant: ("None", "None") for grant in new_grants["GrantViewId"].to_list() if grant
    }

    manifest["grantId"] = ""
    manifest["grantSynapseTeam"] = manifest["GrantSynapseTeam"]
    manifest["grantSynapseProject"] = manifest["GrantSynapseProject"]
    curr = portal_to_sources(curr_portal)

    def assemble(info):
        """All grants in staging shape, new rows carrying ``info``'s IDs."""
        # Add new Grant info to the current portal rows (which win on conflict).
        frame = (
            pd.concat([curr, manifest])
            .drop_duplicates(subset=["GrantViewId"])
            .reset_index(drop=True)
        )
        # Flatten list-like cells; add Project and Team info to new rows.
        for col in ["GrantThemeName", "GrantInstitutionAlias", "GrantInstitutionName", "consortium"]:
            frame[col] = frame[col].map(_list_to_string)
        for idx, row in frame.iterrows():
            if row["grantId"] == "":  # If row was added via new manifest
                grantId, teamId = info[row["GrantViewId"]]
                frame.at[idx, "grantId"] = grantId
                frame.at[idx, "grantSynapseTeam"] = f"https://www.synapse.org/#!Team:{teamId}"
                frame.at[idx, "grantSynapseProject"] = f"https://www.synapse.org/Synapse:{grantId}"
        return frame

    if live_types is not None and not new_grants.empty:
        # Dry-run the conversion with placeholder IDs; raises on bad data.
        placeholder = {g: ("syn0", "0") for g in grant_info_dict}
        crosswalk.to_portal(assemble(placeholder), COMPONENT, live_types)

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
            failed_grants = [
                g for g, ids in grant_info_dict.items()
                if any(i in ("None", "", None) for i in ids)
            ]

    new_manifest = assemble(grant_info_dict)
    if failed_grants:
        # Never write a grant that has no project: it would become a
        # permanent "None" row and never be retried.
        for g in failed_grants:
            logger.error("Grant %s was not created; leaving it out of the table", g)
        new_manifest = new_manifest[~new_manifest["GrantViewId"].isin(failed_grants)]
        new_manifest = new_manifest.reset_index(drop=True)
    new_manifest.attrs["failed_grants"] = failed_grants

    print("DONE ✓")

    return new_manifest


if __name__ == "__main__":
    process_new_grants()
