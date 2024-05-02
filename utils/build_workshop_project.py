"""
Create a Synapse project, configured for an MC2 Center resource sharing workshop.

author: orion.banks
"""

import synapseclient
from synapseclient import Project, Wiki, Folder, Team, File
import argparse
from random import randint
import pandas as pd


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Create workshop Synapse project")
    parser.add_argument(
        "-s",
        "--sheet",
        type=str,
        required=True,
        help=("Required. The path to a workshop config CSV file")
    )
    parser.add_argument(
        "-n",
        "--name",
        type=str,
        default=f"New Workshop Project {randint(100,999)}",
        help=(
            "The name to be applied to the Synapse project, if not provided in the workshop config"
        )
    )
    parser.add_argument(
        "-f",
        "--folders",
        action="store_true",
        help=(
            "If provided, attendee folders will be created in the new project using information from the config"
        )
    )
    parser.add_argument(
        "-c",
        "--content",
        action="store_true",
        help=(
            "If provided, workshop content will be added to the project using information from the config"
        )
    )
    parser.add_argument(
        "-t",
        "--team",
        action="store_true",
        help=(
            "If provided, a Synapse team will be created and connected to the workshop project"
        )
    )
    parser.add_argument(
        "-r",
        "--resources",
        action="store_true",
        help=(
            "If provided, sub folders will be populated with links, descriptions, and templates from the config"
        )
    )
    parser.add_argument(
        "-a",
        "--admin",
        type=str,
        default="3450948",
        help=(
            "The Synapse user or team ID that will have admin access. If a value is provided in the config, it will overwrite any argument provided here. (Default: 3450948, MC2 Admin)"
        )
    )
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


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
    ]
}


def _syn_prettify(name):
    """Prettify a name that will conform to Synapse naming rules.

    Names can only contain letters, numbers, spaces, underscores, hyphens,
    periods, plus signs, apostrophes, and parentheses.
    """
    valid = {38: "and", 58: "-", 59: "-", 47: "_"}
    return name.translate(valid)


def create_wiki_pages(
    syn, project_id, project_name, slides_link, worksheet_link, resource_link, dca_link, team_id
):
    """Create main Wiki page, activity sub wiki, and example resource wiki for the Project."""

    # Main Wiki page
    title = project_name

    content = "".join([f"""
# Welcome to the {title} Synapse Project!

### The intent of this workshop is to familiarize attendees with:
#### --> basic principles and motivations for sharing resources
#### --> core tools and processes available through the MC^2^ Center to support resource management and dissemination through the [Cancer Complexity Knowledge Portal](https://www.cancercomplexity.synapse.org/)

### If you haven't done so already, please be sure to join the workshop team using the button below!
""",
"${jointeam?teamId=",
f"{team_id}",
"&isChallenge=false&isSimpleRequestButton=false&isMemberMessage=Already a member&successMessage=Successfully joined&text=Join the team here&requestOpenText=Your request to join this team has been sent%2E}",
f"""
### Please visit the Resource Curation Activity subpage (left) for instructions on how to participate
""",
f"""
### You can access the slides for this workshop via [this link]({slides_link})
"""
    ])

    main_wiki = Wiki(title=title, owner=project_id, markdown=content)
    main_wiki = syn.store(main_wiki)

    # Sub-wiki page: Resource Curation Activity
    rcm_top = f"""
## The instructions below are an outline of the workshop resource sharing activity 
### To get started, please access the [resource annotation worksheet]({worksheet_link}), which contains step-by-step instructions and additional info.
**Note: The worksheet link above will create a copy of the document for you to work with** 

#### Step 1: Select the resource type you would like to work with from [this folder]({resource_link})

#### Step 2: Open the metadata manifest contained in your selected resource folder and fill in any missing information 

#### Step 3: Validate and submit the metadata through the [Data Curator App]({dca_link})
    """
    rcm_top_wiki = Wiki(
        title="Resource Curation Activity",
        owner=project_id,
        markdown=rcm_top,
        parentWikiId=main_wiki.id,
    )
    rcm_top_wiki = syn.store(rcm_top_wiki)

    folder_info = f"""
### Please select one of the example folders below to find resource descriptions and template manifests.
    """
    folder_wiki = Wiki(
        title="Workshop Example Resources", owner=resource_link, markdown=folder_info
    )

    folder_wiki = syn.store(folder_wiki)


def create_folders(
    syn, project_id, folder_name, resource_types, links, templates, descriptions
):
    """Create top-levels and add workshop content.

    workshop_i...workshop_n for a range of i to n
    example_resources
    workshop_table_contents
    """
    folder_list = ["example_resources", "workshop_table_contents"]

    sub_folders = resource_types.split(", ")
    links = links.split(", ")
    templates = templates.split(", ")
    descriptions = descriptions.split("#")

    working_folders = [
        "_".join([folder_name, str(folder_id)]) for folder_id in range(1,51)
    ]

    folder_list = folder_list + working_folders

    folder_dict = {}
    sub_dict = {}

    for name in folder_list:
        folder = Folder(name, parent=project_id)
        folder = syn.store(folder)
        folder_dict.update([(folder.name, folder.id)])

    for sub, link, temp, desc in zip(sub_folders, links, templates, descriptions):
        name = "_".join([sub, "example"])
        folder = Folder(name, parent=folder_dict["example_resources"])
        folder = syn.store(folder)
        sub_dict.update([(folder.name, (folder.id, link, temp, desc))])

    return folder_dict, sub_dict


def create_team(syn, project_id, team_name, access_type="edit"):
    """Create team for new grant project."""
    try:
        team_name = " ".join([team_name, "Attendees"])
        new_team = Team(name=team_name, canPublicJoin=False)
        new_team = syn.store(new_team)
        syn.setPermissions(
            project_id, principalId=new_team.id, accessType=PERMISSIONS.get(access_type)
        )
    except ValueError as err:
        if err.__context__.response.status_code == 409:
            print(f"Team already exists: {team_name}")
        else:
            print(f"Something went wrong! Team: {team_name}")
    
    return new_team.id


def populate_folders(syn, res_dict):
    """Add resource links, manifest links, and resource description wiki content to folders"""

    for (
        k,
        v,
    ) in res_dict.items():  # use syn.get and pull from exisiting materials in Synapse?
        res_name = "_".join([k, "link"])
        temp_name = "_".join([k, "manifest"])
        target, link, template, description = v
        resource = File(name=res_name, path=link, parent=target, synapseStore=False)
        manifest = File(
            name=temp_name, path=template, parent=target, synapseStore=False
        )
        res_wiki = Wiki(
            title=f"{k} Resource Details", owner=target, markdown=description
        )
        syn.store(resource)
        syn.store(manifest)
        syn.store(res_wiki)


def create_workshop_project(syn, args, inputs):
    """Create a new Synapse project for a workshop and populate with a wiki and folder content."""
    name = _syn_prettify(args.n)
    try:
        project = Project(name)
        project = syn.store(project)
        syn.setPermissions(
            project.id, principalId=args.a, accessType=PERMISSIONS.get("admin")
        )
        if args.folders:
            top_level, sub_level = create_folders(
                syn,
                project.id,
                inputs["folders"],
                inputs["resources"],
                inputs["links"],
                inputs["templates"],
                inputs["descriptions"]
            )

        if args.resources:
            populate_folders(syn, sub_level)

        if args.team:
            team_id = create_team(syn, project.id, args.n)

        if args.content:
            create_wiki_pages(
                syn,
                project.id,
                args.n,
                inputs["slides"],
                inputs["worksheet"],
                top_level["example_resources"],
                inputs["dca"],
                team_id
            )

    except synapseclient.core.exceptions.SynapseHTTPError:
        print(f"Project {name} could not be created")


def parse_config(csv):
    """Pull workshop info from CSV config and return as a dict"""
    config = pd.read_csv(csv)
    features = [(col, config.at[0, col]) for col in config.columns]
    detail_dict = {}
    for f in features:
        detail_dict.update([f])

    return detail_dict


def main():
    """Main function."""
    syn = synapseclient.Synapse()
    syn.login(silent=True)
    args = get_args()
    inputs = parse_config(args.sheet)
    print(inputs)

    if inputs["name"]:
        args.n = inputs["name"]

    if inputs["admin"]:
        args.a = inputs["admin"]

    if args.dryrun:
        print("\u26A0", "WARNING:", "dryrun is enabled (no updates will be done)\n")
        print()
    else:
        print(f"Creating new workshop project {args.n}...")
        create_workshop_project(syn, args, inputs)
    print("DONE ✓")


if __name__ == "__main__":
    main()
