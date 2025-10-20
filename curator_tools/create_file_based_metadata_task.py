"""
Create a file view and CurationTask for schema-bound folders following the file-based metadata workflow.
Pre-Requisites:
    Requires conflicting versions of schematicpy and synapseclient.
    Install schematicpy dependencies first, then uninstall synapseclient and reinstall with pip install git+https://github.com/Sage-Bionetworks/synapsePythonClient.git@synpy-1653-metadata-tasks-and-recordsets
Usage:
    python create_file_view.py --folder-id syn12345678 --datatype MyDatatype.studyXYZ
    python create_file_view.py --folder-id syn12345678 --datatype MyDatatype.studyXYZ \\
        --instructions "Custom curation instructions"
    python create_file_view.py --folder-id syn12345678 --datatype MyDatatype.studyXYZ --no-wiki
Users can also set arguments using the global variables below,
  but CLI arguments are used first.
"""

import argparse
import warnings
from typing import Any, Optional

from synapseclient import Synapse  # type: ignore
from synapseclient import Wiki  # type: ignore
from synapseclient.core.exceptions import SynapseHTTPError  # type: ignore
from synapseclient.models import (  # type: ignore
    Column,
    ColumnType,
    EntityView,
    Folder,
    ViewTypeMask,
)
from synapseclient.models.curation import CurationTask, FileBasedMetadataTaskProperties

FOLDER_ID = ""  # The Synapse ID of the entity you want to create the file view and CurationTask for
ATTACH_WIKI = None  # Whether or not to attach the file view to the folder wiki. True or False
DATATYPE = ""  # Data type name for the CurationTask (required)
# Instructions for the curation task (required)
INSTRUCTIONS = ""


TYPE_DICT = {
    "string": ColumnType.STRING,
    "number": ColumnType.DOUBLE,
    "integer": ColumnType.INTEGER,
    "boolean": ColumnType.BOOLEAN,
}

LIST_TYPE_DICT = {
    "string": ColumnType.STRING_LIST,
    "integer": ColumnType.INTEGER_LIST,
    "boolean": ColumnType.BOOLEAN_LIST,
}


def create_json_schema_entity_view(
    syn: Synapse,
    synapse_entity_id: str,
    entity_view_name: str = "JSON Schema view",
) -> str:
    """
    Creates a Synapse entity view based on a JSON Schema that is bound to a Synapse entity
    This functionality is needed only temporarily. See note at top of module.
    Args:
        syn: A Synapse object thats been logged in
        synapse_entity_id: The ID of the entity in Synapse to bind the JSON Schema to
        entity_view_name: The name the crated entity view will have
    Returns:
        The Synapse id of the crated entity view
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    js_service = syn.service("json_schema")
    json_schema = js_service.get_json_schema(synapse_entity_id)
    org = js_service.JsonSchemaOrganization(
        json_schema["jsonSchemaVersionInfo"]["organizationName"]
    )
    schema_version = js_service.JsonSchemaVersion(
        org,
        json_schema["jsonSchemaVersionInfo"]["schemaName"],
        json_schema["jsonSchemaVersionInfo"]["semanticVersion"],
    )
    columns = _create_columns_from_json_schema(schema_version.body)
    view = EntityView(
        name=entity_view_name,
        parent_id=synapse_entity_id,
        scope_ids=[synapse_entity_id],
        view_type_mask=ViewTypeMask.FILE,
        columns=columns,
    ).store(synapse_client=syn)
    # This reorder is so that these show up in the front of the EntityView in Synapse
    view.reorder_column(name="createdBy", index=0)
    view.reorder_column(name="name", index=0)
    view.reorder_column(name="id", index=0)
    view.store(synapse_client=syn)
    return view.id


def create_or_update_wiki_with_entity_view(
    syn: Synapse,
    entity_view_id: str,
    owner_id: str,
    title: Optional[str] = None,
) -> Wiki:
    """
    Creates or updates a Wiki for an entity if the wiki exists or not.
    An EntityView query is added to the wiki markdown
    This functionality is needed only temporarily. See note at top of module.
    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the EntityView for the query
        owner_id: The ID of the entity in Synapse that the wiki will be created/updated
        title: The (new) title of the wiki to be created/updated
    Returns:
        The created Wiki object
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    entity = syn.get(owner_id)

    try:
        wiki = syn.getWiki(entity)
    except SynapseHTTPError:
        wiki = None
    if wiki:
        return update_wiki_with_entity_view(syn, entity_view_id, owner_id, title)
    return create_entity_view_wiki(syn, entity_view_id, owner_id, title)


def create_entity_view_wiki(
    syn: Synapse,
    entity_view_id: str,
    owner_id: str,
    title: Optional[str] = None,
) -> Wiki:
    """
    Creates a wiki with a query of an entity view
    This functionality is needed only temporarily. See note at top of module.
    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the entity view to make the wiki for
        owner_id: The ID of the entity in Synapse to put as owner of the wiki
        title: The title of the wiki to be created
    Returns:
        The created wiki object
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    content = (
        "${synapsetable?query=select %2A from "
        f"{entity_view_id}"
        "&showquery=false&tableonly=false}"
    )
    if title is None:
        title = "Entity View"
    wiki = Wiki(title=title, owner=owner_id, markdown=content)
    wiki = syn.store(wiki)
    return wiki


def update_wiki_with_entity_view(
    syn: Synapse, entity_view_id: str, owner_id: str, title: Optional[str] = None
) -> Wiki:
    """
    Updates a wiki to include a query of an entity view
    This functionality is needed only temporarily. See note at top of module.
    Args:
        syn: A Synapse object thats been logged in
        entity_view_id: The Synapse id of the entity view to make the query for
        owner_id: The ID of the entity in Synapse to put as owner of the wiki
        title: The title of the wiki to be updated
    Returns:
        The created wiki object
    """
    warnings.warn(
        "This function is a prototype, and could change or be removed at any point."
    )
    entity = syn.get(owner_id)
    wiki = syn.getWiki(entity)

    new_content = (
        "\n"
        "${synapsetable?query=select %2A from "
        f"{entity_view_id}"
        "&showquery=false&tableonly=false}"
    )
    wiki.markdown = wiki.markdown + new_content
    if title:
        wiki.title = title

    syn.store(wiki)
    return wiki


def _create_columns_from_json_schema(json_schema: dict[str, Any]) -> list[Column]:
    """Creates a list of Synapse Columns based on the JSON Schema type
    Arguments:
        json_schema: The JSON Schema in dict form
    Raises:
        ValueError: If the JSON Schema has no properties
        ValueError: If the JSON Schema properties is not a dict
    Returns:
        A list of Synapse columns based on the JSON Schema
    """
    properties = json_schema.get("properties")
    if properties is None:
        raise ValueError("The JSON Schema is missing a 'properties' field.")
    if not isinstance(properties, dict):
        raise ValueError(
            "The 'properties' field in the JSON Schema must be a dictionary."
        )
    columns = []
    for name, prop_schema in properties.items():
        column_type = _get_column_type_from_js_property(prop_schema)
        maximum_size = None
        if column_type == "STRING":
            maximum_size = 100
        if column_type in LIST_TYPE_DICT.values():
            maximum_size = 5

        column = Column(
            name=name,
            column_type=column_type,
            maximum_size=maximum_size,
            default_value=None,
        )
        columns.append(column)
    return columns


def _get_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema property.
    The JSON Schema should be valid but that should not be assumed.
    If the type can not be determined ColumnType.STRING will be returned.
    Args:
        js_property: A JSON Schema property in dict form.
    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # Enums are always strings in Synapse tables
    if "enum" in js_property:
        return ColumnType.STRING
    if "type" in js_property:
        if js_property["type"] == "array":
            return _get_list_column_type_from_js_property(js_property)
        return TYPE_DICT.get(js_property["type"], ColumnType.STRING)
    # A oneOf list usually indicates that the type could be one or more different things
    if "oneOf" in js_property and isinstance(js_property["oneOf"], list):
        return _get_column_type_from_js_one_of_list(js_property["oneOf"])
    return ColumnType.STRING


def _get_column_type_from_js_one_of_list(js_one_of_list: list[Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema oneOf list.
    Items in the oneOf list should be dicts, but that should not be assumed.
    Args:
        js_one_of_list: A list of items to check for type
    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    # items in a oneOf list should be dicts
    items = [item for item in js_one_of_list if isinstance(item, dict)]
    # Enums are always strings in Synapse tables
    if [item for item in items if "enum" in item]:
        return ColumnType.STRING
    # For Synapse ColumnType we can ignore null types in JSON Schemas
    type_items = [item for item in items if "type" in item if item["type"] != "null"]
    if len(type_items) == 1:
        type_item = type_items[0]
        if type_item["type"] == "array":
            return _get_list_column_type_from_js_property(type_item)
        return TYPE_DICT.get(type_item["type"], ColumnType.STRING)
    return ColumnType.STRING


def _get_list_column_type_from_js_property(js_property: dict[str, Any]) -> ColumnType:
    """
    Gets the Synapse column type from a JSON Schema array property
    Args:
        js_property: A JSON Schema property in dict form.
    Returns:
        A Synapse ColumnType based on the JSON Schema type
    """
    if "items" in js_property and isinstance(js_property["items"], dict):
        # Enums are always strings in Synapse tables
        if "enum" in js_property["items"]:
            return ColumnType.STRING_LIST
        if "type" in js_property["items"]:
            return LIST_TYPE_DICT.get(
                js_property["items"]["type"], ColumnType.STRING_LIST
            )

    return ColumnType.STRING_LIST


def create_file_view(
    folder_id: str,
    attach_wiki: bool,
    datatype: str,
    instructions: str
) -> tuple[str, str]:
    """
    Create a file view for a schema-bound folder using schematic.
    Args:
        folder_id: The Synapse Folder ID to crate the file view for
        attach_wiki (bool): Wether or not to attack a Synapse Wiki
        datatype (str): Data type name for the CurationTask (required)
        instructions (str): Instructions for the curation task (required)
    Returns:
        A tuple:
          The first item is Synapse ID of the entity view created
          The second item is the task ID of the curation task created
    """
    syn = Synapse()
    syn.login()

    syn.logger.info("Attempting to create entity view.")
    try:
        entity_view_id = create_json_schema_entity_view(
            syn=syn,
            synapse_entity_id=folder_id
        )
    except Exception as e:
        msg = f"Error creating entity view: {str(e)}"
        syn.logger.error(msg)
        raise e
    syn.logger.info("Created entity view.")

    if attach_wiki:
        syn.logger.info("Attempting to attach wiki.")
        try:
            create_or_update_wiki_with_entity_view(
                syn=syn,
                entity_view_id=entity_view_id,
                owner_id=folder_id
            )
        except Exception as e:
            msg = f"Error creating wiki: {str(e)}"
            syn.logger.error(msg)
            raise e
        syn.logger.info("Wiki attached.")

    # Validate that the folder has an attached JSON schema
    # The datatype parameter is now required and used directly for the CurationTask.

    js = syn.service("json_schema")
    syn.logger.info("Attempting to get the attached schema.")
    try:
        js.get_json_schema_from_entity(folder_id)
    except Exception as e:
        msg = "Error getting the attached schema."
        syn.logger.exception(msg)
        raise e
    syn.logger.info("Schema retrieval successful")

    # Use the provided datatype (required parameter)
    task_datatype = datatype

    syn.logger.info("Attempting to get the Synapse ID of the provided folders project.")
    try:
        entity = Folder(folder_id).get(synapse_client=syn)
        parent = syn.get(entity.parent_id)
        project = None
        while not project:
            if parent.concreteType == "org.sagebionetworks.repo.model.Project":
                project = parent
                break
            parent = syn.get(parent.parentId)
    except Exception as e:
        msg = "Error getting the Synapse ID of the provided folders project}"
        syn.logger.exception(msg)
        raise e
    syn.logger.info("Got the Synapse ID of the provided folders project.")

    syn.logger.info("Attempting to create the CurationTask.")
    try:
        task = CurationTask(
            data_type=task_datatype,
            project_id=project.id,
            instructions=instructions,
            task_properties=FileBasedMetadataTaskProperties(
                upload_folder_id=folder_id,
                file_view_id=entity_view_id,
            )
        ).store(synapse_client=syn)
    except Exception as e:
        msg = f"Error creating the CurationTask.: {str(e)}"
        syn.logger.error(msg)
        raise e
    syn.logger.info("Created the CurationTask.")

    return (entity_view_id, task.task_id)


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Create file views for schema-bound folders"
    )
    parser.add_argument(
        '--folder-id',
        type=str,
        # required=True,
        help='Synapse folder ID'
    )
    parser.add_argument(
        '--datatype',
        type=str,
        help='Data type name for the CurationTask (required)'
    )
    parser.add_argument(
        '--instructions',
        type=str,
        help='Instructions for the curation task (required)'
    )
    parser.add_argument(
        '--no-wiki',
        action='store_false',
        help='Do not attach view to folder wiki'
    )
    args = parser.parse_args()

    if args.folder_id is not None:
        folder_id = args.folder_id
    elif FOLDER_ID:
        folder_id = FOLDER_ID
    else:
        raise ValueError("folder_id must be provided via CLI or global in script")

    if args.datatype is not None:
        datatype = args.datatype
    elif DATATYPE:
        datatype = DATATYPE
    else:
        raise ValueError("datatype must be provided via CLI argument --datatype or set in global variable DATATYPE")

    if args.instructions is not None:
        instructions = args.instructions
    elif INSTRUCTIONS:
        instructions = INSTRUCTIONS
    else:
        raise ValueError(
            "instructions must be provided via CLI argument --instructions or set in global variable INSTRUCTIONS"
        )

    if not args.no_wiki:
        attach_wiki = False
    elif ATTACH_WIKI is not None:
        attach_wiki = ATTACH_WIKI
    else:
        attach_wiki = True

    entity_view_id, curation_task_id = create_file_view(
        folder_id=folder_id,
        attach_wiki=attach_wiki,
        datatype=datatype,
        instructions=instructions
    )
    print(f"Wiki attached: {attach_wiki}")
    print(f"View ID: {entity_view_id}")
    print(f"Task ID: {curation_task_id}")


if __name__ == "__main__":
    main()