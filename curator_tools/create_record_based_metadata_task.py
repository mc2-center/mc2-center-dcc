"""
Generate and upload CSV templates as a RecordSet for record-based metadata, create a
CurationTask, and also create a Grid to bootstrap the ValidationStatistics.
Usage:
    python create_record_based_metadata_task.py --folder-id syn12345678 --dcc AD \\
        --datatype BiospecimenMetadataTemplate --schema_path path/to/schema.json \\
        --schema_uri schema_uri --upsert_keys specimenID \\
        --instructions "Please curate this metadata according to the schema requirements"
        
    # Multiple upsert keys:
    python create_record_based_metadata_task.py --folder-id syn12345678 --dcc AD \\
        --datatype BiospecimenMetadataTemplate --schema_uri schema_uri \\
        --upsert_keys specimenID participantID sampleDate
Users can also set arguments using the global variables below,
  but CLI arguments are used first.
"""

import argparse
import tempfile
import pandas as pd
from pprint import pprint
from typing import Dict, Any, List, Optional
import json

import synapseclient
from synapseclient import Synapse
from synapseclient.models import RecordSet, CurationTask, RecordBasedMetadataTaskProperties, Grid
from synapseclient.services.json_schema import JsonSchemaService

PROJECT_ID = "" # The Synapse ID of the project where the folder exists
FOLDER_ID = ""  # The Synapse ID of the folder to upload to
DCC = ""  # Data Coordination Center
DATATYPE = ""  # Data type name
SCHEMA_URI = ""  # JSON schema URI
SCHEMA_PATH = None  # Path to JSON schema file located on your machine, alternative to SCHEMA_URI
UPSERT_KEYS = []  # List of column names to use as upsert keys, e.g., ['specimenID', 'participantID']
# Instructions for the curation task (required)
INSTRUCTIONS = "These are my custom instructions to tell someone what to do"

def extract_property_titles(schema_data: Dict[str, Any]) -> List[str]:
    """
    Extract title fields from all properties in a JSON schema.
    Args:
        schema_data: The parsed JSON schema data
    Returns:
        List of title values from the properties
    """
    titles = []

    # Check if 'properties' exists in the schema
    if 'properties' not in schema_data:
        return titles

    properties = schema_data['properties']

    for property_name, property_data in properties.items():
        if isinstance(property_data, dict):
            if 'title' in property_data:
                titles.append(property_data['title'])
            else:
                titles.append(property_name)

    return titles


def create_dataframe_from_titles(titles: List[str]) -> pd.DataFrame:
    """
    Create an empty DataFrame with the extracted titles as column names.
    Args:
        titles: List of title strings to use as column names
    Returns:
        Empty DataFrame with titles as columns
    """
    if not titles:
        return pd.DataFrame()

    df = pd.DataFrame(columns=titles)
    return df


def extract_schema_properties_from_dict(schema_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Process a JSON schema dictionary and return a DataFrame with property titles as columns.
    Args:
        schema_data: The parsed JSON schema data as a dictionary
    Returns:
        DataFrame with property titles as columns
    """
    titles = extract_property_titles(schema_data)

    df = create_dataframe_from_titles(titles)

    return df


def extract_schema_properties_from_file(json_file_path: str) -> pd.DataFrame:
    """
    Process a JSON schema file and return a DataFrame with property titles as columns.
    Args:
        json_file_path: Path to the JSON schema file
    Returns:
        DataFrame with property titles as columns
    Raises:
        FileNotFoundError: If the JSON file doesn't exist
        json.JSONDecodeError: If the JSON file is malformed
        ValueError: If the file doesn't contain a valid schema structure
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            schema_data = json.load(file)

        return extract_schema_properties_from_dict(schema_data)

    except FileNotFoundError as e:
        raise FileNotFoundError(f"JSON schema file not found: {json_file_path}") from e
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in file '{json_file_path}': {e}", e.doc, e.pos)


def extract_schema_properties_from_web(syn: Synapse, schema_uri: str) -> pd.DataFrame:
    """
    Extract schema properties from a web-based JSON schema URI using Synapse.
    This function retrieves a JSON schema from a web URI through the Synapse platform
    and extracts property titles to create a DataFrame with those titles as columns.
    Args:
        syn: Authenticated Synapse client instance
        schema_uri: URI pointing to the JSON schema resource
    Returns:
        DataFrame with property titles from the schema as column names
    """
    try:
        org_name, schema_name, version = schema_uri.split("-")
    except ValueError as e:
        raise ValueError(
            f"Invalid schema URI format: {schema_uri}. Expected format 'org-name-schema.name.schema-version'.") from e

    js = JsonSchemaService(synapse=syn)
    schemas_list = js.list_json_schemas(organization_name=org_name)
    if not any(schema_name == s["schemaName"] for s in schemas_list):
        raise ValueError(f"Schema URI '{schema_uri}' not found in Synapse JSON schemas.")

    schema = js.get_json_schema_body(json_schema_uri=schema_uri)
    return extract_schema_properties_from_dict(schema)


def extract_schema(syn: Synapse, schema_path: Optional[str] = None, schema_uri: Optional[str] = None) -> pd.DataFrame:
    """
    Extract schema properties from either a local file or web URI.
    This function provides a unified interface for extracting JSON schema properties
    from different sources. It accepts either a local file path or a web URI and
    delegates to the appropriate extraction function.
    Args:
        syn: Authenticated Synapse client instance (required for web URI extraction)
        schema_path: Optional path to a local JSON schema file
        schema_uri: Optional URI pointing to a web-based JSON schema resource
    Returns:
        DataFrame with property titles from the schema as column names
    Raises:
        ValueError: If neither schema_path nor schema_uri is provided, or if both are provided
        FileNotFoundError: If schema_path is provided but the file doesn't exist
        json.JSONDecodeError: If the local schema file contains invalid JSON
        SynapseError: If there are issues retrieving the web-based schema
    Note:
        At least one of schema_path or schema_uri must be provided, if both are given the uri will be used.
    """
    if schema_uri:
        return extract_schema_properties_from_web(syn, schema_uri)
    elif schema_path:
        return extract_schema_properties_from_file(schema_path)
    else:
        raise ValueError("Either schema_path or schema_uri must be provided.")


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate and upload CSV templates for record-based metadata"
    )

    parser.add_argument('--folder_id', type=str, required=False,
                        help='Synapse folder ID for upload')
    parser.add_argument('--dcc', type=str, required=False,
                        help='Data Coordination Center')
    parser.add_argument('--datatype', type=str, required=False,
                        help='Data type name')
    parser.add_argument('--schema_uri', type=str, required=False, default=None,
                        help='JSON schema URI')
    parser.add_argument('--schema_path', type=str, required=False, default=None,
                        help='path to JSON schema')
    parser.add_argument('--upsert_keys', type=str, nargs='+', required=False,
                        help='Column names to use as upsert keys (one or more)')
    parser.add_argument('--instructions', type=str, required=False,
                        help='Instructions for the curation task (required)')

    args = parser.parse_args()

    # Use CLI arguments first, then fall back to constants
    folder_id = args.folder_id if args.folder_id is not None else FOLDER_ID
    dcc = args.dcc if args.dcc is not None else DCC
    datatype = args.datatype if args.datatype is not None else DATATYPE
    schema_uri = args.schema_uri if args.schema_uri is not None else SCHEMA_URI
    schema_path = args.schema_path if args.schema_path is not None else SCHEMA_PATH
    upsert_keys = args.upsert_keys if args.upsert_keys is not None else UPSERT_KEYS
    instructions = args.instructions if args.instructions is not None else INSTRUCTIONS

    # Validate required parameters
    if folder_id is None:
        raise ValueError("folder_id must be provided via CLI or global variable FOLDER_ID")
    if dcc is None:
        raise ValueError("dcc must be provided via CLI or global variable DCC")
    if datatype is None:
        raise ValueError("datatype must be provided via CLI or global variable DATATYPE")
    if upsert_keys is None:
        raise ValueError("upsert_keys must be provided via CLI or global variable UPSERT_KEYS")
    if instructions is None:
        raise ValueError("instructions must be provided via CLI or global variable INSTRUCTIONS")

    syn = synapseclient.Synapse()
    syn.login()

    template_df = extract_schema(syn=syn, schema_path=schema_path, schema_uri=schema_uri)
    syn.logger.info(f"Extracted schema properties and created template: {template_df.columns.tolist()}")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    try:
        with open(tmp.name, 'w') as f:
            template_df.to_csv(f, index=False)
    except Exception as e:
        syn.logger.error(f"Error writing template to temporary CSV file: {e}")
        raise e

    try:
        with open(tmp.name, 'r') as f:
            recordset_with_data = RecordSet(
                name=f"{dcc}_{datatype}_RecordSet",
                parent_id=folder_id,
                description=f"RecordSet for {dcc} {datatype}",
                path=f.name,
                upsert_keys=upsert_keys
            ).store(synapse_client=syn)
            recordset_id = recordset_with_data.id
            syn.logger.info(f"Created RecordSet with ID: {recordset_id}")
            pprint(recordset_with_data)
    except Exception as e:
        syn.logger.error(f"Error creating RecordSet in Synapse: {e}")
        raise e

    try:
        curation_task = CurationTask(
            data_type=datatype,
            project_id=PROJECT_ID,
            instructions=instructions,
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=recordset_id,
            )
        ).store(synapse_client=syn)
        syn.logger.info(
            f"Created CurationTask ({curation_task.task_id}) in folder {folder_id} for data type {datatype}")
        pprint(curation_task)
    except Exception as e:
        syn.logger.error(f"Error creating CurationTask in Synapse: {e}")
        raise e

    try:
        curation_grid: Grid = Grid(
            record_set_id=recordset_id,
        )
        curation_grid.create(synapse_client=syn)
        curation_grid = curation_grid.export_to_record_set(synapse_client=syn)
        syn.logger.info(f"Created Grid view for RecordSet ID: {recordset_id} for data type {datatype}")
        pprint(curation_grid)
    except Exception as e:
        syn.logger.error(f"Error creating Grid view in Synapse: {e}")
        raise e


if __name__ == "__main__":
    main()