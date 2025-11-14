"""synapse_json_schema_bind.py

This script registers and binds a JSON schema to a Synapse entity using the Synapse client.
It can access the JSON schema from a provided URL or file path, register it under a specified
organization, and bind it to a target Synapse entity.
Usage:
python synapse_json_schema_bind.py [options]

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
import requests
import json


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        type=str,
        default=None,
        help="Synapse Id of an entity to which a schema will be bound.",
        required=False
    )
    parser.add_argument(
        "-l",
        type=str,
        default=None,
        help="The URL for the JSON schema to be bound to the requested entity.",
        required=False
    )
    parser.add_argument(
        "-p",
        type=str,
        default=None,
        help="The file path for the JSON schema to be bound to the requested entity.",
        required=False
    )
    parser.add_argument(
        "-n",
        type=str,
        default="Example Organization",
        help="The name of the organization with which the JSON schema should be associated. Default: 'Example Organization'.",
        required=False
    )
    parser.add_argument(
        "-ar",
        action="store_true",
        help="Indicates if the schema includes Access Requirement information.",
        required=False,
        default=None
    )
    parser.add_argument(
        "--no_bind",
        action="store_true",
        help="Indicates the schema should not be bound to the entity.",
        required=False,
        default=None
    )
    return parser.parse_args()


def get_schema_organization(service, org_name: str) -> tuple:
    """
    Access or create a JSON schema organization in Synapse.
    Args:
        service: Synapse JSON schema service.
        org_name (str): Name of the organization.
    Returns:
        tuple: (service, schema organization object, organization name)
    """
    
    print(f"Creating organization: {org_name}")

    try:
        schema_org = service.JsonSchemaOrganization(name = org_name)
        schema_org.create()
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(f"\nOrganization {org_name} already exists, getting info now...")
        schema_org = service.get_organization(organization_name = org_name)
    
    return service, schema_org, org_name


def register_json_schema(org, schema_type: str, schema_json: json, version: str, schema_org_name: str) -> str:
    """
    Register a JSON schema in Synapse under the specified organization.
    Args:
        org: Synapse JSON schema organization object.
        schema_type (str): Type of the schema (e.g., AccessRequirement, DatasetView).
        schema_json (json): JSON schema to register.
        version (str): Version of the schema (e.g., v1.0.0).
        schema_org_name (str): Name of the organization.
    Returns:
        str: Registered schema
    Notes:
        Expected uri format: [schema_org_name]-[schema_type]-[num_version]
    
        Example uri: ExampleOrganization-CA987654AccessRequirement-2.0.0
    """
    
    num_version = version.split("v")[1]

    uri = "-".join([schema_org_name.replace(" ", ""), schema_type,num_version])

    try:
        schema = org.create_json_schema(schema_json, schema_type, semantic_version=num_version)
        uri = schema.uri
        print(f"JSON schema {uri} was successfully registered.")
    except synapseclient.core.exceptions.SynapseHTTPError as error:
        print(error)
        print(f"JSON schema {uri} was previously registered and will not be updated.\n")
    
    print(f"\nSchema is available at https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{uri}\nThe schema can be referenced using the id: {uri}")
    
    return uri


def bind_schema_to_entity(syn, service, schema_uri: str, entity_id: str, component_type: str, includes_ar: bool):
    """
    Bind a registered JSON schema to a Synapse entity.
    Args:
        syn: Synapse client object.
        service: Synapse JSON schema service.
        schema_uri (str): URI of the registered JSON schema.
        entity_id (str): Synapse entity ID to bind the schema to.
        component_type (str): Type of the schema component.
        includes_ar (bool): Flag indicating if the schema includes Access Requirement information.
    Returns:
        None
    Note:
        Access Requirement schemas require a different binding method than other schema types.
        This includes any schema that has Access Requirement information, even if it is not an Access Requirement schema.
        Derived annotations must be enabled, to ensure accessRequirementIds can be applied based on annotations.
    """

    if component_type == "AccessRequirement" or includes_ar is not None:
        print(f"\nBinding AR schema {schema_uri}")
        request_body = {
            "entityId": entity_id,
            "schema$id": schema_uri,
            "enableDerivedAnnotations": True
            }
        syn.restPUT(
            f"/entity/{entity_id}/schema/binding", body=json.dumps(request_body)
        )
    
    else:
        print(f"\nBinding non-AR schema {schema_uri}")
        service.bind_json_schema(schema_uri, entity_id)
   
def get_schema_from_url(url: str, path: str) -> tuple[any, str, str, str]:
    """
    Access JSON schema from a URL or file path.
    Args:
        url (str): URL of the JSON schema.
        path (str): File path of the JSON schema.
    Returns:
        tuple: (schema JSON, component adjusted name, base component name, version)
    Notes:
        Filename must match expected conventions:
        Non-AR schema example: mc2.DatasetView-v1.0.0-schema.json
        AR schema example: MC2.AccessRequirement-CA000001-v3.0.2-schema.json
    """

    if url or path is not None:
        if url is not None:
            schema = url
            source_schema = requests.get(url)
            schema_json = source_schema.json()
        else:
            schema = path
            source_schema = open(path, "r")
            schema_json = json.load(source_schema)
            
        schema_info = schema.split("/")[-1]
        base_component = schema_info.split(".")[1].split("-")[0]
        
        if base_component == "AccessRequirement":
            component = "".join(schema_info.split("-")[0:-2]).split(".")[1]
            version = schema_info.split("-")[-2]
        else:
            component = base_component
            version = schema_info.split("-")[1]

    print(f"\nJSON schema {component} {version} successfully acquired from repository")

    return schema_json, component, base_component, version


def get_register_bind_schema(syn, target: str, schema_org_name: str, org, service, path, url, includes_ar: bool, no_bind: bool):
    """
    Get, register, and bind a JSON schema to a Synapse entity.
    Args:
        syn: Synapse client object.
        target (str): Synapse entity ID to bind the schema to.
        schema_org_name (str): Name of the organization.
        org: Synapse JSON schema organization object.
        service: Synapse JSON schema service.
        path (str): File path of the JSON schema.
        url (str): URL of the JSON schema.
        includes_ar (bool): Flag indicating if the schema includes Access Requirement information.
        no_bind (bool): Flag indicating if the schema should not be bound to the entity.
    Returns:
        None"""

    schema_json, component_adjusted, base_component, version = get_schema_from_url(url, path)
    print(f"\nRegistering JSON schema {component_adjusted} {version}\n")

    uri = register_json_schema(org, component_adjusted, schema_json, version, schema_org_name)

    if no_bind is None and target is not None:
        bind_schema_to_entity(syn, service, uri, target, base_component, includes_ar)
        print(f"\nSchema {component_adjusted} {version} successfully bound to entity {target}")
    else:
        print("\nSchema was not bound to an entity.")
    
    print("\nDONE ✅")
        

def main():

    args = get_args()
    
    syn = synapseclient.login()

    target, url, path, org_name, includes_ar, no_bind = args.t, args.l, args.p, args.n, args.ar, args.no_bind

    if no_bind is not None:
        print(f"Warning ❗❗❗ Schema will not be bound to the entity if one was provided.\n")

    if target is None:
        print(f"Warning ❗❗❗ No entity id provided. Schema will only be registered.\n")
    
    syn.get_available_services()

    schema_service = syn.service("json_schema")

    service, org, schema_org_name = get_schema_organization(schema_service, org_name)
    
    get_register_bind_schema(syn, target, schema_org_name, org, service, path, url, includes_ar, no_bind)

if __name__ == "__main__":
    main()
