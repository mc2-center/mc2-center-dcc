"""synapse_json_schema_bind.py

This script will create and bind a JSON schema to an entity

Usage:

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
        help="Synapse Id of an entity to which a schema will be bound.",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="The Component name of the schema that will be bound to the requested entity.",
        required=False
    )
    parser.add_argument(
        "-v",
        type=str,
        help="The release version of the schema. This should match the release version tag on GitHub.",
        required=True
    )
    parser.add_argument(
        "-g",
        type=str,
        help="Grant number associated with a duoCodeAR type schema, in CAxxxxxx format (e.g., CA274499).",
        required=False
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Path to a CSV file with entity Synapse Ids and Components on each row.",
        required=False
    )
    return parser.parse_args()


def get_schema_organization(service):

    org_name = "Multi Consortia Coordinating Center"
    
    print(f"Creating organization: {org_name}")

    try:
        schema_org = service.JsonSchemaOrganization(name = org_name)
        schema_org.create()
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(f"Organization {org_name} already exists, getting info now...")
        schema_org = service.get_organization(organization_name = org_name)
    
    return service, schema_org, org_name


def register_json_schema(org, schema_type, schema_json, version, schema_org_name):
    
    num_version = version.split("v")[1]

    uri = "-".join([schema_org_name,schema_type,num_version])

    try:
        schema = org.create_json_schema(schema_json, schema_type, semantic_version=num_version)
        uri = schema.uri
        print(f"JSON schema {uri} was successfully registered.")
    except synapseclient.core.exceptions.SynapseHTTPError as error:
        print(error)
        print(f"JSON schema {uri} was previously registered and will be bound to the entity.")
        
    return uri


def bind_schema_to_entity(syn, service, schema_uri, entity_id, component_type):

    if component_type != "duoCodeAR":
        print(f"Binding non-AR schema {schema_uri}")
        service.bind_json_schema(schema_uri, entity_id)

    else:
        print(f"Binding AR schema {schema_uri}")
        request_body = {
            "entityId": entity_id,
            "schema$id": schema_uri,
            "enableDerivedAnnotations": True
            }
        syn.restPUT(
            f"/entity/{entity_id}/schema/binding", body=json.dumps(request_body)
        )

   
def get_schema_from_url(component, version, grant):

    #base_schema_url = "".join(["https://raw.githubusercontent.com/mc2-center/data-models/refs/tags/v", version, "/json_schemas/"])
    base_schema_url = "".join(["https://raw.githubusercontent.com/mc2-center/data-models/refs/heads/136-173-dataset-schema/json_schemas/"])

    if grant is not None and component == "duoCodeAR":
        component = "".join([grant, component])
    
    component_json_name = ".".join(["mc2", component, "schema", "json"])
    
    schema_url = "".join([base_schema_url, component_json_name])

    source_schema = requests.get(schema_url) 

    schema_json = source_schema.json()

    print(f"JSON schema {component} {version} successfully acquired from repository")

    return schema_json, component


def get_register_bind_schema(syn, component, grant, version, target, schema_org_name, org, service):
    
    schema_json, component_adjusted = get_schema_from_url(component, version, grant)
    print(f"Registering JSON schema {component_adjusted} {version}")

    uri = register_json_schema(org, component_adjusted, schema_json, version, schema_org_name)
    
    bound_schema = bind_schema_to_entity(syn, service, uri, target, component)
    print(f"\nSchema {component_adjusted} {version} successfully bound to entity {target}")


def main():

    syn = (
        synapseclient.Synapse()
    )
    syn.login()

    args = get_args()

    target, component, version, grant, sheet= args.t, args.c, args.v, args.g, args.s
    
    syn.get_available_services()

    schema_service = syn.service("json_schema")

    service, org, schema_org_name = get_schema_organization(schema_service)
    
    if sheet:
        idSet = pd.read_csv(sheet, header=None)
        if idSet.iat[0,0] == "entity" and idSet.iat[0,1] == "component":
            print(f"\nInput sheet read successfully!\n\nBinding schemas now...")
            idSet = idSet.iloc[1:,:]
            count = 0
            for row in idSet.itertuples(index=False):
                target = row[0]
                component = row[1]
                get_register_bind_schema(syn, component, grant, version, target, schema_org_name, org, service)  
                count += 1
            print(f"\n\nDONE ✅\n{count} schemas bound")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else: #if no sheet provided, run process for one round of inputs only
        if target and component:
            get_register_bind_schema(syn, component, grant, version, target, schema_org_name, org, service)
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")

if __name__ == "__main__":
    main()
