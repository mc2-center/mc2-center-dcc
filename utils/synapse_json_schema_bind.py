"""synapse_json_schema_bind.py

This script will create and bind a JSON schema to an entity

Usage:

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
import requests


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        type=str,
        help="Synapse Id of an entity to which a schema will be bound",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="The Component name of the schema that will be bound to the requested entity",
        required=False
    )
    parser.add_argument(
        "-v",
        type=str,
        help="The release version of the schema",
        required=True
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Path to a CSV file with entity Synapse Ids and Components on each row",
        required=False
    )
    return parser.parse_args()


def get_schema_organization(service):

    org_name = "Multi Consortia Coordinating Center"
    org_name_nospace = org_name.strip(" ")

    print(f"Creating organization: {org_name}")

    try:
        schema_org = service.JsonSchemaOrganization(name = org_name)
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(f"Organization {org_name} already exists, getting info now...")
        schema_org = service.get_organization(organization_name = org_name)
    
    #schema_org.set_acl("3450948, 3458480")

    return service, schema_org, org_name_nospace


def register_json_schema(org, schema_type, schema_json, version, org_name):
    
    uri = "-".join([org_name,schema_type,version])

    try:
        schema = org.create_json_schema(schema_json, schema_type, semantic_version=version)
        uri = schema.uri
        print(f"JSON schema {schema.name} {version} was successfully registered.")
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(f"JSON schema {schema_type} {version} was previously registered. Accessing schema now...")
        
    return uri


def bind_schema_to_entity(service, schema_uri, dataset_id):

    service.bind_json_schema(schema_uri, dataset_id)

   
def get_schema_from_url(component, version):

    #base_schema_url = "".join(["https://raw.githubusercontent.com/mc2-center/data-models/refs/tags/v", version, "/json_schemas/"])
    base_schema_url = "".join(["https://raw.githubusercontent.com/mc2-center/data-models/refs/heads/refactor-add-cds-imaging/json_schemas/"])

    component_json_name = ".".join(["mc2", component, "schema", "json"])
    
    schema_url = "".join([base_schema_url, component_json_name])

    source_schema = requests.get(schema_url) 

    schema_json = source_schema.json()

    print(f"JSON schema {component} {version} successfully acquired from repository")

    return schema_json


def get_register_bind_schema(component, version, target, org_name, org, service):
    
    schema_json = get_schema_from_url(component, version)
    print(f"Registering JSON schema {component} {version}")

    uri = register_json_schema(org, component, schema_json, version, org_name)
    
    bound_schema = bind_schema_to_entity(service, uri, target)
    print(f"\nSchema {component} {version} successfully bound to entity {target}")


def main():

    syn = (
        synapseclient.login()
    ) 

    args = get_args()

    target, component, version, sheet= args.t, args.c, args.v, args.s
    
    schema_service = synapseclient.services.json_schema.JsonSchemaService(syn)

    service, org, org_name = get_schema_organization(schema_service)
    
    if sheet:
        idSet = pd.read_csv(sheet, header=None)
        if idSet.iat[0,0] == "entity" and idSet.iat[0,1] == "component":
            print(f"\nInput sheet read successfully!\n\nBinding schemas now...")
            idSet = idSet.iloc[1:,:]
            count = 0
            for row in idSet.itertuples(index=False):
                target = row[0]
                component = row[1]
                get_register_bind_schema(component, version, target, org_name, org, service)  
                count += 1
            print(f"\n\nDONE ✅\n{count} schemas bound")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else: #if no sheet provided, run process for one round of inputs only
        if target and component:
            get_register_bind_schema(component, version, target, org_name, org, service)
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")

if __name__ == "__main__":
    main()
