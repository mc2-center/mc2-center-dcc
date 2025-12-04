"""
Query the Synapse schema registry table to retrieve Schema URIs based on DCC and datatype.
This script queries the schema registry table at syn69735275 to find matching schemas
based on the provided DCC (Data Coordination Center) and datatype parameters.
Results are sorted by version and the URI is returned.
Usage:
    python query_schema_registry.py --dcc ad --datatype IndividualAnimalMetadataTemplate
    
    # Or use the global variables in the script
    python query_schema_registry.py
Users can also set arguments using the global variables below,
but CLI arguments take precedence.
"""

import argparse
from typing import List, Optional
from synapseclient import Synapse
from synapseclient.models import Table

# Global variables - set these if you don't want to use command line arguments
DCC = ""  # Data Coordination Center (e.g., 'ad', 'amp', 'mc2')
DATATYPE = ""  # Data type name from schema

# The Synapse ID of the schema registry table
SCHEMA_REGISTRY_TABLE_ID = ""


def query_schema_registry(
    dcc: str,
    datatype: str,
    synapse_client: Optional[Synapse] = None
) -> List[dict]:
    """
    Query the schema registry table to find schemas matching DCC and datatype.
    Arguments:
        dcc: Data Coordination Center identifier (e.g., 'ad', 'amp', 'mc2')
        datatype: Data type name from the schema
        synapse_client: Authenticated Synapse client instance
    Returns:
        List of dictionaries containing schema information, sorted by version
    """
    if synapse_client is None:
        syn = Synapse()
        syn.login()
    else:
        syn = synapse_client

    # Construct SQL query to search for schemas matching DCC and datatype
    # The query looks for exact matches in DCC and contains match for datatype
    # Results are sorted by version in descending order (newest first)
    query = f"""
    SELECT * FROM {SCHEMA_REGISTRY_TABLE_ID}
    WHERE dcc = '{dcc}'
    AND datatype LIKE '%{datatype}%'
    ORDER BY version DESC
    """

    print(f"Querying schema registry with DCC='{dcc}' and datatype='{datatype}'...")
    print(f"SQL Query: {query}")

    # Query the table and get results as a pandas DataFrame
    table = Table(id=SCHEMA_REGISTRY_TABLE_ID)
    results_df = table.query(query=query)

    if results_df.empty:
        print(f"No schemas found for DCC='{dcc}' and datatype='{datatype}'")
        return []

    # Convert DataFrame to list of dictionaries for easier handling
    results = results_df.to_dict('records')

    print(f"Found {len(results)} matching schema(s):")
    for i, result in enumerate(results, 1):
        print(f"  {i}. URI: {result.get('uri', 'N/A')}")
        print(f"     Version: {result.get('version', 'N/A')}")
        print(f"     DCC: {result.get('dcc', 'N/A')}")
        print(f"     DataType: {result.get('datatype', 'N/A')}")
        if i < len(results):
            print()

    return results


def get_latest_schema_uri(dcc: str, datatype: str, synapse_client: Optional[Synapse] = None) -> Optional[str]:
    """
    Get the URI of the latest schema version for the given DCC and datatype.
    Arguments:
        dcc: Data Coordination Center identifier
        datatype: Data type name from the schema
        synapse_client: Authenticated Synapse client instance
    Returns:
        URI string of the latest schema version, or None if not found
    """
    results = query_schema_registry(dcc, datatype, synapse_client)

    if results:
        latest_schema = results[0]  # Results are sorted by version DESC, so first is latest
        uri = latest_schema.get('uri')
        print(f"\nLatest schema URI: {uri}")
        return uri
    else:
        print(f"\nNo schema found for DCC='{dcc}' and datatype='{datatype}'")
        return None


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Query the Synapse schema registry to find Schema URIs by DCC and datatype"
    )
    parser.add_argument(
        '--dcc',
        type=str,
        help='Data Coordination Center identifier (e.g., ad, amp, mc2)'
    )
    parser.add_argument(
        '--datatype',
        type=str,
        help='Data type name from the schema (e.g., IndividualAnimalMetadataTemplate)'
    )

    args = parser.parse_args()

    # Use command line arguments if provided, otherwise use global variables
    if args.dcc is not None:
        dcc = args.dcc
    elif DCC:
        dcc = DCC
    else:
        raise ValueError("DCC must be provided via CLI argument --dcc or set in global variable DCC")

    if args.datatype is not None:
        datatype = args.datatype
    elif DATATYPE:
        datatype = DATATYPE
    else:
        raise ValueError("datatype must be provided via CLI argument --datatype or set in global variable DATATYPE")

    # Initialize Synapse client
    syn = Synapse()
    syn.login()

    # Get just the latest schema URI
    latest_uri = get_latest_schema_uri(dcc, datatype, syn)
    if latest_uri:
        print(f"\nUse this URI in your scripts: {latest_uri}")


if __name__ == "__main__":
    main()