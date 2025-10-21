from synapseclient import Synapse
from synapseclient.models import Folder

# Data from: https://synapse.org/Synapse:syn69735275/tables/
# The URI of the JSON Schema you want to bind, for example: `sage.schemas.v2571-ad.IndividualAnimalMetadataTemplate.schema-0.1.0`
URI = ""
# The Synapse ID of the entity you want to bind the JSON Schema to. This should be the ID of a Folder where you want to enforce the schema.
FOLDER_ID = ""

syn = Synapse()
syn.login()

folder = Folder(id=FOLDER_ID).get()
schema_validation = folder.validate_schema()

print(f"Schema validation result for folder {FOLDER_ID}: {schema_validation}")