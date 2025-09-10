"""
build_template_ttl.py

Converts a metadata template CSV info to a ttl file defining the template.
ttl file can be used as input for the arachne agent and would be available as a target.

author: orion.banks
"""

import argparse
import os
import pandas as pd
from pathlib import Path
from uuid import uuid4

def get_args():
	"""Set up command-line interface and get arguments."""
	parser = argparse.ArgumentParser()
	parser.add_argument(
        "-t",
		"--template",
        type=str,
        help="Path to metadata template CSV",
        required=False
    )
	parser.add_argument(
        "-o",
		"--output",
        type=str,
        help="Path to folder where graph should be stored",
        required=False,
		default=os.getcwd()
    )
	parser.add_argument(
        "-g",
		"--org_name",
        type=str,
        help="Abbreviation for org, used in RDF prefixes",
        required=False,
		default="new_org"
    )
	parser.add_argument(
        "-p",
		"--tag_prefix",
        type=str,
        help="The tag that will be used as a prefix in RDF",
        required=False,
		default="http://syn.org"
    )
	parser.add_argument(
        "-v",
		"--version",
        type=str,
        help="Version applied to output ttl filename (Default: None)",
        required=False,
		default=None
    )
	return parser.parse_args()


def format_uri(column_name):
	
	return column_name.strip().lower().replace(" ", "_")


def main():
	
	args = get_args()

	prefix_tag = args.tag_prefix
	conform_tag = "<http://purl.org/dc/terms/conformsTo>"
	int_tag = "<http://www.w3.org/2001/XMLSchema#integer>"
	
	if args.template:
		print(f"Processing model [{args.template}] to template.ttl...")
		template_path = Path(args.template)
		template_name = template_path.stem
		sep = "," if template_path.suffix == ".csv" else "\t" 
		template_df = pd.read_csv(args.template, header=0, keep_default_na=True, sep=sep)

	out_file = "/".join([args.output, f"{args.org_name}_{template_name}_{args.version}.ttl"])

	with open(out_file, "w+") as f:
		print(f"Building RDF triples and serializing to TTL...")

		# write template definition
		f.write(f"<{prefix_tag}/{args.org_name}/{template_name}> a <{prefix_tag}/Template> ."+"\n")
		f.write(f"<{prefix_tag}/{args.org_name}/{template_name}> {conform_tag} <{prefix_tag}/{args.org_name}> ."+"\n")
		
		# write column definitions
		# set col position counter
		col_position = 0
		for col in template_df.columns:
			clean_col = format_uri(col)
			col_uuid = uuid4()
			if col in ["Component", "type"]:
				f.write(f'<{prefix_tag}/{args.org_name}/{template_name}/{clean_col}> <{prefix_tag}/defaultValue> "{template_name}"'+"\n")
			f.write(f"<{prefix_tag}/{args.org_name}/{template_name}> <{prefix_tag}/hasColumn> <{prefix_tag}/{args.org_name}/{template_name}/{clean_col}> ."+"\n")
			f.write(f"<{prefix_tag}/{col_uuid}>a <{prefix_tag}/ColumnPosition> ;"+"\n")
			f.write(f"<{prefix_tag}/template> <{prefix_tag}/{args.org_name}/{template_name}> ;"+"\n")
			f.write("\t"+f"<{prefix_tag}/column> <{prefix_tag}/{args.org_name}/{template_name}/{clean_col}> ;"+"\n")
			f.write("\t"+f'<{prefix_tag}/header> "{col}" ;'+"\n")
			f.write("\t"+f'<{prefix_tag}/position> "{col_position}"^^{int_tag} .'+"\n")
			col_position += 1
	
	print(f"Done ✅")
	print(f"{out_file} was written with {col_position} attributes!")

if __name__ == "__main__":
    main()
