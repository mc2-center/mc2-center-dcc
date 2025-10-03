"""
build_template_ttl.py

Converts a metadata template CSV info to a ttl file defining the template.
ttl file can be used as input for the arachne agent and would be available as a target.

usage: build_template_ttl.py [-h] [-t TEMPLATE] [-o OUTPUT] [-g ORG_NAME] [-p TAG_PREFIX] [-v VERSION]

options:
  -h, --help            show this help message and exit
  -t TEMPLATE, --template TEMPLATE
                        Path to metadata template CSV
  -o OUTPUT, --output OUTPUT
                        Path to folder where graph should be stored
  -g ORG_NAME, --org_name ORG_NAME
                        Abbreviation for org, used in RDF prefixes
  -p TAG_PREFIX, --tag_prefix TAG_PREFIX
                        The tag that will be used as a prefix in RDF
  -v VERSION, --version VERSION
                        Version applied to output ttl filename (Default: 1.0.0)

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
        "-b",
		"--base_tag",
        type=str,
        help="The tag that will be used as a prefix in RDF",
        required=False,
		default="http://syn.org"
    )
	parser.add_argument(
        "-r",
		"--base_ref",
        type=str,
        help="The prefix that will be used to represent the base_tag",
        required=False,
		default="syn"
    )
	parser.add_argument(
        "-v",
		"--version",
        type=str,
        help="Version applied to output ttl filename (Default: 1.0.0)",
        required=False,
		default="1.0.0"
    )
	return parser.parse_args()


def format_uri(column_name):
	
	return column_name.strip().lower().replace(" ", "_")


def main():
	
	args = get_args()

	base_tag = args.base_tag
	base_ref = args.base_ref

	org_tag = args.org_name
	conform_tag = "conformsTo"
	int_tag = "integer"
	version = args.version
	template_tag = "Template"
	col_tag = "hasColumn"
	pos_tag = "ColumnPosition"
	colname_tag = "column"
	header_tag = "header"
	value_tag = "position"
	def_val_tag = "defaultValue"
	uuid_tag = "uuid"

	tag_dict = {  # Can replace tuples with alternative tag definitions
		base_ref : (base_ref, f"<{base_tag}/>"),
		org_tag : (org_tag, f"<{base_ref}:{org_tag}/>"),
		conform_tag : ("purl", "<http://purl.org/dc/terms/>"),
		int_tag : ("xml", "<http://www.w3.org/2001/XMLSchema#>"),
		template_tag : (base_ref, f"<{base_tag}/>"),
		col_tag : (base_ref, f"<{base_tag}/>"),
		uuid_tag : (base_ref, f"<{base_tag}/>"),
		pos_tag : (base_ref, f"<{base_tag}/>"),
		colname_tag : (base_ref, f"<{base_tag}/>"),
		header_tag : (base_ref, f"<{base_tag}/>"),
		value_tag : (base_ref, f"<{base_tag}/>"),
		def_val_tag: (base_ref, f"<{base_tag}/>")
		}
	
	if args.template:
		print(f"Processing model [{args.template}] to template.ttl...")
		template_path = Path(args.template)
		template_name = template_path.stem
		sep = "," if template_path.suffix == ".csv" else "\t" 
		template_df = pd.read_csv(args.template, header=0, keep_default_na=True, sep=sep)

	if template_name.startswith("GC_Data_Loading_Template"):
		template_name_split = template_name.split("_")
		template_name = template_name_split[-2]
		version = template_name_split[-1]

	out_file = "/".join([args.output, f"{args.org_name}_{template_name}_{version}.ttl"])

	with open(out_file, "w+") as f:
		print(f"Building RDF triples and serializing to TTL...")
		prefix_set = [prefix for prefix in tag_dict.keys()]
		first_lines = [f"@prefix {tag_dict[prefix][0]}: {tag_dict[prefix][1]}"+" .\n" for prefix in prefix_set]
		template_name_tag = f"@prefix {template_name}: <{tag_dict[template_tag][0]}:{template_name}/>"+" .\n\n"
		first_lines_set = "".join(sorted(set(first_lines)))
		f.write(first_lines_set)
		f.write(template_name_tag)
		
		# write template definition
		f.write(f"{tag_dict[org_tag][0]}:{template_name} a {tag_dict[base_ref][0]}:{template_tag} ."+"\n")
		f.write(f"{tag_dict[template_tag][0]}:{template_name} {tag_dict[conform_tag][0]}:{conform_tag} {tag_dict[base_ref][0]}:{org_tag} ."+"\n")
		
		# write column definitions
		# set col position counter
		col_position = 0
		for col in template_df.columns:
			clean_col = format_uri(col)
			col_uuid = uuid4()
			if col in ["Component", "type"]:
				f.write(f'{template_name}:{clean_col} {tag_dict[def_val_tag][0]}:{def_val_tag} "{template_name}" .'+"\n")
			f.write(f"{tag_dict[template_tag][0]}:{template_name} {tag_dict[col_tag][0]}:{col_tag} {template_name}:{clean_col} ."+"\n")
			f.write(f"{tag_dict[uuid_tag][0]}:{col_uuid} a {tag_dict[pos_tag][0]}:{pos_tag} ;"+"\n")
			f.write(f"{tag_dict[template_tag][0]}:{template_tag} {tag_dict[org_tag][0]}:{template_name} ;"+"\n")
			f.write("\t"+f"{tag_dict[colname_tag][0]}:{colname_tag} {template_name}:{clean_col} ;"+"\n")
			f.write("\t"+f'{tag_dict[header_tag][0]}:{header_tag} "{clean_col}" ;'+"\n")
			f.write("\t"+f'{tag_dict[value_tag][0]}:{value_tag} "{col_position}"^^{tag_dict[int_tag][0]}:{int_tag} .'+"\n")
			col_position += 1
	
	print(f"Done ✅")
	print(f"{out_file} was written with {col_position} attributes!")

if __name__ == "__main__":
    main()
