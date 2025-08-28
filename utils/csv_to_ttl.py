"""
csv_to_ttl.py

Converts a CSV with formatted ttl info to a ttl file
ttl file can be used as a graph input for the arachne agent.

author: orion.banks
"""

import argparse
import os
import pandas as pd

def get_args():
	"""Set up command-line interface and get arguments."""
	parser = argparse.ArgumentParser()
	parser.add_argument(
        "-m",
		"--mapping",
        type=str,
        help="Path to ttl source content file",
        required=True
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
        help="Abbreviation used to label the data model",
        required=False,
		default="new_org"
    )
	return parser.parse_args()

def main():
	
	args = get_args()

	base_tag = "<http://syn.org>"
	label_tag = "<http://www.w3.org/2000/01/rdf-schema#label>"
	desc_tag = "<http://purl.org/dc/terms/description>"
	node_tag = "<http://syn.org/node>"
	type_tag = "<http://syn.org/type>"
	req_tag = "<http://syn.org/requiredBy>"
	cde_tag = "<http://syn.org/isCDE>"
	key_tag = "<http://syn.org/isKey>"

	ttl_df = pd.read_csv(args.mapping, header=0, keep_default_na=False)

	out_file = "/".join([args.output, f"{args.org_name}.ttl"])

	with open(out_file, "w+") as f:

		for _, row in ttl_df.iterrows():
			ttl_dict = {
			"term": row["term"],
			label_tag: row["label"],
			desc_tag: row["description"],
			node_tag: row["node"],
			type_tag: row["type"],
			req_tag: row["required_by"],
			cde_tag: row["is_cde"],
			key_tag: row["is_key"]
			}
			
			f.write(f"{ttl_dict['term']} {label_tag} {ttl_dict[label_tag]};"+"\n")
			f.write("\t"+f"{desc_tag} {ttl_dict[desc_tag]};"+"\n")
			f.write("\t"+f"{node_tag} {ttl_dict[node_tag]};"+"\n")
			line_end = ";" if ttl_dict[req_tag] or ttl_dict[key_tag] or ttl_dict[cde_tag] else " ."
			f.write("\t"+f"{type_tag} {ttl_dict[type_tag]}{line_end}"+"\n")
			if ttl_dict[req_tag]:
				line_end = ";\n" if ttl_dict[key_tag] or ttl_dict[cde_tag] else " .\n"
				f.write("\t"+f"{req_tag} {''.join([ttl_dict[req_tag], line_end])}")
			if ttl_dict[key_tag]:
				line_end = ";\n" if ttl_dict[cde_tag] else " .\n"
				f.write("\t"+f"{key_tag} {''.join([ttl_dict[key_tag], line_end])}")
			if ttl_dict[cde_tag]:
				f.write("\t"+f"{cde_tag} {' '.join([ttl_dict[cde_tag], '.'])}"+"\n")
			f.write("\n")
	
if __name__ == "__main__":
    main()
