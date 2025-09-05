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
		"--model",
        type=str,
        help="Path to schematic data model CSV",
        required=False
    )
	parser.add_argument(
        "-p",
		"--mapping",
        type=str,
        help="Path to ttl source content file",
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
        help="Abbreviation used to label the data model",
        required=False,
		default="new_org"
    )
	return parser.parse_args()


def convert_schematic_model_to_ttl_format(input_df: pd.DataFrame, org_name) -> pd.DataFrame:

	out_df = pd.DataFrame()
	
	# Step 1: Identify all node rows (treat rows with non-empty DependsOn as nodes)
	node_rows = input_df[input_df["DependsOn"].notna()]
	attribute_rows = input_df[input_df["DependsOn"].isna()].set_index("Attribute")
	attribute_to_node = {row["Attribute"]: str(row["DependsOn"]).split(", ") for _, row in node_rows.iterrows()}
	
	
	attribute_info = [(attribute, node) for node, attribute_list in attribute_to_node.items() for attribute in attribute_list]
	out_df["label"] = [entry[0] for entry in attribute_info]
	out_df["Resolved_Node"] = [entry[1] for entry in attribute_info]

	# Step 2: Assign node URI for each attribute
	out_df["Resolved_Node_URI"] = out_df["Resolved_Node"].apply(
		lambda x: f"<http://syn.org/mc2/{x.strip().lower().replace(' ', '_')}>"
		)
	
	# Step 3: Construct term URIs for each attribute
	out_df["term"] = out_df.apply(lambda row: format_uri(row["Resolved_Node"], row["label"], org_name), axis=1)
	
	# Step 4: TTL-compatible column formatting
	for _, row in out_df.iterrows():
		out_df.at[_, "description"] = attribute_rows.loc[row["label"], "Description"]
		out_df.at[_, "is_cde"] = get_cde_id(str(attribute_rows.loc[row["label"], "Properties"]))
		out_df.at[_, "node"] = row["Resolved_Node_URI"]
		out_df.at[_, "is_key"] = "true" if str(attribute_rows.loc[row["label"], "Validation Rules"]).strip().lower() == "unique" else ""
		out_df.at[_, "required_by"] = row["Resolved_Node_URI"] if str(attribute_rows.loc[row["label"], "Required"]).strip().lower() == "true" else ""	
		col_type = attribute_rows.loc[row["label"], "columnType"]
		is_enum = True if str(attribute_rows.loc[row["label"], "Valid Values"]) != "nan" else False
		out_df.at[_, "type"] = '"' + str(convert_schematic_column_type(col_type, is_enum)) + '"'
	
	out_df["label"] = '"' + out_df["label"].fillna('') + '"'
	out_df["description"] = '"' + out_df["description"].fillna('').replace('"', '') + '"'
	out_df["is_cde"] = out_df["is_cde"].fillna("")
	
	# Final output
	final_cols = ["term", "label", "description", "node", "type", "required_by", "is_cde", "is_key"]
	return out_df[final_cols]


def format_uri(node, attribute, org_name):
	
	node_segment = node.strip().lower().replace(" ", "_")
	attr_segment = attribute.strip().lower().replace(" ", "_")
    
	return f"<http://syn.org/{org_name}/{node_segment}/{attr_segment}>"


def convert_schematic_column_type(type, is_enum):
	
	if type in ["string", "string_list"]:
		string_type = "string;enum" if is_enum else "string"
		if type == "string_list":
			out_type = f"array[{string_type}]"
		else:
			out_type = string_type
	else:
		out_type = type
	
	return out_type


def get_cde_id(entry):

	entry = entry.split(", ") if len(entry.split(", ")) > 1 else entry
	
	if type(entry) == list:
		for ref in entry:
			if ref.split(":")[0] == "CDE":
				return ref.split(":")[1]
	else:
		return entry.split(":")[1] if entry.split(":")[0] == "CDE" else ""

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

	if args.mapping:
		print(f"Processing RDF triples precursor CSV [{args.mapping}]...")
		ttl_df = pd.read_csv(args.mapping, header=0, keep_default_na=False)
		print(f"RDF triples will be built from pre-cursor file!")
	
	elif args.model:
		print(f"Processing model [{args.model}] to RDF triples precursor CSV...")
		model_df = pd.read_csv(args.model, header=0, keep_default_na=True)
		ttl_df = convert_schematic_model_to_ttl_format(model_df, args.org_name)
		print(f"RDF triples will be built from the generated precursor dataframe!")

	out_file = "/".join([args.output, f"{args.org_name}.ttl"])

	with open(out_file, "w+") as f:
		print(f"Building RDF triples and serializing to TTL...")
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
	
	print(f"Done ✅")
	print(f"{out_file} was written with {len(ttl_df)} triples!")

if __name__ == "__main__":
    main()
