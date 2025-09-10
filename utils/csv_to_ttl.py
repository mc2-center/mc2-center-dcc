"""
csv_to_ttl.py

Converts a schematic data model CSV or CRDC data model TSV to RDF triples
Serialized triples to a ttl file
ttl file can be used as a graph input for the arachne agent.

usage: csv_to_ttl.py [-h] [-m MODEL] [-p MAPPING] [-o OUTPUT] [-g ORG_NAME] [-b BASE_TAG]

options:
  -h, --help						show this help message and exit
  -m MODEL, --model MODEL			Path to schematic model CSV or CRDC data model TSV
  -p MAPPING, --mapping MAPPING		Path to ttl source content file
  -o OUTPUT, --output OUTPUT		Path to folder where graph should be stored (Default: current directory)
  -g ORG_NAME, --org_name ORG_NAME	Abbreviation used to label the data model and determine how model should be processed (Default: 'new_org', schematic processing)
  -b BASE_TAG, --base_tag BASE_TAG	url applied to the beginning of internal tags (Default: 'http://syn.org')

author: orion.banks
"""

import argparse
import os
import pandas as pd
from pathlib import Path
import re

def get_args():
	"""Set up command-line interface and get arguments."""
	parser = argparse.ArgumentParser()
	parser.add_argument(
        "-m",
		"--model",
        type=str,
        help="Path to schematic model CSV or CRDC data model TSV",
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
        help="Path to folder where graph should be stored (Default: current directory)",
        required=False,
		default=os.getcwd()
    )
	parser.add_argument(
        "-g",
		"--org_name",
        type=str,
        help="Abbreviation used to label the data model and determine how model should be processed (Default: 'new_org', schematic processing)",
        required=False,
		default="new_org"
    )
	parser.add_argument(
        "-b",
		"--base_tag",
        type=str,
        help="url applied to the beginning of internal tags (Default: 'http://syn.org')",
        required=False,
		default="http://syn.org"
    )
	return parser.parse_args()


def convert_schematic_model_to_ttl_format(input_df: pd.DataFrame, org_name: str, base_tag: str) -> pd.DataFrame:
	"""Convert schematic model DataFrame to TTL format."""
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
		lambda x: f"<{base_tag}/{org_name}/{x.strip().lower().replace(' ', '_')}>"
		)
	
	# Step 3: Construct term URIs for each attribute
	out_df["term"] = out_df.apply(lambda row: format_uri(base_tag, row["Resolved_Node"], row["label"], org_name), axis=1)
	
	# Step 4: Info extraction and TTL-compatible column formatting
	for _, row in out_df.iterrows():
		out_df.at[_, "description"] = attribute_rows.loc[row["label"], "Description"]
		out_df.at[_, "is_cde"] = get_cde_id(str(attribute_rows.loc[row["label"], "Properties"]))
		out_df.at[_, "node"] = row["Resolved_Node_URI"]
		out_df.at[_, "is_key"] = "true" if str(attribute_rows.loc[row["label"], "Validation Rules"]).strip().lower() == "unique" else ""
		out_df.at[_, "required_by"] = row["Resolved_Node_URI"] if str(attribute_rows.loc[row["label"], "Required"]).strip().lower() == "true" else ""
		out_df.at[_, "has_enum"] = str(attribute_rows.loc[row["label"], "Required"]) if str(attribute_rows.loc[row["label"], "Valid Values"]) != "nan" else ""
		col_type = attribute_rows.loc[row["label"], "columnType"]
		is_enum = True if str(attribute_rows.loc[row["label"], "Valid Values"]) != "nan" else False
		out_df.at[_, "type"] = '"' + str(convert_schematic_column_type(col_type, is_enum)) + '"'
	
	out_df["label"] = '"' + out_df["label"].fillna('') + '"'
	out_df["description"] = '"' + out_df["description"].fillna('').replace('"', '') + '"'
	out_df["is_cde"] = out_df["is_cde"].fillna("")

	node_name = "all" if len(out_df["node"].unique()) > 1 else str(out_df["node"].unique()).split("/")[-1].split(">")[0]
	
	# Final output
	final_cols = ["term", "label", "description", "node", "type", "required_by", "is_cde", "is_key", "has_enum"]
	return out_df[final_cols], node_name


def convert_crdc_model_to_ttl_format(input_df: pd.DataFrame, org_name: str, base_tag: str) -> pd.DataFrame:
	"""Convert CRDC model DataFrame to TTL format."""
	out_df = pd.DataFrame()
	
	out_df["term"] = input_df.apply(
		lambda row: format_uri(base_tag, row["Node"], row["Property"], org_name), axis=1)
	out_df["label"] = '"' + input_df["Property"].fillna('') + '"'
	out_df["description"] = input_df["Description"].fillna("").replace('"', '')
	out_df["cde_name"] = input_df["CDEFullName"].fillna("")
	out_df["node"] = input_df["Node"].apply(
		lambda x: f"<{base_tag}/{org_name}/{x.strip().lower().replace(' ', '_')}>")
	out_df["is_cde"] = input_df["CDECode"].fillna("").apply(lambda x: str(x).split(".")[0])
	out_df["is_key"] = input_df["Key Property"].apply(lambda x: str(x)).replace(["False", "True"], ["", "true"])
	out_df["required_by"] = input_df["Required"].apply(lambda x: str(x))
	out_df["type"] = input_df["Type"].apply(lambda x: str(x))
	out_df["has_enum"] = input_df["Acceptable Values"].fillna("").apply(lambda x: x.split(","))
	out_df["cde_name"] = input_df["CDEFullName"].apply(lambda x: str(x))

	for _, row in out_df.iterrows():
		col_type = row["type"]
		is_enum = True if len(row["has_enum"]) > 1 else False
		out_df.at[_, "type"] = '"' + str(convert_gc_column_type(col_type, is_enum)) + '"'
		out_df.at[_, "required_by"] = row["node"] if row["required_by"] == "required" else ""
		out_df.at[_, "has_enum"] = ", ".join(row["has_enum"])
		out_df.at[_, "description"] = '"' + (f'{row["cde_name"]}: ' if str(row["cde_name"]) != "nan" else "") + row["description"] + '"'
	
	node_name = "all" if len(out_df["node"].unique()) > 1 else str(out_df["node"].unique()).split("/")[-1].split(">")[0]
	
	final_cols = ["term", "label", "description", "node", "type", "required_by", "is_cde", "is_key", "has_enum"]
	return out_df[final_cols], node_name


def format_uri(base_tag:str, node:str, attribute:str, org_name:str) -> str:
	"""Format the URI for a given node and attribute."""

	node_segment = node.strip().lower().replace(" ", "_")
	attr_segment = attribute.strip().lower().replace(" ", "_")
    
	return f"<{base_tag}/{org_name}/{node_segment}/{attr_segment}>"


def convert_schematic_column_type(type:str, is_enum:bool) -> str: 
	"""Convert schematic column type to TTL-compatible format."""

	if type in ["string", "string_list"]:
		string_type = "string;enum" if is_enum else "string"
		if type == "string_list":
			out_type = f"array[{string_type}]"
		else:
			out_type = string_type
	else:
		out_type = type
	
	return out_type


def get_cde_id(entry: str) -> str:
	"""Extract CDE ID from Properties entry."""
	entry = entry.split(", ") if len(entry.split(", ")) > 1 else entry
	
	if type(entry) == list:
		for ref in entry:
			if ref.split(":")[0] == "CDE":
				return ref.split(":")[1]
	else:
		return entry.split(":")[1] if entry.split(":")[0] == "CDE" else ""


def convert_gc_column_type(type:str, is_enum:bool) -> str: 
	"""Convert GC column type to TTL-compatible format."""

	if type in ["string", "list"]:
		string_type = "string;enum" if is_enum else "string"
		if type == "list":
			out_type = f"array[{string_type}]"
		else:
			out_type = string_type
	elif re.match(r'{"pattern"', type) is not None:
		out_type = "string"
	elif re.match(r'{"value_type":"number"', type) is not None:
		out_type = "number"
	else:
		out_type = type
	
	return out_type


def main():
	
	args = get_args()

	base_tag = args.base_tag
	label_tag = "<http://www.w3.org/2000/01/rdf-schema#label>"
	desc_tag = "<http://purl.org/dc/terms/description>"
	node_tag = f"<{base_tag}/node>"
	type_tag = f"<{base_tag}/type>"
	req_tag = f"<{base_tag}/requiredBy>"
	cde_tag = f"<{base_tag}/isCDE>"
	key_tag = f"<{base_tag}/isKey>"
	enum_tag = f"<{base_tag}/acceptableValues>"

	if args.mapping:
		print(f"Processing RDF triples precursor CSV [{args.mapping}]...")
		ttl_df = pd.read_csv(args.mapping, header=0, keep_default_na=False)
		node_name = "mapped"
		print(f"RDF triples will be built from pre-cursor file!")
	
	elif args.model:
		print(f"Processing model [{args.model}] to RDF triples precursor CSV...")
		sep = "," if Path(args.model).suffix == ".csv" else "\t" 
		model_df = pd.read_csv(args.model, header=0, keep_default_na=True, sep=sep)
		if str(args.org_name).lower() in ["new_org", "mc2", "nf", "adkp", "htan"]:
			ttl_df, node_name = convert_schematic_model_to_ttl_format(model_df, args.org_name, base_tag)
		if str(args.org_name).lower() in ["gc", "crdc", "dh"]:
			ttl_df, node_name = convert_crdc_model_to_ttl_format(model_df, args.org_name, base_tag)
		print(f"RDF triples will be built from the generated precursor dataframe!")

	out_file = "/".join([args.output, f"{args.org_name}_{node_name}.ttl"])

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
			key_tag: row["is_key"],
			enum_tag: row["has_enum"]
			}
			
			f.write(f"{ttl_dict['term']} {label_tag} {ttl_dict[label_tag]};"+"\n")
			f.write("\t"+f"{desc_tag} {ttl_dict[desc_tag]};"+"\n")
			f.write("\t"+f"{node_tag} {ttl_dict[node_tag]};"+"\n")
			line_end = ";" if ttl_dict[req_tag] or ttl_dict[key_tag] or ttl_dict[cde_tag] or ttl_dict[enum_tag] else " ."
			f.write("\t"+f"{type_tag} {ttl_dict[type_tag]}{line_end}"+"\n")
			if ttl_dict[req_tag]:
				line_end = ";\n" if ttl_dict[key_tag] or ttl_dict[cde_tag] or ttl_dict[enum_tag] else " .\n"
				f.write("\t"+f"{req_tag} {''.join([ttl_dict[req_tag], line_end])}")
			if ttl_dict[key_tag]:
				line_end = ";\n" if ttl_dict[cde_tag] or ttl_dict[enum_tag] else " .\n"
				f.write("\t"+f"{key_tag} {''.join([ttl_dict[key_tag], line_end])}")
			if ttl_dict[cde_tag]:
				line_end = ";\n" if ttl_dict[enum_tag] else " .\n"
				f.write("\t"+f"{cde_tag} {''.join([ttl_dict[cde_tag], line_end])}")
			if ttl_dict[enum_tag]:
				line_end = " .\n"
				f.write("\t"+f"{enum_tag} {''.join([ttl_dict[enum_tag], line_end])}")
			f.write("\n")
	
	print(f"Done ✅")
	print(f"{out_file} was written with {len(ttl_df)} triples!")

if __name__ == "__main__":
    main()
