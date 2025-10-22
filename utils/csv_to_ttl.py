"""
csv_to_ttl.py

Converts a schematic data model CSV or CRDC data model TSV to RDF triples
Serializes triples to a ttl file - ttl file can be used as a graph input for the arachne agent.
For schematic-based models, conditional dependencies are extracted and added to the data model graph.
Optionally generates a data model diagram and an interactive model viewer (WIP)

usage: csv_to_ttl.py [-h] [-m MODEL] [-p MAPPING] [-o OUTPUT] [-g ORG_NAME] [-r {schematic,crdc}] [-b BASE_TAG] [-f BASE_REF] [-v VERSION] [-s SUBSET] [-bg] [-ig]

options:
  -h, --help            show this help message and exit
  -m MODEL, --model MODEL
                        Path to schematic model CSV or CRDC data model TSV
  -p MAPPING, --mapping MAPPING
                        Path to ttl source content file
  -o OUTPUT, --output OUTPUT
                        Path to folder where graph should be stored (Default: current directory)
  -g ORG_NAME, --org_name ORG_NAME
                        Abbreviation used in the data model name and RDF tags. (Default: 'new_org')
  -r {schematic,crdc}, --reference_type {schematic,crdc}
                        The type of data model reference used as a basis for the input. One of 'schematic' or 'crdc'. If no input is given, the reference type will be
                        automatically determined based on the provided org name (Default: None)
  -b BASE_TAG, --base_tag BASE_TAG
                        url applied to the beginning of internal tags (Default: 'http://syn.org')
  -f BASE_REF, --base_ref BASE_REF
                        Reference tag used to represent base_tag in ttl header (Default: 'syn')
  -v VERSION, --version VERSION
                        Version applied to output ttl filename (Default: None)
  -s SUBSET, --subset SUBSET
                        The name of one or more data types to extract from the model. Provide multiple as a quoted comma-separated list, e.g., 'Study, Biospecimen' (Default:
                        None)
  -bg, --build_graph    Boolean. Pass this flag to generate a PNG of the input model (Default: None)
  -ig, --interactive_graph
                        Boolean. Pass this flag to generate an interactive visualization of the input model (Default: None)

author: orion.banks
"""

import argparse
import io
from IPython.display import display, Image
import matplotlib.pyplot as plt
import networkx as nx
import os
import pandas as pd
from pathlib import Path
from PIL import Image
import pydotplus
import rdflib
from rdflib.extras.external_graph_libs import rdflib_to_networkx_multidigraph
from rdflib.tools import rdf2dot
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
        help="Abbreviation used in the data model name and RDF tags. (Default: 'new_org')",
        required=False,
		default="new_org"
    )
	parser.add_argument(
        "-r",
		"--reference_type",
        type=str,
		choices=["schematic", "crdc"],
        help="The type of data model reference used as a basis for the input. One of 'schematic' or 'crdc'. If no input is given, the reference type will be automatically determined based on the provided org name (Default: None)",
        required=False,
		default=None
    )
	parser.add_argument(
        "-b",
		"--base_tag",
        type=str,
        help="url applied to the beginning of internal tags (Default: 'http://syn.org')",
        required=False,
		default="http://syn.org"
    )
	parser.add_argument(
        "-f",
		"--base_ref",
        type=str,
        help="Reference tag used to represent base_tag in ttl header (Default: 'syn')",
        required=False,
		default="syn"
    )
	parser.add_argument(
        "-v",
		"--version",
        type=str,
        help="Version applied to output ttl filename (Default: None)",
        required=False,
		default=None
    )
	parser.add_argument(
        "-s",
		"--subset",
        type=str,
        help="The name of one or more data types to extract from the model. Provide multiple as a quoted comma-separated list, e.g., 'Study, Biospecimen' (Default: None)",
        required=False,
		default=None
    )
	parser.add_argument(
        "-bg",
		"--build_graph",
        help="Boolean. Pass this flag to generate a PNG of the input model (Default: None)",
		action="store_true",
        required=False,
		default=None
    )
	parser.add_argument(
        "-ig",
		"--interactive_graph",
        help="Boolean. Pass this flag to generate an interactive visualization of the input model (Default: None)",
		action="store_true",
        required=False,
		default=None
    )
	return parser.parse_args()


def convert_schematic_model_to_ttl_format(input_df: pd.DataFrame, org_name: str, subset: None|str) -> tuple[pd.DataFrame, list[str]]:
	"""Convert schematic model DataFrame to TTL format."""
	out_df = pd.DataFrame()
	
	# Step 1: Identify all node rows (treat rows with non-empty DependsOn as nodes)
	if subset is None:
		node_rows = input_df[input_df["DependsOn"].str.contains("Component")]
		node_list = node_rows["Attribute"].to_list()
		input_df = subset_model(input_df, node_list)
	
	node_rows = input_df[input_df["DependsOn"].str.contains("Component")]
	node_list = node_rows["Attribute"].to_list()
	
	attribute_rows = input_df[~input_df["DependsOn"].str.contains("Component")].set_index("Attribute")
	attribute_to_node = {row["Attribute"]: str(row["DependsOn"]).split(", ") for _, row in node_rows.iterrows()}
	attribute_info = set([(attribute, node) for node, attribute_list in attribute_to_node.items() for attribute in attribute_list])
	
	out_df["label"] = [entry[0] for entry in attribute_info]
	out_df["Resolved_Node"] = [entry[1] for entry in attribute_info]

	# Step 2: Assign node URI for each attribute
	out_df["Resolved_Node_URI"] = out_df["Resolved_Node"].apply(
		lambda x: f"{org_name}:{str(x).strip().lower().replace(' ', '_')}"
		)
	
	# Step 3: Construct term URIs for each attribute
	out_df["term"] = out_df.apply(lambda row: format_uri(row["Resolved_Node"], row["label"]), axis=1)
	
	# Step 4: Info extraction and TTL-compatible column formatting
	key_tuples = []
	for _, row in out_df.iterrows():
		out_df.at[_, "description"] = attribute_rows.loc[row["label"], "Description"]
		mappings = get_reference_id(str(attribute_rows.loc[row["label"], "Properties"]))
		out_df.at[_, "maps_to"] = mappings[0]
		out_df.at[_, "node"] = row["Resolved_Node_URI"]
		out_df.at[_, "is_key"] = "true" if "primary_key" in mappings[1] else ""
		out_df.at[_, "required_by"] = row["Resolved_Node_URI"] if str(attribute_rows.loc[row["label"], "Required"]).strip().lower() == "true" else ""
		out_df.at[_, "has_enum"] = '"[' + ", ".join(str(attribute_rows.loc[row["label"], "Valid Values"]).split(", ")) + ']"' if str(attribute_rows.loc[row["label"], "Valid Values"]) != "nan" else ""
		col_type = attribute_rows.loc[row["label"], "columnType"]
		validation = attribute_rows.loc[row["label"], "Validation Rules"]
		is_enum = True if str(attribute_rows.loc[row["label"], "Valid Values"]) != "" else False
		out_df.at[_, "type"] = '"' + convert_schematic_column_type(col_type, validation, is_enum) + '"'
		for e in mappings[1]:
			key_tuples.append((e, row["Resolved_Node_URI"], row["term"]))
	
	out_df["label"] = '"' + out_df["label"].fillna('') + '"'
	out_df["description"] = '"' + out_df["description"].fillna('').apply(lambda x: x.replace('"', '')) + '"'
	out_df["maps_to"] = out_df["maps_to"].fillna("")
	
	# Final output
	final_cols = ["term", "label", "description", "node", "type", "required_by", "maps_to", "is_key", "has_enum"]
	return out_df[final_cols], node_list, [key_tuple for key_tuple in key_tuples if key_tuple is not None]


def convert_crdc_model_to_ttl_format(input_df: pd.DataFrame, org_name: str) -> tuple[pd.DataFrame, list[str]]:
	"""Convert CRDC model DataFrame to TTL format."""
	out_df = pd.DataFrame()
	
	out_df["term"] = input_df.apply(
		lambda row: format_uri(row["Node"], row["Property"]), axis=1)
	out_df["label"] = '"' + input_df["Property"].fillna('') + '"'
	out_df["description"] = input_df["Description"].fillna("")
	out_df["node"] = input_df["Node"].apply(
		lambda x: f"{org_name}:{x.strip().lower().replace(' ', '_')}")
	out_df["maps_to"] = input_df["CDECode"].fillna("").apply(lambda x: ":".join(["CDE", str(x).split(".")[0]]) if x not in ["", "TBD"] else "")
	out_df["is_key"] = input_df["Key Property"].apply(lambda x: str(x)).replace(["FALSE", "True"], ["", "true"])
	out_df["required_by"] = input_df["Required"].apply(lambda x: str(x))
	out_df["type"] = input_df["Type"].apply(lambda x: str(x))
	out_df["has_enum"] = input_df["Acceptable Values"].fillna("").apply(lambda x: x.split(","))
	out_df["cde_name"] = input_df["CDEFullName"].apply(lambda x: str(x))
	node_list = input_df["Node"].to_list()

	for _, row in out_df.iterrows():
		col_type = row["type"]
		is_enum = True if len(row["has_enum"]) > 1 else False
		out_df.at[_, "type"] = '"' + str(convert_gc_column_type(col_type, is_enum)) + '"'
		out_df.at[_, "required_by"] = row["node"] if row["required_by"] == "required" else ""
		out_df.at[_, "has_enum"] = (''.join(['"[', ', '.join(row["has_enum"]).replace('"', '').replace('[', '').replace(']', ''), ']"'])) if is_enum else ""
		out_df.at[_, "description"] = '"' + ''.join([f'{str(row["cde_name"])}: ' if str(row["cde_name"]) != "" else "", row["description"]]).replace('"', '') + '"'

	final_cols = ["term", "label", "description", "node", "type", "required_by", "maps_to", "is_key", "has_enum"]
	return out_df[final_cols], node_list


def format_uri(node:str, attribute:str) -> str:
	"""Format the URI for a given node and attribute."""

	node_segment = str(node).strip().lower().replace(" ", "_").replace('10x_', '')
	attr_segment = attribute.strip().lower().replace(" ", "_").replace('10x_', '')
    
	return f"{node_segment}:{attr_segment}"


def convert_schematic_column_type(type:str, validation: str, is_enum:bool) -> str: 
	"""Convert schematic column type to TTL-compatible format."""

	if type in ["string", "string_list"] or validation in ["str", "list like"]:
		string_type = "string;enum" if is_enum else "string"
		if type == "string_list" or validation == "list like":
			out_type = f"array[{string_type}]"
		else:
			out_type = string_type
	else:
		out_type = type if type else validation
	
	return out_type


def get_reference_id(entry: str) -> tuple[str, list[str]]:
	"""Extract CDE ID from Properties entry."""
	entry = entry.split(", ") if len(entry.split(", ")) > 1 else [entry]

	refs = [e for e in entry if len(e.split(":")) > 1]
	keys = [e for e in entry if len(e.split("_")) > 1]
	
	ref_out = ", ".join([f"{ref.split(':')[0]}:{ref.split(':')[1]}" for ref in refs])
	key_out = keys

	return (ref_out, key_out)

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


def subset_model(model_df: pd.DataFrame, nodes: str) -> pd.DataFrame:

	nodes = nodes.split(", ") if type(nodes)==str else nodes
	
	model_df = model_df.set_index("Attribute")

	out_df_list = []

	for node in nodes:
		subset_df = pd.DataFrame()
		node_attributes = model_df.loc[node, "DependsOn"].split(", ")
		attribute_deps = model_df.loc[model_df["DependsOn"] != ""]
		attribute_deps = attribute_deps.loc[~attribute_deps["DependsOn"].str.contains("Component", regex=False)]
		attribute_attributes = attribute_deps["DependsOn"].tolist()
		node_attributes.append(node)
		
		node_rows = model_df.loc[node_attributes]
		attribute_dep_rows = model_df.loc[attribute_attributes]
		
		subset_df = pd.concat([subset_df, node_rows, attribute_dep_rows])
		subset_df.at[node, "DependsOn"] = ", ".join(subset_df.index.drop(node).tolist())
		out_df_list.append(subset_df)

	node_subset_df = pd.concat(out_df_list).reset_index(names="Attribute").drop_duplicates().fillna("nan")
	
	return node_subset_df


def main():
	
	args = get_args()

	base_tag = args.base_tag
	base_ref = args.base_ref

	label = "label"
	desc = "description"
	node = "node"
	type = "type"
	reqby = "requiredBy"
	key = "isKey"
	enum = "acceptableValues"
	duo = "DUO_"
	cde = "CDE"

	tag_dict = {  # Can replace tuples with alternative tag definitions
		label : ("rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"),
		desc : ("purl", "<http://purl.org/dc/terms/>"),
		node : (base_ref, f"<{base_tag}/>"),
		type : (base_ref, f"<{base_tag}/>"),
		reqby : (base_ref, f"<{base_tag}/>"),
		key : (base_ref, f"<{base_tag}/>"),
		enum : (base_ref, f"<{base_tag}/>"),
		duo : ("obo", "<http://purl.obolibrary.org/obo/>"),
		cde : (base_ref, f"<{base_tag}/>"),
		}
	
	if args.mapping:
		print(f"Processing RDF triples precursor CSV [{args.mapping}]...")
		ttl_df = pd.read_csv(args.mapping, header=0, keep_default_na=False)
		node_name = "mapped"
		print(f"RDF triples will be built from pre-cursor file!")
	
	elif args.model:
		print(f"Processing model [{args.model}] to RDF triples precursor dataframe...")
		sep = "," if Path(args.model).suffix == ".csv" else "\t" 
		model_df = pd.read_csv(args.model, header=0, keep_default_na=False, na_values="nan", sep=sep, dtype=str)
		ref = args.reference_type
		if ref is None:
			if str(args.org_name).lower() in ["new_org", "mc2", "nf", "adkp", "htan", "ada"]:
				ref = "schematic"
			if str(args.org_name).lower() in ["gc", "crdc", "dh"]:
				ref = "crdc"
		if ref == "schematic":
			print(f"Processing model based on schematic CSV specification...")
			if args.subset is not None:
				model_df = subset_model(model_df, f"{args.subset}")
			ttl_df, node_list, key_tuple_list = convert_schematic_model_to_ttl_format(model_df, args.org_name, args.subset)
		if ref == "crdc":
			print(f"Processing model based on CRDC TSV specification...")
			if args.subset is not None:
				model_df = model_df[model_df["Node"].isin(args.subset.split(", "))]
			ttl_df, node_list = convert_crdc_model_to_ttl_format(model_df, args.org_name)
			key_tuple_list = None
		print(f"RDF triples will be built from the generated precursor dataframe!")
	
	node_name = "_".join(args.subset.split(", ")) if args.subset is not None else "all"
	out_file = "/".join([args.output, f"{args.org_name}_{node_name}_{args.version}.ttl"])

	prefix_list = []

	with open(out_file, "w+") as f:
		print(f"Building RDF triples and serializing to TTL...")
		for _, row in ttl_df.iterrows():
			props = None
			ttl_dict = {
			label: row["label"],
			desc: row["description"],
			node: row["node"],
			type: row["type"],
			reqby: row["required_by"],
			key: row["is_key"],
			enum: row["has_enum"]
			}
			
			if row["maps_to"]:
				props = {f"{mapping.split(':')[0].upper()}":f"{mapping.split(':')[1]}" for mapping in row["maps_to"].split(", ")}
				ttl_dict.update(props)

			new_prefixes = [item for item in ttl_dict]
			prefix_list = prefix_list + new_prefixes
				
			f.write("\n")
			f.write(f"{row['term']} {tag_dict[label][0]}:{label} {ttl_dict[label]};"+"\n")
			f.write("\t"+f"{tag_dict[desc][0]}:{desc} {ttl_dict[desc]};"+"\n")
			f.write("\t"+f"{tag_dict[node][0]}:{node} {ttl_dict[node]};"+"\n")
			if ttl_dict[type] != "":
				line_end = ";" if ttl_dict[reqby] or ttl_dict[key] or props or ttl_dict[enum] not in ['"[]"', ""] else " ."
				f.write("\t"+f"{tag_dict[type][0]}:{type} {ttl_dict[type]}{line_end}"+"\n")
			if ttl_dict[reqby]:
				line_end = ";\n" if ttl_dict[key] or props or ttl_dict[enum] not in ['"[]"', ""] else " .\n"
				f.write("\t"+f"{tag_dict[reqby][0]}:{reqby} {''.join([ttl_dict[reqby], line_end])}")
			if ttl_dict[key]:
				line_end = ";\n" if props or ttl_dict[enum] not in ['"[]"', ""] else " .\n"
				f.write("\t"+f"{tag_dict[key][0]}:{key} {''.join([ttl_dict[key], line_end])}")
			if ttl_dict[enum] not in ['"[]"', ""]:
				line_end = ";\n" if props else " .\n"
				f.write("\t"+f"{tag_dict[enum][0]}:{enum} {''.join([ttl_dict[enum], line_end])}")
			if props:
				end = len(props)
				i = 0
				for prop in props:
					i += 1
					line_end = ";\n" if i < end else " .\n"
					if props[prop] and props[prop] != "TBD":
						f.write("\t"+f"{tag_dict[prop][0]}:{prop} {''.join([props[prop], line_end])}")
	
	with open(out_file, "r") as f:
		current_lines = f.read()
	
	with open(out_file, "w+") as f:
		prefix_set = set(prefix_list)
		node_set = set(node_list)
		first_lines = [f"@prefix {tag_dict[prefix][0]}: {tag_dict[prefix][1]}"+" .\n" for prefix in prefix_set]
		org_line = f"@prefix {args.org_name}: <{base_ref}:{args.org_name}/> .\n"
		node_lines = "".join([f"@prefix {node_type.lower().replace(' ', '_').replace('10x_', '')}: <{args.org_name}:{node_type.lower().replace(' ', '_').replace('10x_', '')}/> .\n" for node_type in node_set])
		first_lines_set = "".join(set(first_lines))
		f.write(first_lines_set)
		f.write(org_line)
		f.write(node_lines)
		f.write(current_lines)
		f.write("\n")
		if key_tuple_list is not None:
			for primary, schema, foreign in key_tuple_list:
				if "id" in primary.split("_"):
					line = f"{':'.join([str(primary).split('_')[0].lower(), str(primary).lower()])} {str(schema).lower()} {str(foreign).lower()} .\n"
					f.write(line)
	
	print(f"Done ✅")
	print(f"{out_file} was written with {len(ttl_df)} triples!")
	
	g = rdflib.Graph()
	model_graph = g.parse(out_file, format="turtle")
	image_path = "/".join([args.output, f"{args.org_name}_{node_name}_{args.version}.png"])

	if args.build_graph is not None:
		retry = 1
		image = None
		while image is None:
			if retry > 0:
				value_tag = rdflib.URIRef(f"{base_tag}/acceptableValues")
				model_graph = model_graph.remove((None, value_tag, None))
			dot_stream = io.StringIO()
			rdf2dot.rdf2dot(model_graph, dot_stream)
			dot_string = dot_stream.getvalue()
			graph = pydotplus.graph_from_dot_data(dot_string)
			try:
				graph.write_png(image_path, prog="dot")
				image = Image.open(image_path)
				image.show()
				print(f"Success! Graph visualization is available at {image_path}")
				image = True
			except:
				print("Failed to generate a visualization of the graph. Retrying with fewer triples...")
				retry += 1
				if retry == 2:
					print("Failed to generate a visualization of the graph. Skipping.")
					with open("graph_string_error.txt", "w+") as f:
						f.write(graph.to_string())
					break
		
	if args.interactive_graph is not None:
		print("Generating interactive plot...")
		model_graph = rdflib_to_networkx_multidigraph(model_graph)
		nx.draw_networkx(model_graph, arrows=False, with_labels=True, font_size=4, node_size=200)
		plt.show()
	
	print(f"Done ✅")
		
if __name__ == "__main__":
    main()
