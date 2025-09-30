import json
import argparse
import pathlib


def generate_json_schema(input):
    """
    Corrects conditional logic for arrays.
    """

    output = "".join([pathlib.Path(input).stem, "-updated.json"]) 
    
    with open(input, 'r') as i:
        model = json.load(i)

    conditions = model["allOf"]

    for property in conditions:
        for prop in property["if"]["properties"]:
            if property["if"]["properties"][prop]["enum"]:
                property["if"]["properties"][prop] = {"contains": property["if"]["properties"][prop]}
    
    with open(output, 'w') as f:
        json.dump(model, f, indent=2)

    print(f"✅ JSON Schema written to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate corrected JSON schema")
    parser.add_argument("input_path", help="Path to a JSON schema file.")
    args = parser.parse_args()

    
    generate_json_schema(args.input_path)