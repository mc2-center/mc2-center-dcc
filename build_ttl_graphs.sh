#/usr/bin/bash

#Usage: bash build_ttl_graphs.sh [SOURCE_DIR] [FILE_EXTENSION] [ORG_NAME] [VERSION]

#This script will convert files of the given extension to TTL-formatted RDF via csv_to_ttl.py
#It will concatenate multiple TTLs to a single file, if multiple were provided in the source directory


dir="$1"
datatype="$2"
org="$3"
version="$4"
outdir="$org"_ttl_graphs

mkdir -p ./"$outdir"

for file in "$dir"/*."$datatype"; do
	if [ -f "$file" ]; then
		python utils/csv_to_ttl.py -m "$file" -g "$org" -o "$outdir" -v "$version"
	fi
done

for ttl in "$outdir"/*.ttl; do
	if [ -f "$ttl" ]; then
		cat "$ttl" >> "$org"_"$version"_model.ttl
	fi
done

mv "$org"_"$version"_model.ttl "$outdir"
