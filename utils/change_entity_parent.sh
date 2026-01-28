#/usr/bin/bash

# Path to input CSV
INPUT="$1"

# Field separator in the input files
SEP=$","

tail -n +2 "${INPUT}" | tr -d '\r' | \
while IFS=',' read -r remove target; do
	if [[ -z "${remove}${target}" ]]; then
        continue
    fi
	synapse mv --id "${remove}" --parentid "${target}"
done
