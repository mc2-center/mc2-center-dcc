#!/bin/bash
#Outlines workflow for curation processing and upload 
#author: aditi.gopalan

# Activate the Conda environment
echo "Activating conda environment: schematic"
conda activate schematic

<<<<<<< Updated upstream
# Check if activation was successful
if [ $? -eq 0 ]; then
    # Run python3 split_manifest_grants.py
    echo "Splitting manifests..."
    python3 split_manifest_grants.py manifest_sep.csv publication ./output/output_sep --csv
=======

# Run python3 split_manifest_grants.py
echo "Splitting manifests..."
python3 split_manifest_grants.py april-datasets.csv dataset ./output/output_april --csv

# Check if split_manifest_grants.py was successful
if [ $? -eq 0 ]; then
    # Run python3 script_name.py
    echo "Generating file paths..."
    python3 gen-mp-csv.py ./output/output_april april_datasets_filepaths.csv datasets
>>>>>>> Stashed changes

    # Check if split_manifest_grants.py was successful
    if [ $? -eq 0 ]; then
<<<<<<< Updated upstream
        # Run python3 script_name.py
        echo "Generating file paths..."
        python3 gen-mp-csv.py ./output/output_june june_filepaths.csv publication

        # Check if script_name.py was successful
        if [ $? -eq 0 ]; then
            # Run python3 combined_script.py
            echo "Processing split files..."
            python3 processing-splits.py /path/to/your/output/folder/with/split/manifests

            # Check if combined_script.py was successful
            if [ $? -eq 0 ]; then
                # Run python3 schema_update.py
                echo "Running schema updates..."
                python3 schema_update.py june_filepaths.csv Publication
=======
        # Run python3 combined_script.py
        echo "Processing split files..."
        python3 processing-splits.py ./output/output_april
        # Check if combined_script.py was successful
        if [ $? -eq 0 ]; then
            # Run python3 schema_update.py
            # echo "Running schema updates..."
            # python3 schema_update.py april_datasets_filepaths.csv Dataset

            echo "Generating Dataset/ Tool/ Educational Resource folder Ids"
            python3 create_id_folders.py -m april_datasets_filepaths.csv -t DatasetView

            if [ $? -eq 0 ]; then
                # Run python3 upload-manifests.py
                echo "Uploading manifests..."
                python3 upload-manifests.py -m april_datasets_filepaths.csv -t DatasetView -c ./config.yml
>>>>>>> Stashed changes

                # Check if schema_update.py was successful
                if [ $? -eq 0 ]; then
                    # Run python3 upload-manifests.py
                    echo "Uploading manifests..."
                    python3 upload-manifests.py -m input.csv -t PublicationView -c /Users/agopalan/schematic/config.yml

                    # Check if upload-manifests.py was successful
                    if [ $? -eq 0 ]; then
                        echo "All scripts executed successfully."
                    else
                        echo "Error: upload-manifests.py failed."
                    fi
                else
                    echo "Error: schema_update.py failed."
                fi
            else
                echo "Error: combined_script.py failed."
            fi
        else
            echo "Error: script_name.py failed."
        fi
    else
        echo "Error: split_manifest_grants.py failed."
    fi
else
    echo "Error: Conda environment activation failed."
fi
