#!/bin/bash
#Outlines workflow for curation processing and upload 
#author: aditi.gopalan

#Make sure to run this script from the root directory (~/)end


# Activate the Conda environment
echo "Activating conda environment: schematic2"
conda init
conda activate schematic2


# Run python3 split_manifest_grants.py
echo "Splitting manifests..."
python3 ~/Documents/code/mc2-center-dcc/annotations/split_manifest_grants.py ~/Documents/code/mc2-center-dcc/annotations/input/jan25_pubs.csv publication ~/Documents/code/mc2-center-dcc/annotations/output/output_jan25 --csv

# Check if split_manifest_grants.py was successful
if [ $? -eq 0 ]; then
    # Generate file paths for split manifests
    echo "Generating file paths..."
    python3 ~/Documents/code/mc2-center-dcc/annotations/gen-mp-csv.py ~/Documents/code/mc2-center-dcc/annotations/output/output_nov ~/Documents/code/mc2-center-dcc/annotations/input/jan25_filepaths.csv publications

    # Check if file path generation was successful 
    if [ $? -eq 0 ]; then
        # Format manifests
        echo "Processing split files..."
        python3 ~/Documents/code/mc2-center-dcc/annotations/processing-splits.py ~/Documents/code/mc2-center-dcc/annotations/output/output_jan25
        # Check if formatting was successful
        if [ $? -eq 0 ]; then
            # Run schema updates, generating IDs
            echo "Running schema updates..."
            python3 ~/Documents/code/mc2-center-dcc/annotations/schema_update.py ~/Documents/code/mc2-center-dcc/annotations/input/jan25_filepaths.csv Publication

            # echo "Generating Dataset/ Tool/ Educational Resource folder Ids"
            # python3 create_id_folders.py -m nov_datasets_filepaths.csv -t DatasetView

            # Check if schema_update.py was successful
            if [ $? -eq 0 ]; then
                # Run python3 upload-manifests.py
                echo "Uploading manifests..."
                python3 ~/Documents/code/mc2-center-dcc/annotations/upload-manifests.py -m ~/Documents/code/mc2-center-dcc/annotations/input/jan25_filepaths.csv -t PublicationView -c ~/config.yml

                if [ $? -eq 0 ]; then
                # Run python3 upload-validation.py
                echo "Validate Uploads..."
                python3 ~/Documents/code/mc2-center-dcc/annotations/upload_validation.py ~/Documents/code/mc2-center-dcc/annotations/input/jan25_filepaths.csv Publication

                    # Check if upload-manifests.py was successful
                    if [ $? -eq 0 ]; then
                        echo "All scripts executed successfully."
                    else
                        echo "Error: upload_validation.py failed."
                    fi
                else
                    echo "Error: upload-manifests.py failed."
                fi
            else
                echo "Error: schema_update.py failed."
            fi
        else
            echo "Error: upload-manifests.py failed."
        fi
    else
        echo "Error: Formatting failed"
    fi
else
    echo "Error: split_manifest_grants.py failed."
fi