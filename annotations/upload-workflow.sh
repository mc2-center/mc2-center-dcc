#!/bin/bash
#Outlines workflow for curation processing and upload 
#author: aditi.gopalan

# Activate the Conda environment
echo "Activating conda environment: schematic"
conda activate schematic


# Run python3 split_manifest_grants.py
echo "Splitting manifests..."
python3 split_manifest_grants.py feb_manifest.csv publication ./output/output_feb --csv

# Check if split_manifest_grants.py was successful
if [ $? -eq 0 ]; then
    # Generate file paths for split manifests
    echo "Generating file paths..."
    python3 gen-mp-csv.py ./output/output_feb feb_filepaths.csv publications

    # Check if file path generation was successful 
    if [ $? -eq 0 ]; then
        # Format manifests
        echo "Processing split files..."
        python3 processing-splits.py ./output/output_feb
        # Check if formatting was successful
        if [ $? -eq 0 ]; then
            # Run schema updates, generating IDs
            echo "Running schema updates..."
            python3 schema_update.py feb_filepaths.csv Publication

            echo "Generating Dataset/ Tool/ Educational Resource folder Ids"
            python3 create_entity_links.py -m april_datasets_filepaths.csv -t DatasetView

            # Check if schema_update.py was successful
            if [ $? -eq 0 ]; then
                # Run python3 upload-manifests.py
                echo "Uploading manifests..."
                python3 upload-manifests.py -m feb_filepaths.csv -t PublicationView -c /Users/agopalan/schematic/config.yml

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
            echo "Error: upload-manifests.py failed."
        fi
    else
        echo "Error: Formatting failed"
    fi
else
    echo "Error: split_manifest_grants.py failed."
fi
else
    echo "Error: Conda environment activation failed."
fi
