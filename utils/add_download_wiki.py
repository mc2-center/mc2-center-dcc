"""add_download_wiki.py

This script will add wiki content to a Synapse Entity.

Usage:
python add_download_wiki.py -d <Dataset Synapse Id> -s <Folder Synapse Id containing files for Dataset> -c <CSV with dataset and folder Synapse Ids>

author: orion.banks
"""

import synapseclient
from synapseclient import Wiki
import argparse
import pandas as pd


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="Synapse Id of a Dataset to update with files from input folder",
        required=False
    )
    parser.add_argument(
        "-s",
        type=str,
        help="Synapse Id of a folder that should have all files added to the Dataset",
        required=False
    )
    parser.add_argument(
        "-n",
        type=str,
        help="Brief text description of the dataset",
        required=False
    )
    parser.add_argument(
        "-c",
        type=str,
        help="Path to a CSV file with Dataset and Folder Synapse Ids.",
        required=False
    )
    return parser.parse_args()

def build_wiki(description, entity, scopeId):

    desc = f"""
##Dataset description:

#### {description}

"""
    option1 = """
##Available methods for downloading the files and annotations contained in this dataset

<summary>Option 1: Download a single file from this dataset</summary>
<details>

####From the Dataset table, select the Synapse Id of the file you would like to download:
${image?fileName=select_single_file.png&align=Left&scale=75&responsive=true&altText=Screenshot showing a single SynapseId can be selected to access the record of the associated file.}
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
####On the file record page, click on "Download Options" and select from the available methods to access the file:
${image?fileName=file_download_options.png&align=Left&scale=75&responsive=true&altText=Screenshot showing available download options on a single file within Synapse.}
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
- Select **Download File** to initiate the single file download through your browser.
&nbsp;

- Select **Add to Download Cart** to add the file to your Synapse Download Cart. See this page in the Synapse documentation on [downloading data using the cart](https://help.synapse.org/docs/Downloading-Data-From-the-Synapse-UI.2004254837.html).
&nbsp;

- Select **Programmatic Options** to view code that can be used with Synapse clients to download the file. See this page in the Synapse documentation on [downloading data programmatically](https://help.synapse.org/docs/Downloading-Data-Programmatically.2003796248.html).

</details>

&nbsp;

""" 
    option2 = f"""
<summary>Option 2: Download files and replicate folder structure on your local storage system</summary>
<details>

####This Dataset is also available as a direct folder download, which will recreate the folder directory on your local storage system

####Folder Synapse ID: {scopeId}

####Please follow the instructions below to download the source folder for this Dataset

- Install the [Synapse Python client](https://python-docs.synapse.org/tutorials/installation/)

- Set up your [method of authentication](https://python-docs.synapse.org/tutorials/authentication/) for accessing Synapse

- Run the following `synapse get` command in your terminal to download the Dataset folder contents (includes files and folders):

    `synapse get {scopeId} -r --downloadLocation /path/to/folder`

    **Note**: Please ensure that you replace the `/path/to/folder` example path argument with a local folder path, .e.g. `user/Documents` or `.` (the current working directory). 
    The requested folders will be created in this directory and files will be organized into folders as they are shown in Synapse.

####Further information on the Synapse Python client is available at the following links:

- Visit the [synapse get](https://python-docs.synapse.org/tutorials/command_line_client/#get) documentation to learn about additional parameters that can be used with this command.
- General documentation on the [Synapse Python client](https://python-docs.synapse.org/)
- General documentation on the [Synapse Python client command line interface](https://python-docs.synapse.org/tutorials/command_line_client/)

</details>

&nbsp;
"""

    option3 = """
<summary>Option 3: Download all files in this dataset to a single folder</summary>
<details>

####From the Dataset table, select the Download Options icon:
${image?fileName=dataset_download_options.png&align=Left&scale=50&responsive=true&altText=Screenshot showing that dataset download options are available using the upward-facing arrow icon on the dataset toolbar.}
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
&nbsp;
####Choose from the available options:
${image?fileName=dataset_download_selection.png&align=None&scale=50&responsive=true&altText=Screenshot showing that dataset download options are available using the upward-facing arrow icon on the dataset toolbar.}

####- Select "Add All Files to Download Cart" to download the files from within your web browser. See this page in the Synapse documentation on [downloading data using the cart](https://help.synapse.org/docs/Downloading-Data-From-the-Synapse-UI.2004254837.html).
####- Select "Programmatic Options" to view code that can be used with Synapse clients to download the dataset. See this page in the Synapse documentation on [downloading data programmatically](https://help.synapse.org/docs/Downloading-Data-Programmatically.2003796248.html).

</details>

"""

    attachments = ["/Users/obanks/Documents/screenshots/select_single_file.png",
               "/Users/obanks/Documents/screenshots/file_download_options.png",
               "/Users/obanks/Documents/screenshots/dataset_download_options.png",
               "/Users/obanks/Documents/screenshots/dataset_download_selection.png"]

    wiki = Wiki(title="Download Files in this Dataset", owner=entity, markdown=desc+option1+option2+option3, attachments=attachments)

    return wiki

def main():

    syn = (
        synapseclient.login()
    )  # you can pass your username and password directly to this function

    args = get_args()

    datasetId, scopeId, description, idSheet= args.d, args.s, args.n, args.c  # assign path to manifest file from command line input

    if idSheet:
        idSet = pd.read_csv(idSheet, header=None)
        if idSet.iat[0,0] == "DatasetView_id" and idSet.iat[0,1] == "Folder_id":
            print(f"\nInput sheet read successfully...\n\nCreating Dataset wikis now:")
            idSet = idSet.iloc[1:,:]
            count = 0
            for row in idSet.itertuples(index=False):
                datasetId = row[0]
                scopeId = row[1]
                description = row[2]
                dataset = syn.get(datasetId)
                new_wiki = build_wiki(description, dataset, scopeId)
                new_wiki = syn.store(new_wiki)
                print(f"\nDataset {datasetId} successfully updated with wiki content")
                count += 1
            print(f"\n\nDONE ✅\n{count} Datasets processed")
        else:
            print(f"\n❗❗❗ The table provided does not appear to be formatted for this operation.❗❗❗\nPlease check its contents and try again.")
    
    else:
        if datasetId and scopeId:
            dataset = syn.get(datasetId)

            new_wiki = build_wiki(description, dataset, scopeId)
            new_wiki = syn.store(new_wiki)
            print(f"\nDataset {datasetId} successfully updated with wiki content")
        else:
            print(f"\n❗❗❗ No dataset information provided.❗❗❗\nPlease check your command line inputs and try again.")


if __name__ == "__main__":
    main()
