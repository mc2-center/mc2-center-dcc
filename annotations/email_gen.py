import os
import pandas as pd

# Step 1: Get a list of all files in the folder
folder_path = os.getcwd()
files = os.listdir(folder_path)

# Step 2: Find .xlsx files with the same first 8 characters
file_names_dict = {}
for file in files:
    if file.endswith(".xlsx"):  # Check if the file is an Excel file
        file_name = os.path.splitext(file)[0]
        first_8_chars = file_name[:8]
        if first_8_chars in file_names_dict:
            file_names_dict[first_8_chars].append(file)
        else:
            file_names_dict[first_8_chars] = [file]

# Step 3 and 4: Create a text file with the desired template
output_file = "output.txt"
with open(output_file, "w") as f:
    for first_8_chars, file_list in file_names_dict.items():
        template = "DATASET var_data TEMPLATE" if any(file.endswith("_dataset.xlsx") for file in file_list) else "PUBLICATION TEMPLATE"

        # If 'DATASET TEMPLATE,' process XLSX files to extract 'Dataset Alias'
        if template == "DATASET var_data TEMPLATE":
            dataset_aliases = set()  # Use a set to store unique entries
            for file in file_list:
                xlsx_file_path = os.path.join(folder_path, file)
                try:
                    df = pd.read_excel(xlsx_file_path)
                    dataset_alias_values = df['Publication Dataset Alias'].values
                    dataset_aliases.update(dataset_alias_values)  # Add unique aliases to the set
                except Exception as e:
                    print(f"Error reading {file}: {e}")

            # Print dataset aliases
            dataset_aliases = {alias for alias in dataset_aliases if alias != "Not Applicable"}
            dataset_aliases = [alias for alias in dataset_aliases if pd.notna(alias)]
            print(dataset_aliases)

            # Save the dataset aliases as a string separated by a semicolon
            dataset_aliases_str = "".join(dataset_aliases)
            template = """Dear , 

            As part of the ongoing efforts within the Multi-Consortia Coordinating (MC2) Center to maximize the impact and FAIRness of resources created by NIH/NCI Division of Cancer Biology (DCB) consortia members, we wanted to update you on the continued transition towards a contributor-focused model of resource curation and would like to provide an opportunity for you to become involved in this process.

            Every month, we run a publication crawler script to identify publications in PubMed emerging from DCB consortia grants, where we annotate your publications and identify any associated datasets and tools. These annotations enable your resources to be findable and searchable in the Cancer Complexity Knowledge Portal. Recognizing that the most accurate annotations are provided by data and tool generators directly, we are transitioning to support community-driven annotations so that you can easily and accurately annotate your own resources for submission and dissemination via the portal.

            To continue the transition from our current process to a community-driven process, we are reaching out to ask for your input regarding the annotation of your resources. This process is not currently expected to take a lot of time on your part, but your feedback in this process will be extremely valuable. Of note, this will be an evolving and iterative process.

            As we did previously, we are sending you publications that we annotated for you to the best of our abilities. We request that you take a look at the attached publication “manifests” (spreadsheets) of your resources that were picked up for the month of June. However, this month we did not annotate the tools or datasets associated with the publications. For the publications that included a tool or novel dataset, we have attached an empty manifest for you to complete. Please use the next two weeks to review the manifests and reply to this email with:
            1. Confirmation of the resources captured. Is there anything missing and needs to be added (e.g. additional datasets, tools, etc.)?
            2. Any changes you would like to see in the annotations: is there a term used that is inaccurate? If so, what would be better?
            3. Any feedback on the current metadata being collected about your resources.
            4. For datasets : a completed manifest of the datasets annotated by you and any feedback about the process. For the listed publication, we identified var_data as related datasets.

            Please respond no later than November 3rd, 2023. Please feel free to share this email and attachments with members of your research team to support this process.

            We also encourage you to review any existing resources we have curated and disseminated via the Cancer Complexity Knowledge Portal. Visit the portal at https://cancercomplexity.synapse.org/ -> Select “Explore” and “Grants” in the top right corner -> select the magnifying glass icon to open the search bar to search for your grant.

            Please note the resources reflected in the attached manifests are not intended to reflect the entirety of resources for a given grant, but only the resources added to PubMed during the month (in this case - August).

            We greatly appreciate your time and feedback, and we look forward to working with you to make your resources FAIR!

            Thank you,
            Aditi Gopalan, on behalf of the MC2 Center"""
            template = template.replace("var_data", dataset_aliases_str)

        elif template == "PUBLICATION TEMPLATE":
            template = """Dear , 

            As part of the ongoing efforts within the Multi-Consortia Coordinating (MC2) Center to maximize the impact and FAIRness of resources created by NIH/NCI Division of Cancer Biology (DCB) consortia members, we wanted to update you on the continued transition towards a contributor-focused model of resource curation and would like to provide an opportunity for you to become involved in this process.

            Every month, we run a publication crawler script to identify publications in PubMed emerging from DCB consortia grants, where we annotate your publications and identify any associated datasets and tools. These annotations enable your resources to be findable and searchable in the Cancer Complexity Knowledge Portal. Recognizing that the most accurate annotations are provided by data and tool generators directly, we are transitioning to support community-driven annotations so that you can easily and accurately annotate your own resources for submission and dissemination via the portal.

            To continue the transition from our current process to a community-driven process, we are reaching out to ask for your input regarding the annotation of your resources. This process is not currently expected to take a lot of time on your part, but your feedback in this process will be extremely valuable. Of note, this will be an evolving and iterative process.

            As we did previously, we are sending you publications that we annotated for you to the best of our abilities. We request that you take a look at the attached publication “manifests” (spreadsheets) of your resources that were picked up for the month of June. However, this month we did not annotate the tools or datasets associated with the publications. For the publications that included a tool or novel dataset, we have attached an empty manifest for you to complete. Please use the next two weeks to review the manifests and reply to this email with:
            1. Confirmation of the resources captured. Is there anything missing and needs to be added (e.g. additional datasets, tools, etc.)?
            2. Any changes you would like to see in the annotations: is there a term used that is inaccurate? If so, what would be better?
            3. Any feedback on the current metadata being collected about your resources.

            Please respond no later than November 3rd, 2023. Please feel free to share this email and attachments with members of your research team to support this process.

            We also encourage you to review any existing resources we have curated and disseminated via the Cancer Complexity Knowledge Portal. Visit the portal at https://cancercomplexity.synapse.org/ -> Select “Explore” and “Grants” in the top right corner -> select the magnifying glass icon to open the search bar to search for your grant.

            Please note the resources reflected in the attached manifests are not intended to reflect the entirety of resources for a given grant, but only the resources added to PubMed during the month (in this case - August).

            We greatly appreciate your time and feedback, and we look forward to working with you to make your resources FAIR!

            Thank you,
            Aditi Gopalan, on behalf of the MC2 Center"""

        spacer = "##################################"
        f.write(f"\n {first_8_chars} \n {template} \n {spacer} \n \n")

print(f"Output written to {output_file}")

