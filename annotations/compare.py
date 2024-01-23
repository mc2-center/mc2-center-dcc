import pandas as pd

# Load the 'publication.csv', 'dataset.csv', and 'tool.csv' into DataFrames
publication_df = pd.read_csv('2023-09-01_publications-manifest.csv')
dataset_df = pd.read_csv('sep_datasetview.csv')
tool_df = pd.read_csv('sep_toolview.csv')

# Add the 'Dataset Grant Number' column to 'dataset.csv' by mapping 'Pubmed Id'
dataset_df['Dataset Grant Number'] = dataset_df['Pubmed Id'].map(publication_df.set_index('Pubmed Id')['Publication Grant Number'])

# Add the 'Tool Grant Number' column to 'tool.csv' by mapping 'Pubmed Id'
tool_df['Tool Grant Number'] = tool_df['Pubmed Id'].map(publication_df.set_index('Pubmed Id')['Publication Grant Number'])

# Add additional columns for 'datasetview_sep.csv'
dataset_df['Dataset Name'] = ''  # You can set the values as needed
dataset_df['Dataset Description'] = ''
dataset_df['Dataset Design'] = ''
dataset_df['Dataset Assay'] = ''
dataset_df['Dataset Species'] = ''
dataset_df['Dataset Tumor Type'] = ''
dataset_df['Dataset Tissue'] = ''
dataset_df['Dataset Url'] = ''
dataset_df['Dataset File Formats'] = ''
dataset_df['Component'] = 'DatasetView'

# Add additional columns for 'toolview_sep.csv'
tool_df['Tool Name'] = ''  # You can set the values as needed
tool_df['Tool Description'] = ''
tool_df['Tool Version'] = ''
tool_df['Tool Operation'] = ''
tool_df['Tool Input Data'] = ''
tool_df['Tool Output Data'] = ''
tool_df['Tool Input Format'] = ''
tool_df['Tool Output Format'] = ''
tool_df['Tool Function Note'] = ''
tool_df['Tool Cmd'] = ''
tool_df['Tool Type'] = ''
tool_df['Tool Topic'] = ''
tool_df['Tool Operating System'] = ''
tool_df['Tool Language'] = ''
tool_df['Tool License'] = ''
tool_df['Tool Cost'] = ''
tool_df['Tool Accessibility'] = ''
tool_df['Tool Download Url'] = ''
tool_df['Tool Download Type'] = ''
tool_df['Tool Download Note'] = ''
tool_df['Tool Download Version'] = ''
tool_df['Tool Documentation Url'] = ''
tool_df['Tool Documentation Type'] = ''
tool_df['Tool Documentation Note'] = ''
tool_df['Tool Link Url'] = ''
tool_df['Tool Link Type'] = ''
tool_df['Tool Link Note'] = ''
tool_df['Component'] = 'ToolView'

# Save the updated 'datasetview_sep.csv' and 'toolview_sep.csv'
dataset_df.to_csv('datasetview_sep.csv', index=False)
tool_df.to_csv('toolview_sep.csv', index=False)
