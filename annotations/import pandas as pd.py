import pandas as pd

# Load the first CSV file
df1 = pd.read_csv('2023-09-01_publications-manifest.csv')

# Load the second CSV file
df2 = pd.read_csv('datasetview_sep.csv')

# Merge the dataframes based on the "Pubmed Id" column
merged_df = pd.merge(df2, df1[['Pubmed Id', 'Publication Assay', 'Publication Tumor Type', 'Publication Tissue']],
                     on='Pubmed Id', how='left', suffixes=('', '_from_file1'))

# Fill NaN values with the original values from the second file
merged_df['Dataset Assay'] = merged_df['Publication Assay_from_file1'].fillna(merged_df['Dataset Assay'])
merged_df['Dataset Tumor Type'] = merged_df['Publication Tumor Type_from_file1'].fillna(merged_df['Dataset Tumor Type'])
merged_df['Dataset Tissue'] = merged_df['Publication Tissue_from_file1'].fillna(merged_df['Dataset Tissue'])

# Drop the columns used for merging
merged_df = merged_df.drop(columns=['Publication Assay_from_file1', 'Publication Tumor Type_from_file1', 'Publication Tissue_from_file1'])

# Save the updated dataframe to the second file
merged_df.to_csv('file2_updated.csv', index=False)
