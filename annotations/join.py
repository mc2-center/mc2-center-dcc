import pandas as pd

# Load the first CSV file
df1 = pd.read_csv('2023-09-01_publications-manifest.csv')

# Load the second CSV file
df2 = pd.read_csv('datasetview_sep.csv')

# Merge based on "Pubmed Id" and update values in the second file
merged_df = pd.merge(df2, df1[['Pubmed Id', 'Publication Assay', 'Publication Tumor Type', 'Publication Tissue']],
                     on='Pubmed Id', how='left')

# Update values in the second file only where "Pubmed Id" matches
merged_df['Dataset Assay'] = merged_df['Publication Assay'].combine_first(merged_df['Dataset Assay'])
merged_df['Dataset Tumor Type'] = merged_df['Publication Tumor Type'].combine_first(merged_df['Dataset Tumor Type'])
merged_df['Dataset Tissue'] = merged_df['Publication Tissue'].combine_first(merged_df['Dataset Tissue'])

# Drop the columns used for merging
merged_df = merged_df.drop(columns=['Publication Assay', 'Publication Tumor Type', 'Publication Tissue'])

# Save the updated dataframe to the second file
merged_df.to_csv('datasetview_sep_updated.csv', index=False)

