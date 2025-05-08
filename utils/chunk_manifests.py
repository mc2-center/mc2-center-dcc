"""Clean and prep metadata tables

This script will break manifests into multiple parts and remove unnecessary columns.

author: orion.banks
"""

from pathlib import Path
import pandas as pd
import sys

def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns with missing metadata."""

    for col in df.columns:
        if df[col].isnull().all():
            df.drop(col, axis=1, inplace=True)
            print(f"🗑️ Dropping column: {col}")

    return df


def main():
    """Main function."""
    input = sys.argv[1]
    chunks = int(sys.argv[2])
    clean = sys.argv[3] if sys.argv[3] == "clean" else None
    
    name = input.split("/")[-1].split(".csv")[0]

    output_folder = f"{name}_manifest_chunks/"

    Path(output_folder).mkdir(exist_ok=True)

    for _, chunk in enumerate(pd.read_csv(input, header=0, chunksize=chunks)):
        
        if clean is not None:
            chunk = drop_columns(chunk)
        
        path = output_folder + name + f"_chunk_{_}.csv"

        chunk.to_csv(path, index=False)
    
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
