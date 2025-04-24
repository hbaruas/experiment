import pandas as pd
import numpy as np

def reduce_file(country):
    year = 2022  # fixed for your current dataset

    # Load similarity results
    df = pd.read_csv("noun_chunks_with_similarity.csv")

    print("File has finished appending")

    # Clean text fields
    df['noun_chunk'] = df['noun_chunk'].astype(str).str.strip().str.lower()
    df['counter'] = 1

    # Optional placeholder: Uncomment if you ever add doc_BGTOCC
    # df['doc_BGTOCC'] = '0000'  # placeholder if needed
    # df['doc_BGTOCC'] = df['doc_BGTOCC'].astype(str).str.replace('[.]', '', regex=True)

    # Filter out low-similarity noun chunks
    df.loc[df['similarity_to_data'] < 0.45, 'noun_chunk'] = np.NaN

    # Group and aggregate
    df = df.groupby(by=['noun_chunk', 'job_id'], dropna=False).agg({
        'similarity_to_data': 'mean',
        'counter': 'sum'
    }).reset_index()

    print(df.head())
    print(f"Total reduced rows: {len(df)}")

    # Save result
    if country == 'UK':
        df.to_csv(f"reducedFile_{year}.csv", index=False)
        # Optional: df.to_parquet(f"reducedFile_{year}.parquet", index=False)

# Run script
if __name__ == "__main__":
    reduce_file('UK')
