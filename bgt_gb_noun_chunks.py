import pandas as pd
import spacy

# Load CSV directly (your 2022 job data)
df = pd.read_csv("your_2022_jobs.csv")

# Drop rows with missing descriptions (optional but useful)
df.dropna(subset=["description"], inplace=True)

# Remove numbers from text
def remove_numbers(text):
    return ''.join(filter(lambda c: not c.isdigit(), str(text)))

df["clean_description"] = df["description"].apply(remove_numbers)

# Load SpaCy model
nlp = spacy.load("en_core_web_md")  # use 'en_core_web_lg' if available
target_token = nlp("data")[0]  # this is the word you compare to

# Process descriptions
docs = list(nlp.pipe(df["clean_description"], disable=["ner", "lemmatizer"]))

# Extract noun chunks + cosine similarity
output = []

for i, doc in enumerate(docs):
    for chunk in doc.noun_chunks:
        if chunk.has_vector:
            output.append({
                "job_id": df.iloc[i]["job_id"],
                "title": df.iloc[i]["title"],
                "noun_chunk": chunk.text,
                "similarity_to_data": chunk.similarity(target_token)
            })

# Save result as a DataFrame
result_df = pd.DataFrame(output)

# Optional: Save to Parquet or CSV
result_df.to_csv("noun_chunks_with_similarity.csv", index=False)
# result_df.to_parquet("noun_chunks_with_similarity.parquet", index=False)

print("Done! Results saved.")
