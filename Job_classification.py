import pandas as pd

# Load enriched file
df = pd.read_csv("enriched_reducedFile_2022.csv")

# --- Apply Screenshot Criteria ---
def classify_data_intensive(row):
    if (
        row['similarity_to_data'] >= 0.45 and
        row['dispersion_score'] <= 0.6 and
        row['landmark_flag'] == True
    ):
        return "data_intensive"
    else:
        return "non_data_intensive"

df['classification'] = df.apply(classify_data_intensive, axis=1)

# --- Optional: Breakdown within data_intensive category ---
# Using mock rules to split further
def assign_subcategory(row):
    if row['classification'] != "data_intensive":
        return "not_applicable"
    noun = str(row['noun_chunk']).lower()
    if any(term in noun for term in ["analysis", "analytics", "insight", "modelling", "visualisation"]):
        return "data_analytics"
    elif any(term in noun for term in ["database", "sql", "oracle", "postgres", "mysql"]):
        return "database"
    elif any(term in noun for term in ["input", "entry", "form", "spreadsheet", "typing"]):
        return "data_entry"
    return "general_data_intensive"

df['subcategory'] = df.apply(assign_subcategory, axis=1)

# Save final classified file
df.to_csv("final_classified_noun_chunks.csv", index=False)
print("🎯 Classification complete! Saved to 'final_classified_noun_chunks.csv'")
