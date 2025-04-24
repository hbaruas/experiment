import pandas as pd
import matplotlib.pyplot as plt

# Load the final classified file
df = pd.read_csv("final_classified_noun_chunks.csv")

# Only keep classified as "data_intensive"
intensive_df = df[df["classification"] == "data_intensive"]

# --- Chart 1: Count per subcategory ---
subcat_counts = intensive_df["subcategory"].value_counts()

plt.figure(figsize=(8, 5))
subcat_counts.plot(kind='bar', color='skyblue')
plt.title("Count of Noun Chunks per Data Subcategory")
plt.xlabel("Subcategory")
plt.ylabel("Number of Chunks")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("chart_subcategory_counts.png")
plt.close()

# --- Chart 2: Top 10 most common data-intensive noun chunks ---
top_chunks = intensive_df["noun_chunk"].value_counts().head(10)

plt.figure(figsize=(10, 6))
top_chunks.plot(kind='barh', color='orange')
plt.title("Top 10 Most Frequent Data-Intensive Noun Chunks")
plt.xlabel("Frequency")
plt.ylabel("Noun Chunk")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("chart_top_10_chunks.png")
plt.close()

# --- Chart 3: Dispersion vs Similarity Scatter (Optional Insight) ---
plt.figure(figsize=(10, 6))
plt.scatter(intensive_df["dispersion_score"], intensive_df["similarity_to_data"], alpha=0.5)
plt.title("Dispersion Score vs Cosine Similarity (Data-Intensive Only)")
plt.xlabel("Dispersion Score")
plt.ylabel("Cosine Similarity to 'data'")
plt.grid(True)
plt.tight_layout()
plt.savefig("chart_dispersion_vs_similarity.png")
plt.close()

# --- Chart 4: Subcategory by SOC Code (stacked bar) ---
soc_subcat = intensive_df.groupby(["soc_code", "subcategory"]).size().unstack(fill_value=0)

top_socs = soc_subcat.sum(axis=1).sort_values(ascending=False).head(10).index
filtered = soc_subcat.loc[top_socs]

filtered.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='Set2')
plt.title("Top 10 SOC Codes by Subcategory (Stacked)")
plt.xlabel("SOC Code")
plt.ylabel("Number of Chunks")
plt.legend(title="Subcategory", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig("chart_soc_subcategory_stacked.png")
plt.close()


# --- Chart 5: Top Noun Chunks from Non-Data-Intensive Group ---
non_df = df[df["classification"] == "non_data_intensive"]
top_non_chunks = non_df["noun_chunk"].value_counts().head(10)

plt.figure(figsize=(10, 6))
top_non_chunks.plot(kind='barh', color='lightcoral')
plt.title("Top 10 Most Frequent Non-Data-Intensive Noun Chunks")
plt.xlabel("Frequency")
plt.ylabel("Noun Chunk")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("chart_top_10_non_data_chunks.png")
plt.close()


print("📊 Visualizations saved:")
print("  • chart_subcategory_counts.png")
print("  • chart_top_10_chunks.png")
print("  • chart_dispersion_vs_similarity.png")
print("  • chart_soc_subcategory_stacked.png")
print("  • chart_top_10_non_data_chunks.png (non-data-intensive chunks)")

