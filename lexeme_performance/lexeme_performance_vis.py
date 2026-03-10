import pandas as pd
import matplotlib
matplotlib.use('Agg')  # prevents Tkinter
import matplotlib.pyplot as plt
import seaborn as sns
import pathlib

# Output folder
output_dir = pathlib.Path("lexeme_performance")
output_dir.mkdir(exist_ok=True)

# Data
data = {
"word":["since","vez","were","refeição","flute","used","lot","went","looks","said"],
"total_attempts":[1003,4905,3880,1541,1004,3570,2471,1587,1364,2037],
"total_correct":[680,3588,2848,1134,739,2632,1822,1216,1046,1565],
"accuracy_percentage":[67.80,73.15,73.40,73.59,73.61,73.73,73.74,76.62,76.69,76.83]
}

df = pd.DataFrame(data)

# Sort by accuracy (lowest first)
df = df.sort_values("accuracy_percentage")

sns.set_theme(style="whitegrid")

plt.figure(figsize=(10,6))

sns.barplot(
    data=df,
    x="accuracy_percentage",
    y="word",
    palette="Reds_r"
)

plt.title("Top 10 Most Difficult Words for Learners", fontsize=16)
plt.xlabel("Accuracy (%)")
plt.ylabel("Word")
plt.xlim(65,80)

plt.tight_layout()

plt.savefig(output_dir / "hardest_words_accuracy.png")
plt.close()

print("✅ Visualization saved in business.analysis folder")