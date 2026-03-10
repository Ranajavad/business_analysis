import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pathlib

# Output folder
output_dir = pathlib.Path("language_success")
output_dir.mkdir(exist_ok=True)

# Data
data = {
"learning_language":["en","es","fr","de","it","pt"],
"unique_learners":[43805,31155,19947,14383,6498,2709],
"total_lessons_taken":[7822709,5306643,2900837,2266339,1227650,484112],
"avg_recall_rate":[89.77,89.83,88.28,89.30,90.88,90.65]
}

df = pd.DataFrame(data)

sns.set_theme(style="whitegrid")

# -------------------------
# 1️⃣ TOTAL LESSONS
# -------------------------
plt.figure(figsize=(10,6))

sns.barplot(
    data=df,
    x="learning_language",
    y="total_lessons_taken",
    palette="Blues_d"
)

plt.title("Total Lessons Taken by Language", fontsize=16)
plt.xlabel("Language")
plt.ylabel("Total Lessons")
plt.tight_layout()

plt.savefig(output_dir / "language_lessons.png")
plt.close()


# -------------------------
# 2️⃣ UNIQUE LEARNERS
# -------------------------
plt.figure(figsize=(10,6))

sns.barplot(
    data=df,
    x="learning_language",
    y="unique_learners",
    palette="Greens_d"
)

plt.title("Number of Unique Learners by Language", fontsize=16)
plt.xlabel("Language")
plt.ylabel("Unique Learners")
plt.tight_layout()

plt.savefig(output_dir / "language_learners.png")
plt.close()


# -------------------------
# 3️⃣ RECALL RATE
# -------------------------
plt.figure(figsize=(10,6))

sns.barplot(
    data=df,
    x="avg_recall_rate",
    y="learning_language",
    palette="Oranges_d"
)

plt.title("Average Recall Rate by Language", fontsize=16)
plt.xlabel("Recall Rate (%)")
plt.ylabel("Language")
plt.xlim(87,92)

plt.tight_layout()

plt.savefig(output_dir / "language_recall_rate.png")
plt.close()

print("✅ Language visualizations saved.")