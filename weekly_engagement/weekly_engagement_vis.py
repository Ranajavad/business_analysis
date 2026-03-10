import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pathlib

# Output folder
output_dir = pathlib.Path("weekly_engagement")
output_dir.mkdir(exist_ok=True)

# Data
data = {
"day_of_week":["Monday","Friday","Sunday","Saturday","Thursday","Tuesday","Wednesday"],
"total_interactions":[3284782,3230755,3156461,2936581,2615982,2434973,2348756],
"daily_accuracy":[89.58,89.53,89.55,89.67,89.66,89.66,89.60]
}

df = pd.DataFrame(data)

# Order days correctly
order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
df["day_of_week"] = pd.Categorical(df["day_of_week"], categories=order, ordered=True)
df = df.sort_values("day_of_week")

sns.set_theme(style="whitegrid")

# -------- Engagement Chart --------

plt.figure(figsize=(10,6))

sns.barplot(
    data=df,
    x="day_of_week",
    y="total_interactions",
    palette="Blues"
)

plt.title("Weekly Learning Activity", fontsize=16)
plt.xlabel("Day of Week")
plt.ylabel("Total Learning Interactions")

plt.tight_layout()

plt.savefig(output_dir / "weekly_engagement.png")
plt.close()


# -------- Accuracy Chart --------

plt.figure(figsize=(10,6))

sns.lineplot(
    data=df,
    x="day_of_week",
    y="daily_accuracy",
    marker="o"
)

plt.title("Learning Accuracy Throughout the Week", fontsize=16)
plt.xlabel("Day of Week")
plt.ylabel("Accuracy (%)")

plt.ylim(89.4, 89.8)

plt.tight_layout()

plt.savefig(output_dir / "weekly_accuracy.png")
plt.close()

print("✅ Weekly visualizations saved")