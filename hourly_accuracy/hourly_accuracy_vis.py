import pandas as pd
import matplotlib
matplotlib.use('Agg')  # prevents Tkinter
import matplotlib.pyplot as plt
import seaborn as sns
import pathlib

# --- Create output folder ---
output_dir = pathlib.Path("hourly_accuracy")
output_dir.mkdir(exist_ok=True)

# --- Data ---
data = {
"hour_of_day":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23],
"avg_memory_strength":[89.65,89.71,89.82,89.94,89.25,88.99,89.68,89.62,89.88,89.53,89.31,89.42,89.60,89.61,89.58,89.77,89.67,89.58,89.50,89.71,89.70,89.67,89.59,89.32],
"session_accuracy":[90.46,90.52,90.57,90.72,90.20,90.03,90.50,90.49,90.64,90.43,90.21,90.32,90.44,90.48,90.50,90.54,90.53,90.46,90.35,90.54,90.53,90.50,90.40,90.24],
"total_interactions":[1002606,1022233,1036025,943389,778233,585366,428870,388113,376213,418568,491952,551635,634180,753427,865616,983542,999419,1020706,1066270,1081915,1164956,1210737,1158757,1045562]
}

df = pd.DataFrame(data)

sns.set_theme(style="whitegrid")

# -------------------------
# 1️⃣ USER ACTIVITY
# -------------------------
plt.figure(figsize=(12,6))

sns.barplot(
    data=df,
    x="hour_of_day",
    y="total_interactions",
    color="skyblue"
)

plt.title("User Activity by Hour of Day", fontsize=16)
plt.xlabel("Hour of Day")
plt.ylabel("Total Interactions")
plt.tight_layout()

plt.savefig(output_dir / "hourly_user_activity.png")
plt.close()

# -------------------------
# 2️⃣ SESSION ACCURACY
# -------------------------
plt.figure(figsize=(12,6))

sns.lineplot(
    data=df,
    x="hour_of_day",
    y="session_accuracy",
    marker="o",
    linewidth=3,
    color="green"
)

plt.title("Session Accuracy by Hour of Day", fontsize=16)
plt.xlabel("Hour of Day")
plt.ylabel("Accuracy (%)")
plt.ylim(89.8,91)
plt.tight_layout()

plt.savefig(output_dir / "hourly_session_accuracy.png")
plt.close()

# -------------------------
# 3️⃣ MEMORY STRENGTH
# -------------------------
plt.figure(figsize=(12,6))

sns.lineplot(
    data=df,
    x="hour_of_day",
    y="avg_memory_strength",
    marker="o",
    linewidth=3,
    color="blue"
)

plt.title("Average Memory Strength by Hour of Day", fontsize=16)
plt.xlabel("Hour of Day")
plt.ylabel("Memory Strength (%)")
plt.ylim(88.8,90)
plt.tight_layout()

plt.savefig(output_dir / "hourly_memory_strength.png")
plt.close()

print("✅ All visualizations saved in business.analysis folder")