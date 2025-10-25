import numpy as np
import pandas as pd
from pandasql import sqldf
import matplotlib.pyplot as plt
import queries

#Load Results file into Dataframe
df = pd.read_csv('results.csv', header=0)

#Separate Hash Generation - small results
hash_gen_sm_df = df[df["k"] == 26]
#Separate Hash Generation - large results
hash_gen_lg_df = df[df["k"] == 32]


#Analysis
#Hash Generation - Small (k=26)
result_df_hash_gen_sm = sqldf(queries.query_hash_gen_sm, globals()) # or locals()
#print(result_df_hash_gen_sm)

#Hash Generation - Large (k=32)
result_df_hash_gen_lg = sqldf(queries.query_hash_gen_lg, globals()) # or locals()
print(result_df_hash_gen_lg)

#Plots

#Heat Map for Best SM Config
result_df_hash_gen_sm_best = sqldf(queries.query_hash_gen_sm_best_config, globals()) # or locals()
#print(result_df_hash_gen_sm_best)

# pivot the data
pivot = result_df_hash_gen_sm_best.pivot(index="threads", columns="mem_mb", values="best_time")

fig, ax = plt.subplots(figsize=(8,6))
im = ax.imshow(pivot, cmap="viridis", origin="lower")

# set ticks
ax.set_xticks(np.arange(len(pivot.columns)))
ax.set_xticklabels(pivot.columns)
ax.set_yticks(np.arange(len(pivot.index)))
ax.set_yticklabels(pivot.index)

ax.set_xlabel("Memory (MB)")
ax.set_ylabel("Threads")
ax.set_title("Minimum I/O Time by Threads and Memory")

# annotate values
for i in range(len(pivot.index)):
    for j in range(len(pivot.columns)):
        ax.text(j, i, f"{pivot.iloc[i,j]:.2f}", ha="center", va="center", color="white", fontsize=8)

# add colorbar
cbar = fig.colorbar(im)
cbar.set_label("Best I/O Time")

plt.tight_layout()
plt.show()


#Line Graph for Best LG Config
plt.figure(figsize=(8,5))
plt.plot(
    result_df_hash_gen_lg["mem_mb"],
    result_df_hash_gen_lg["iot_1"],
    marker="o",
    linestyle="-",
    color="tab:blue",
    label="Time"
)

plt.title("Time vs Memory Size (threads = 24)")
plt.xlabel("Memory (MB)")
plt.ylabel("Time (seconds)")
plt.grid(True, linestyle="--", alpha=0.6)
plt.legend()
plt.tight_layout()
plt.show()




