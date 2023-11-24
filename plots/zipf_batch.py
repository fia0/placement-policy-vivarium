#!/bin/env python3
import sys
import matplotlib.pyplot as plt
import pandas as pd
import math

def plot_access_percentile(df: pd.DataFrame, pdf: pd.DataFrame, access, path):
    fig, ax = plt.subplots(1,1)
    for column in [f"{access}_avg", f"{access}_p90", f"{access}_p95", f"{access}_p99", f"{access}_max"]:
        ax.plot(df["now"], df[column] / 1_000_000, label=column, markersize=12)
    ax.set_title("Zipf Batch - Latencies over Iteration")
    ticks = ax.get_xticks()

    end_timestamp = df["now"].max()
    last_end = 0
    for index, row in df.iterrows():
        ax.axhspan(row[f"{access}_avg"] / 1_000_000, row[f"{access}_max"] / 1_000_000, xmin=last_end/end_timestamp,xmax=row["now"]/end_timestamp, alpha=0.5)
        last_end = row["now"] + row["interval"]

    for index, row in pdf.iterrows():
        ax.axvline(row["now"], color="red", alpha=0.3, linestyle="-")
        down, up = ax.get_ylim()
        ax.text(row["now"], up, "M", fontsize="xx-small", rotation=0)

    ax.set_xticks(ticks, [f"{int(lbl/60/60)}:{int(lbl/60%60)}:{int(lbl%60)}" for lbl in ticks])
    ax.set_xlabel("Time (h:m:s)")
    ax.set_ylabel("Latency in s")
    ax.set_xlim(0, end_timestamp)
    ax.set_yscale("log")
    ax.legend()
    fig.savefig(path)

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <PATH_TO_APP_CSV> <PATH_TO_POLICY_CSV>")
    sys.exit(1)

plt.rcParams["font.family"] = "Iosevka"
app_data = pd.read_csv(sys.argv[1])
policy_data = pd.read_csv(sys.argv[2])
plot_access_percentile(app_data, policy_data, "read", "zipf_batch_read.svg")
plot_access_percentile(app_data, policy_data, "write", "zipf_batch_write.svg")
