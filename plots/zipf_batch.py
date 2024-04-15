#!/bin/env python3
import sys
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import math


colors = [
    "#E69F00",
    "#56B4E9",
    "#009E73",
    "#D55E00",
    "#CC79A7",
    "#0072B2",
    "#F0E442",
    "#D55E00",
    "#000000",
]

def plot_access_percentile(df: pd.DataFrame, pdf: pd.DataFrame, access, path):
    fig, ax = plt.subplots(1,1)

    is_scatter = False
    if df["read_total"].median() < 20:
        is_scatter = True

    for color, column in zip(colors, [f"{access}_median", f"{access}_avg", f"{access}_p90", f"{access}_p95", f"{access}_p99", f"{access}_max"]):
        if is_scatter: 
            ax.scatter(df["now"], df[column] / 1_000_000, label=column, linewidths=0.5, color=color)
        else:
            ax.plot(df["now"], df[column] / 1_000_000, label=column, color=color)
        # trend = np.polyfit(df["now"].to_numpy(), df[column].to_numpy() / 1_000_000, 4)
        # ax.plot(df["now"], np.poly1d(trend)(df["now"].to_numpy()), linewidth=2, linestyle="dashed", alpha=0.8, color=color)
    ax.set_title("Zipf Batch - Latencies over Iteration")

    # # Averages trend line to approximate iteration I/O time
    # avgs = np.polyfit(df["now"].to_numpy(), df[f"{access}_avg"].to_numpy() / 1_000_000, 1)
    # ax.plot(df["now"], np.poly1d(avgs)(df["now"].to_numpy()), linewidth=2, linestyle="dashed", alpha=0.8)

    file_suffix = "svg"
    #if len(df.index) > 1000:
    #    # avoid painfully slow svg by switching to rasterized plots
    #    file_suffix = "png"

    end_timestamp = df["now"].max()
    last_end = 0
    for index, row in df.iterrows():
        ax.axhspan(
                row[f"{access}_median"] / 1_000_000,
                row[f"{access}_max"] / 1_000_000,
                xmin=last_end/end_timestamp,
                xmax=row["now"]/end_timestamp,
                alpha=0.5
            )
        last_end = row["now"] + row["interval"]

    for timestamp in pdf["now"].unique():
        ax.axvline(timestamp, color="black", linewidth=0.7, linestyle=":", alpha=0.8)
        down, up = ax.get_ylim()
        ax.text(timestamp, up, "M", fontsize="xx-small", rotation=0)

    ticks = ax.get_xticks()
    ax.set_xticks(ticks, [f"{int(lbl/60/60)}:{int(lbl/60%60)}:{int(lbl%60)}" for lbl in ticks])
    ax.set_xlabel("Time (h:m:s)")
    ax.set_ylabel("Latency in s")
    ax.set_xlim(0, end_timestamp)
    ax.set_yscale("log")
    ax.legend(bbox_to_anchor=(1.04,1))
    fig.savefig(f"{path}.{file_suffix}", bbox_inches="tight")

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <PATH_TO_APP_CSV> <PATH_TO_POLICY_CSV>")
    sys.exit(1)

plt.rcParams["font.family"] = "Iosevka"
app_data = pd.read_csv(sys.argv[1])
policy_data = pd.read_csv(sys.argv[2])
plot_access_percentile(app_data, policy_data, "read", "zipf_batch_read")
plot_access_percentile(app_data, policy_data, "write", "zipf_batch_write")
