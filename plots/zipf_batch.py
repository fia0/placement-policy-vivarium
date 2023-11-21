#!/bin/env python3
import sys
import matplotlib.pyplot as plt
import pandas as pd
import math

def plot_access_percentile(df: pd.DataFrame, access, path):
    fig, ax = plt.subplots(1,1)
    for column in [f"{access}_avg", f"{access}_p90", f"{access}_p95", f"{access}_p99", f"{access}_max"]:
        ax.plot(df["now"], df[column] / 1_000_000, label=column, markersize=12)
    ax.set_title("Zipf Batch - Latencies over Iteration")
    ticks = ax.get_xticks()

    end_timestamp = df["now"].max()
    for index, row in df.iterrows():
        ax.axhspan(row[f"{access}_avg"] / 1_000_000, row[f"{access}_max"] / 1_000_000, xmin=row["now"]/end_timestamp,xmax=(row["now"]+300)/end_timestamp, alpha=0.5)
    ax.set_xticks(ticks, [f"{int(lbl/60/60)}:{int(lbl/60%60)}:{int(lbl%60)}" for lbl in ticks])
    ax.set_xlabel("Time (h:m:s)")
    ax.set_ylabel("Latency in s")
    ax.set_xlim(0, end_timestamp)
    ax.set_yscale("log")
    ax.legend()
    fig.savefig(path)

if len(sys.argv) < 2:
    print("Usage: zipf_batch.py <PATH_TO_CSV>")
    sys.exit(1)

foo = pd.read_csv(sys.argv[1])
plot_access_percentile(foo, "read", "zipf_batch_read.svg")
plot_access_percentile(foo, "write", "zipf_batch_write.svg")
