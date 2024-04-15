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

marker = ['.', ',', 'o', 'v', '^', '<', '>', '1', '2', '3', '4', '8', 's', 'P', 'h', '+', 'x']

def plot_disk_movement(df: pd.DataFrame, path):
    fig, ax = plt.subplots(1,1)

    disk_from_to = df[["from", "to"]].drop_duplicates()
    for m, row in zip(marker, disk_from_to.iterrows()):
        fro = row[1]["from"]
        to  = row[1]["to"]
        relevant = df.where(df["from"] == fro).where(df["to"] == to).dropna()
        ax.plot(relevant["now"], relevant["size"], label=f"{fro} to {to}", marker=m)

    end_timestamp = df["now"].max()
    ticks = ax.get_xticks()
    ax.set_xticks(ticks, [f"{int(lbl/60/60)}:{int(lbl/60%60)}:{int(lbl%60)}" for lbl in ticks])
    ax.set_xlabel("Time (h:m:s)")
    ax.set_ylabel("Number of blocks moved")
    ax.set_xlim(0, end_timestamp)
    ax.legend(bbox_to_anchor=(1.04,1))
    fig.savefig(f"{path}.svg", bbox_inches="tight")

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <PATH_TO_POLICY_CSV>")
    sys.exit(1)

plt.rcParams["font.family"] = "Iosevka"
policy_data = pd.read_csv(sys.argv[1])
plot_disk_movement(policy_data, "policy_movement")
