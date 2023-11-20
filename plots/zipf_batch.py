#!/bin/env python3
import sys
import matplotlib.pyplot as plt
import pandas as pd

if len(sys.argv) < 2:
    print("Usage: zipf_batch.py <PATH_TO_CSV>")
    sys.exit(1)

foo = pd.read_csv(sys.argv[1])
fig, ax = plt.subplots(1,1)
for column in ["write_avg", "write_max", "read_max", "read_avg", "read_p90", "read_p95", "read_p99"]:
    ax.plot(foo[column], label=column, markersize=12)

ax.set_title("Zipf Batch - Latencies over Iteration")
ax.set_xlabel("Iteration")
ax.set_ylabel("Latency in us")
ax.legend()
fig.savefig("zipf_batch.svg")
