import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os
import numpy as np

path = "../results/"

def get_p95_latencies(folder_path, run_number):
    result_path = folder_path + run_number + "/mcperf_results_local.txt"

    header = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target"""
    column_names = header.split()

    latencies_df = pd.read_csv(result_path, sep=r'\s+', engine='python')
    latencies_df.columns = column_names

    result_df = latencies_df.copy()

    return result_df[['p95', 'QPS']]


def export_plot(folder, C):
    folder_path = path + folder + '/'
    
    p95_df = get_p95_latencies(folder_path, C + "_core")

    x_axis = np.arange(0, 230000, 5000)

    fig, ax1 = plt.subplots()

    color = 'tab:red'
    ax1.set_xlabel('Achieved Queries per Second (QPS)')
    ax1.set_ylabel('95th Percentile Latency (µs)', color=color)
    ax1.plot(x_axis, data1, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second Axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('CPU Utilization (%)', color=color)  # we already handled the x-label with ax1
    ax2.plot(x_axis, data2, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    plt.axhline(y=800, color='r', linestyle='.')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    
    plt.show()

    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, "p95_latency_and_cpu_utilization_for_" + C + "_core" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()

def main(folder):
    export_plot(folder, C=1)
    export_plot(folder, C=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
