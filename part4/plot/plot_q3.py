import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os
import numpy as np

## Colors (from main.tex)
colors = {
    "blackscholes" : "#CCA000",
    "canneal" : "#CCCCCA",
    "dedup" : "#CCACCA",
    "ferret" : "#AACCCA",
    "freqmine" : "#0CCA00",
    "radix" : "#00CCA0",
    "vips" : "#CC0A00",
}

path = "../results/"

def get_p95_latencies(folder_path, run_number):
    result_path = folder_path + run_number + "/mcperf_results_local.txt"

    header = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target ts_start ts_end"""
    column_names = header.split()

    latencies_df = pd.read_csv(result_path, sep=r'\s+', engine='python')
    latencies_df.columns = column_names

    result_df = latencies_df.copy()

    return result_df[['p95', 'QPS']]


def export_plot_A(p95_df, folder, run_number):
    x_axis = np.arange(0, 230000, 5000)

    fig, ax1 = plt.subplots()

    color = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel('95th Percentile Latency (µs)', color=color)
    ax1.plot(x_axis, data1, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second Axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('Achieved Queries per Second (QPS)', color=color)  # we already handled the x-label with ax1
    ax2.plot(x_axis, data2, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    
    plt.show()

    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, run_number + "A" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()


def export_plot_B(p95_df, folder, run_number):

    x_axis = np.arange(0, 230000, 5000)

    fig, ax1 = plt.subplots()

    color = 'tab:red'
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Number of CPU cores allocated to memcached', color=color)
    ax1.plot(x_axis, data1, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second Axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('Achieved Queries per Second (QPS)', color=color)  # we already handled the x-label with ax1
    ax2.plot(x_axis, data2, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    
    plt.show()

    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, run_number + "B" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()


def export_plots(folder, run_number):
    folder_path = path + folder + '/' + run_number
    
    p95_df = get_p95_latencies(folder_path)

    export_plot_A(p95_df, folder, run_number)
    export_plot_B(p95_df, folder, run_number)


def main(folder):
    export_plots(folder, "run_1")
    export_plots(folder, "run_2")
    export_plots(folder, "run_3")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
