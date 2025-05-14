import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os
import numpy as np
import glob
from io import StringIO
import re
from datetime import datetime

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

path = "../part4_task2_results/"

def get_logger_df(logger_path):
    patterns = {
        "start": re.compile(r"(?P<timestamp>[\d\-\:T\.]+)\s+start\s+(?P<task>\w+)\s+\[(?P<cores>[\d,]+)\]\s+(?P<threads>\d+)"),
        "end": re.compile(r"(?P<timestamp>[\d\-\:T\.]+)\s+end\s+(?P<task>\w+)"),
        "execution_time": re.compile(r"(?P<timestamp>[\d\-\:T\.]+)\s+custom\s+(?P<task>\w+)\s+execution_time_(?P<time>[\d\.]+)_seconds"),
        "update_cores": re.compile(r"(?P<timestamp>[\d\-\:T\.]+)\s+update_cores\s+(?P<task>\w+)\s+\[(?P<cores>[\d,]+)\]"),
        "custom": re.compile(r"(?P<timestamp>[\d\-\:T\.]+)\s+custom\s+(?P<task>\w+)\s+(?P<action>.+)"),
        "scheduler": re.compile(r"(?P<timestamp>[\d\-\:T\.]+)\s+start\s+scheduler")
    }
    
    data = []

    with open(logger_path, 'r') as file:
        for line in file:
            line = line.strip()
            matched = False

            for label, pattern in patterns.items():
                match = pattern.match(line)
                if match:
                    entry = match.groupdict()
                    entry["type"] = label  # keep track of which pattern matched
                    data.append(entry)
                    matched = True
                    break

            if not matched:
                print(f"Unmatched line: {line}")

    df = pd.DataFrame(data)

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Convert cores from string to list
    if 'cores' in df.columns:
        df['cores'] = df['cores'].apply(lambda x: list(map(int, x.split(','))) if isinstance(x, str) else x)

    # Convert threads and time to numeric if present
    for col in ['threads', 'time']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def get_mcperf_path(folder_path):
    file_list = glob.glob(os.path.join(folder_path, "mcperf_results*.txt"))

    if len(file_list) == 0:
        raise Exception("No txt file starting with mcperf_results* found.")

    mcperf_path = file_list[0]
    return mcperf_path

def get_p95_latencies(result_path):
    with open(result_path, 'r') as file:
        lines = file.readlines()

    # Start Time
    timestamp_start = None
    for line in lines:
        if line.strip().startswith("Timestamp start:"):
            timestamp_start = int(line.strip().split(":")[1].strip())
            break
    if timestamp_start is None:
        raise ValueError("Start timestamp not found.")

    # Interval Times
    interval_line = next((l for l in lines if "Total number of intervals" in l), None)
    if interval_line is None:
        raise ValueError("Interval line not found.")

    interval_str = re.search(r'\((.*?)\)', interval_line).group(1)
    intervals = list(map(int, interval_str.split(',')))
    if len(intervals) != 100:
        raise ValueError(f"Expected 100 intervals, found {len(intervals)}.")
    
    # Addition Times
    times = [timestamp_start]
    for interval in intervals[:-1]:  # skip last to keep 100 total values
        times.append(times[-1] + interval)

    
    read_lines = [line for line in lines if line.strip().startswith('read')]

    if len(read_lines) != 100:
        raise ValueError(f"Expected 100 'read' lines, found {len(read_lines)}.")

    column_names = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target""".split()

    data_str = ''.join(read_lines)
    result_df = pd.read_csv(StringIO(data_str), sep=r'\s+', names=column_names)
    result_df["time"] = times

    return result_df[['p95', 'QPS', 'time']]


def export_plot_A(p95_df, folder, run_number):
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Left Y-Axis: Latency
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("95th Percentile Latency (µs)")
    ax1.bar(
        p95_df['time'],
        p95_df['p95'],
        width=100000,
        align='edge',
        color='tab:red',
        label="Latency"
    )
    ax1.tick_params(axis='y')

    # Right Y-Axis: QPS
    ax2 = ax1.twinx()
    ax2.set_ylabel("Achieved QPS")
    ax2.scatter(p95_df["time"], p95_df["QPS"], color='tab:blue', s=30, label="Latency")
    ax2.plot(p95_df["time"], p95_df["QPS"], color='tab:blue', label="QPS")
    ax2.tick_params(axis='y')

    # Grid and layout
    ax1.grid(True, linestyle='--', alpha=0.5)
    fig.suptitle(f"{run_number.replace("run_", "")}A: 95th Percentile Latency vs QPS", fontsize=14)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])  # Room for title

    # Save plot
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, run_number.replace("run_", "") + "A" + ".png")
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
    file_path = os.path.join(folder, run_number.replace("run_", "") + "B" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()


def export_plots(folder, run_number):
    #folder_path = path + folder + '/' + run_number + '/'

    folder_path = path + folder + '/'

    mcperf_path = get_mcperf_path(folder_path)
    
    p95_df = get_p95_latencies(mcperf_path)
    print(p95_df)

    logger_df = get_logger_df(folder_path + 'logger_out')
    print(logger_df)

    export_plot_A(p95_df, folder, run_number)
    #export_plot_B(p95_df, folder, run_number)


def main(folder):
    export_plots(folder, "run_1")
    #export_plots(folder, "run_2")
    #export_plots(folder, "run_3")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
