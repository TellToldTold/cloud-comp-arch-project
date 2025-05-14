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

    df["timestamp_ms"] = pd.to_datetime(df["timestamp"]).astype("int64") // 10**6
    return df

def extract_start_end(logger_df):
    filtered_df = logger_df[logger_df['type'].isin(['start', 'end']) & (logger_df['task'] != 'memcached')]
    filtered_df = filtered_df.sort_values(by="timestamp_ms")

    # Separate start and end events
    starts = filtered_df[filtered_df["type"] == "start"][["task", "timestamp_ms"]].rename(columns={"timestamp_ms": "start"})
    ends = filtered_df[filtered_df["type"] == "end"][["task", "timestamp_ms"]].rename(columns={"timestamp_ms": "end"})

    # Merge on task
    task_times = pd.merge(starts, ends, on="task")
    task_times['duration'] = task_times['end'] - task_times['start']
    start_time = task_times.loc[0, 'start']
    task_times['start'] = task_times['start'] - start_time
    task_times['end'] = task_times['end'] - start_time
    return task_times

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
    if len(intervals) != 78:
        raise ValueError(f"Expected 78 intervals, found {len(intervals)}.")
    
    # Addition Times
    times = [timestamp_start]
    for _ in intervals[:-1]:  # skip last to keep 78 total values
        times.append(times[-1] + 100000)

    
    read_lines = [line for line in lines if line.strip().startswith('read')]

    if len(read_lines) != 78:
        raise ValueError(f"Expected 78 'read' lines, found {len(read_lines)}.")

    column_names = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target""".split()

    data_str = ''.join(read_lines)
    result_df = pd.read_csv(StringIO(data_str), sep=r'\s+', names=column_names)
    result_df["start_time"] = times
    result_df["start_time"] = result_df["start_time"] - timestamp_start

    return result_df[['p95', 'QPS', 'start_time']]


def export_plot_A(p95_df, job_df, folder, run_number):
    fig, (ax1, ax2) = plt.subplots(nrows=2,
        ncols=1,
        figsize=(12, 6),
        sharex=True,
        gridspec_kw={'height_ratios': [5, 2]})

    x = p95_df['start_time']

    # Left Y-Axis: Latency
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("95th Percentile Latency (µs)")
    ax1.bar(
        x,
        p95_df['p95'],
        width=90000,
        align='edge',
        color='tab:red',
        label="Latency",
        alpha=0.8,
        zorder=2
    )
    ax1.tick_params(axis='y')

    # Right Y-Axis: QPS
    ax1_2 = ax1.twinx()
    ax1_2.set_ylabel("Achieved QPS")
    ax1_2.bar(
        x,
        p95_df['QPS'],
        width=90000,
        align='edge',
        color='tab:blue',
        label="QPS",
        alpha=0.6,
        zorder=1 
    )
    ax1_2.tick_params(axis='y')

    # Grid and layout
    ax1.grid(True, linestyle='--', alpha=0.5)
    fig.suptitle(f"{run_number.replace("run_", "")}A: 95th Percentile Latency vs QPS", fontsize=14)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])  # Room for title

    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1))
    ax1_2.legend(loc='upper left', bbox_to_anchor=(0, 0.95))

    spacing = 2
    # Job duration horizontal bars
    for i, row in job_df.iterrows():
        color = colors.get(row['task'], '#000000')
        ax2.hlines(
            y=i * spacing, xmin=row['start'], xmax=row['end'],
            color=color, linewidth=6
        )
        ax2.text(
            x=(row['start'] + row['end']) / 2,
            y= i * spacing + 0.3,
            s=row['task'],
            ha='center',
            va='bottom',
            fontsize=8,
            color=color
        )

    ax2.set_yticks([])
    ax2.set_yticklabels([])
    ax2.set_xlabel('Time (ms)')
    ax2.set_ylabel('Jobs')
    ax2.set_ylim(-1, spacing * len(job_df))
    ax2.grid(True, axis='x', which='both', linestyle='--', alpha=0.5)
    ax2.minorticks_on()
    ax2.tick_params(axis='x', which='both', direction='out', top=True)

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

    job_df = extract_start_end(logger_df)
    print(job_df)

    export_plot_A(p95_df, job_df, folder, run_number)
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
