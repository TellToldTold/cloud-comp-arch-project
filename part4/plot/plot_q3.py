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

def get_cpu_usage_df(scheduler_path):
    # Regular expression to extract CPU usage from scheduler_out
    pattern = re.compile(r'\[(?P<datetime>[\d\-\:\ ]+)\s+\|\s+(?P<timestamp>[\d\.]+)\]\s+Core\s+0\s+usage:\s+(?P<usage>[\d\.]+)%')
    
    data = []
    with open(scheduler_path, 'r') as file:
        for line in file:
            match = pattern.search(line)
            if match:
                timestamp_str = match.group('timestamp')
                usage = float(match.group('usage'))
                unix_time = float(timestamp_str)
                data.append({'unix_timestamp': unix_time, 'cpu_usage': usage})
    
    return pd.DataFrame(data)

def get_mcperf_path(folder_path):
    file_list = glob.glob(os.path.join(folder_path, "mcperf_results*.txt"))

    if len(file_list) == 0:
        raise Exception("No txt file starting with mcperf_results* found.")

    mcperf_path = file_list[0]
    return mcperf_path

def get_p95_latencies(result_path):
    with open(result_path, 'r') as file:
        lines = file.readlines()

    # Start and End Time
    timestamp_start = None
    timestamp_end = None
    for line in lines:
        if line.strip().startswith("Timestamp start:"):
            timestamp_start = int(line.strip().split(":")[1].strip())
        elif line.strip().startswith("Timestamp end:"):
            timestamp_end = int(line.strip().split(":")[1].strip())
        if timestamp_start is not None and timestamp_end is not None:
            break
            
    if timestamp_start is None or timestamp_end is None:
        raise ValueError("Start or end timestamp not found.")

    # Calculate total duration and interval size
    total_duration_ms = timestamp_end - timestamp_start
    num_intervals = 78  # As specified in the mcperf log
    interval_ms = total_duration_ms / num_intervals
    
    # Generate evenly spaced timestamps (in milliseconds)
    timestamps_ms = [timestamp_start + i * interval_ms for i in range(num_intervals)]
    
    # Convert to seconds for unix_timestamp (same scale as CPU usage data)
    timestamps_s = [t / 1000.0 for t in timestamps_ms]
    
    # Read the performance data
    read_lines = [line for line in lines if line.strip().startswith('read')]

    if len(read_lines) != num_intervals:
        raise ValueError(f"Expected {num_intervals} 'read' lines, found {len(read_lines)}.")

    column_names = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target""".split()

    data_str = ''.join(read_lines)
    result_df = pd.read_csv(StringIO(data_str), sep=r'\s+', names=column_names)
    
    # Add timestamps to the dataframe
    result_df["unix_timestamp"] = timestamps_s  # Use seconds scale (e+9) to match CPU data
    result_df["start_time"] = [(t - timestamp_start) / 1000.0 for t in timestamps_ms]  # Relative time in seconds

    return result_df[['p95', 'QPS', 'start_time', 'unix_timestamp']]


def export_plot_A(p95_df, cpu_df, folder, run_number, include_cpu=False):
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # X-axis: Time in seconds
    x = p95_df['start_time']
    
    # Left Y-Axis: Latency
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("95th Percentile Latency (µs)", color='tab:red')
    latency_line = ax1.plot(
        x,
        p95_df['p95'],
        'o-',  # Circle markers with lines
        color='tab:red',
        label="Latency",
        alpha=0.8,
        linewidth=1.5,
        markersize=4,
        zorder=2
    )
    ax1.tick_params(axis='y', labelcolor='tab:red')
    
    # Set reasonable y-limits for latency
    latency_max = p95_df['p95'].max() * 1.1  # 10% headroom
    ax1.set_ylim(0, latency_max)

    # Right Y-Axis: QPS
    ax2 = ax1.twinx()
    ax2.set_ylabel("Achieved QPS", color='tab:blue')
    qps_line = ax2.plot(
        x,
        p95_df['QPS'],
        's-',  # Square markers with lines
        color='tab:blue',
        label="QPS",
        alpha=0.8,
        linewidth=1.5,
        markersize=4,
        zorder=1 
    )
    ax2.tick_params(axis='y', labelcolor='tab:blue')
    
    # Set reasonable y-limits for QPS
    qps_max = p95_df['QPS'].max() * 1.1  # 10% headroom
    ax2.set_ylim(0, qps_max)
    
    # Third Y-Axis: CPU Usage (conditional)
    ax3 = None
    if include_cpu and not cpu_df.empty:
        ax3 = ax1.twinx()
        ax3.spines["right"].set_position(("axes", 1.1))  # Offset the right spine of ax3
        ax3.set_ylabel("Core 0 CPU Usage (%)", color='tab:green')
        
        # Debug timestamp ranges
        print(f"P95 timestamp range: {p95_df['unix_timestamp'].min()} to {p95_df['unix_timestamp'].max()}")
        print(f"CPU timestamp range: {cpu_df['unix_timestamp'].min()} to {cpu_df['unix_timestamp'].max()}")
        
        # Convert CPU timestamps to relative time for plotting
        # This aligns them with the p95 data's start_time
        p95_start_time = p95_df['unix_timestamp'].min()
        cpu_df['relative_time'] = cpu_df['unix_timestamp'] - p95_start_time
        
        print(f"Total CPU data points: {len(cpu_df)}")
        
        # Plot all CPU data points directly with semi-transparency
        cpu_line = ax3.plot(
            cpu_df['relative_time'], 
            cpu_df['cpu_usage'], 
            '-',  # Line only, no markers due to high density
            color='tab:green', 
            linewidth=1.0,  # Thinner line
            alpha=0.4,      # Semi-transparent
            label="CPU Usage", 
            zorder=3
        )
        ax3.tick_params(axis='y', labelcolor='tab:green')
        ax3.set_ylim(0, 105)  # 0-105% for CPU usage

    # Grid and layout
    ax1.grid(True, linestyle='--', alpha=0.5)
    
    # Set title based on whether CPU is included
    title_suffix = " vs CPU Usage" if include_cpu else ""
    fig.suptitle(f"{run_number.replace('run_', '')}A: 95th Percentile Latency vs QPS{title_suffix}", fontsize=14)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])  # Room for title

    # Create combined legend
    lines = latency_line + qps_line
    labels = ["Latency", "QPS"]
    
    if ax3:
        lines += cpu_line
        labels.append("CPU Usage")
    
    fig.legend(lines, labels, loc='upper right', bbox_to_anchor=(0.99, 0.99), framealpha=0.9)

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


def export_plots(folder, run_number, include_cpu=False):
    folder_path = path + folder + '/'

    mcperf_path = get_mcperf_path(folder_path)
    
    p95_df = get_p95_latencies(mcperf_path)
    print(p95_df)

    logger_df = get_logger_df(folder_path + 'logger_out')
    print(logger_df)
    
    # Get CPU usage data from scheduler_out if needed
    cpu_df = pd.DataFrame()
    if include_cpu:
        cpu_df = get_cpu_usage_df(folder_path + 'scheduler_out')
        print(cpu_df)

    export_plot_A(p95_df, cpu_df, folder, run_number, include_cpu)
    #export_plot_B(p95_df, folder, run_number)


def main(folder, include_cpu=False):
    export_plots(folder, "run_1", include_cpu)
    #export_plots(folder, "run_2")
    #export_plots(folder, "run_3")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    parser.add_argument("--cpu", action="store_true", help="Include CPU usage data from the plot")
    args = parser.parse_args()
    
    main(args.folder, args.cpu)
