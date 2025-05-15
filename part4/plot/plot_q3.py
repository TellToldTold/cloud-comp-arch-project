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

def extract_start_end(logger_df):
    filtered_df = logger_df[logger_df['type'].isin(['start', 'end']) & (logger_df['task'] != 'memcached')]
    filtered_df = filtered_df.sort_values(by="timestamp_ms")

    # Separate start and end events
    starts = filtered_df[filtered_df["type"] == "start"][["task", "timestamp_ms"]].rename(columns={"timestamp_ms": "start"})
    ends = filtered_df[filtered_df["type"] == "end"][["task", "timestamp_ms"]].rename(columns={"timestamp_ms": "end"})

    # Merge on task
    task_times = pd.merge(starts, ends, on="task")
    task_times['duration'] = (task_times['end'] - task_times['start']) / 1000
    start_time = task_times.loc[0, 'start']
    task_times['start'] = (task_times['start'] - start_time) / 1000
    task_times['end'] = (task_times['end'] - start_time) / 1000

    filtered_df = logger_df[(logger_df['task'] == 'memcached') & (logger_df['type'].isin(['start', 'update_cores']))]
    filtered_df = filtered_df.sort_values(by="timestamp_ms")
    filtered_df['cores'] = filtered_df['cores'].apply(lambda x: 3 - len(x))
    filtered_df = filtered_df[['cores', 'timestamp_ms']]
    filtered_df['duration'] = filtered_df['timestamp_ms'].shift(-1) - filtered_df['timestamp_ms']
    filtered_df['duration'] = filtered_df['duration'] / 1000
    filtered_df['timestamp_ms'] = (filtered_df['timestamp_ms'] - start_time) / 1000
    return task_times, filtered_df


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


def export_plot_A(p95_df, cpu_df, job_df, folder, run_number, include_cpu=False):
    fig, (ax1, ax2) = plt.subplots(nrows=2,
        ncols=1,
        figsize=(12, 6),
        sharex=True,
        gridspec_kw={'height_ratios': [5, 2]})

    # Calculate bar positions
    bar_width = 3.5  # Width of each bar
    spacing = 3.0    # Space between pairs of bars
    
    # Create positions for each pair of bars
    positions = np.arange(len(p95_df)) * (2 * bar_width + spacing)
    
    # Define nicer colors
    latency_color = '#c9184a'  # Light red
    qps_color = '#38a3a5'      # Light blue
    
    # Left Y-Axis: Latency
    ax1.set_xlabel('')
    ax1.set_ylabel("95th Percentile Latency (µs)")
    latency_bars = ax1.bar(
        positions,  # Left bar position
        p95_df['p95'],
        width=bar_width,
        color=latency_color,
        alpha=0.8,
        label="Latency",
        zorder=2
    )
    ax1.tick_params(axis='y')
    
    # Adjust x-ticks to show time in seconds
    # Map bar positions to actual time values
    time_labels = p95_df['start_time'].values
    # Use a subset of positions for readability
    tick_indices = np.linspace(0, len(positions)-1, 10, dtype=int)
    ax1.set_xticks(positions[tick_indices])
    ax1.set_xticklabels([f"{time_labels[i]:.0f}" for i in tick_indices])

    # Right Y-Axis: QPS
    ax1_2 = ax1.twinx()
    ax1_2.set_ylabel("Achieved QPS")
    qps_bars = ax1_2.bar(
        positions + bar_width,  # Right bar position
        p95_df['QPS'],
        width=bar_width,
        color=qps_color,
        label="QPS",
        zorder=1 
    )
    ax1_2.tick_params(axis='y')
    
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
        ax3.plot(
            cpu_df['relative_time'], 
            cpu_df['cpu_usage'], 
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
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_2.get_legend_handles_labels()
    lines3, labels3 = [], []
    if ax3:
        lines3, labels3 = ax3.get_legend_handles_labels()
    
    ax1.legend(lines1 + lines2 + lines3, labels1 + labels2 + labels3, loc='upper left', bbox_to_anchor=(0, 1))

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

def export_plot_B(p95_df, cpu_df, job_df, memcached_df, folder, run_number, include_cpu):

    fig, (ax1, ax2) = plt.subplots(nrows=2,
        ncols=1,
        figsize=(12, 6),
        sharex=True,
        gridspec_kw={'height_ratios': [5, 2]})

    # Calculate bar positions
    bar_width = 3.5  # Width of each bar
    spacing = 3.0    # Space between pairs of bars
    
    # Create positions for each pair of bars
    positions = np.arange(len(p95_df)) * (2 * bar_width + spacing)
    
    # Define nicer colors
    latency_color = '#c9184a'  # Light red
    qps_color = '#38a3a5'      # Light blue
    
    # Left Y-Axis: Latency
    ax1.set_xlabel('')
    ax1.set_ylabel("Number of Cores allocated to memcached")
    latency_bars = ax1.step(
        memcached_df['timestamp_ms'],  # Left bar position
        memcached_df['cores'],
        color=latency_color,
        alpha=0.8,
        label="Memached cores",
        zorder=2
    )
    ax1.tick_params(axis='y')
    
    # Adjust x-ticks to show time in seconds
    # Map bar positions to actual time values
    time_labels = p95_df['start_time'].values
    # Use a subset of positions for readability
    tick_indices = np.linspace(0, len(positions)-1, 10, dtype=int)
    ax1.set_xticks(positions[tick_indices])
    ax1.set_xticklabels([f"{time_labels[i]:.0f}" for i in tick_indices])
    ax1.set_yticks([0, 1, 2])
    ax1.set_yticklabels(['0', '1', '2'])

    # Right Y-Axis: QPS
    ax1_2 = ax1.twinx()
    ax1_2.set_ylabel("Achieved QPS")
    qps_bars = ax1_2.bar(
        positions + bar_width,  # Right bar position
        p95_df['QPS'],
        width=bar_width,
        color=qps_color,
        label="QPS",
        zorder=1 
    )
    ax1_2.tick_params(axis='y')
    
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
        ax3.plot(
            cpu_df['relative_time'], 
            cpu_df['cpu_usage'], 
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
    fig.suptitle(f"{run_number.replace('run_', '')}B: Number of Memcached Cores vs QPS{title_suffix}", fontsize=14)
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])  # Room for title

    # Create combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_2.get_legend_handles_labels()
    lines3, labels3 = [], []
    if ax3:
        lines3, labels3 = ax3.get_legend_handles_labels()
    
    ax1.legend(lines1 + lines2 + lines3, labels1 + labels2 + labels3, loc='upper left', bbox_to_anchor=(0, 1))

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
    file_path = os.path.join(folder, run_number.replace("run_", "") + "B" + ".png")
    plt.savefig(file_path, dpi=300)
    plt.close()


def export_plots(task, run_number, include_cpu=False):
    folder_path = f"../part4_task{task}_results/" + run_number + '/'

    mcperf_path = get_mcperf_path(folder_path)
    
    p95_df = get_p95_latencies(mcperf_path)

    logger_df = get_logger_df(folder_path + 'logger_out')
    
    # Get CPU usage data from scheduler_out if needed
    cpu_df = pd.DataFrame()
    if include_cpu:
        cpu_df = get_cpu_usage_df(folder_path + 'scheduler_out')
        print(cpu_df)

    job_df, memcached_df = extract_start_end(logger_df)

    export_plot_A(p95_df, cpu_df, job_df, folder_path, run_number, include_cpu)
    export_plot_B(p95_df, cpu_df, job_df, memcached_df, folder_path, run_number, include_cpu)


def main(task, include_cpu=False):
    export_plots(task, "run_1", include_cpu)
    export_plots(task, "run_2")
    export_plots(task, "run_3")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("task", help="Number of the task containing the run subdirectories")
    parser.add_argument("--cpu", action="store_true", help="Include CPU usage data from the plot")
    args = parser.parse_args()
    
    main(args.task, args.cpu)
