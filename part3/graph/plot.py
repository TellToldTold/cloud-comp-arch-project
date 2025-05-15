import pandas as pd
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import argparse
import os

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

machines = {
    "blackscholes" : "nobe-b",
    "canneal" : "node-d",
    "dedup" : "node-b",
    "ferret" : "node-d",
    "freqmine" : "node-c",
    "radix" : "node-a",
    "vips" : "node-a",
}

## Path
path = "../results/"


## Get the launch time of each jobs
def get_launch_time(folder, run_number):
    with open(path + folder + '/' + run_number + "/launch_times.txt" , "r") as file:
        times = file.read()

    matches = re.findall(r"Job:\s+(\S+)\s+Start time:\s+(\d+)", times)

    times_df = pd.DataFrame(matches, columns=["job", "start_time"])
    times_df["start_time"] = times_df["start_time"].astype(int)
    times_df = times_df.sort_values(by='start_time').reset_index(drop=True)

    times_df['job'] = times_df['job'].apply(lambda x: x.replace("parsec-", ""))

    return times_df


# Get job duration in sec
def get_time_interval(folder, run_number):
    with open(path + folder + '/' + run_number + "/parsec_times.txt" , "r") as file:
        duration = file.read()
    
    jobs = re.findall(r"Job:\s+([^\n]+)", duration)
    durations = re.findall(r"Job time:\s+([0-9:]+)", duration)

    job_names = [j.replace("parsec-", "") for j in jobs if "parsec-" in j]

    # Convert durations to seconds
    def to_seconds(time_str):
        h, m, s = map(int, time_str.split(":"))
        return h * 3600 + m * 60 + s

    durations_sec = [to_seconds(d) for d in durations]
    durations_ms = [d * 1000 for d in durations_sec]

    df = pd.DataFrame({
        "job": job_names,
        "duration_ms": durations_ms
    })

    return df


## Make the graph
def export_graph(folder, run_number):
    # Export the columns p95, ts_start and ts_end
    result_path = path + folder + '/' + run_number + "/mcperf_results_local.txt"
    header = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target ts_start ts_end"""
    column_names = header.split()

    latencies_df = pd.read_csv(result_path, sep=r'\s+', engine='python')
    latencies_df.columns = column_names

    p95_latencies = latencies_df[['p95', 'ts_start', 'ts_end']].copy()
    p95_latencies['duration'] = p95_latencies['ts_end'] - p95_latencies['ts_start']

    # Assign each rows the job corresponding
    times_df = get_launch_time(folder, run_number)
    duration_df = get_time_interval(folder, run_number)

    # Scale ts_start and ts_end
    p95_latencies['ts_start'] = p95_latencies['ts_start'] - times_df.loc[0, 'start_time']
    p95_latencies['ts_end'] = p95_latencies['ts_end'] - times_df.loc[0, 'start_time']

    # Job interval
    job_interval_df = pd.merge(times_df, duration_df, left_on="job", right_on="job")
    job_interval_df["start_time"] = job_interval_df["start_time"] - times_df.loc[0, 'start_time']
    job_interval_df["end_time"] = job_interval_df["start_time"] + job_interval_df["duration_ms"]

    fig, (ax1, ax2) = plt.subplots(
        nrows=2,
        ncols=1,
        figsize=(12, 6),
        sharex=True,
        gridspec_kw={'height_ratios': [5, 2]}
    )

    # Bar plot for p95 latency
    ax1.bar(
        p95_latencies['ts_start'],
        p95_latencies['p95'],
        width=p95_latencies['duration'],
        align='edge',
        color='lightskyblue'
    )
    ax1.set_ylabel('95th Percentile Latency (µs)')
    ax1.set_title('95th Percentile Memcached Latency During PARSEC Benchmark Loads for ' + run_number)
    ax1.grid(True, which='both', linestyle='--', alpha=0.5)
    ax1.grid(which='minor', linestyle=':', alpha=0.3)
    ax1.minorticks_on()


    spacing = 2
    # Job duration horizontal bars
    for i, row in job_interval_df.iterrows():
        color = colors.get(row['job'], '#000000')
        ax2.hlines(
            y=i * spacing, xmin=row['start_time'], xmax=row['end_time'],
            color=color, linewidth=6
        )
        ax2.text(
            x=(row['start_time'] + row['end_time']) / 2,
            y= i * spacing + 0.3,
            s=row['job'] + ' (' + machines.get(row['job'], '') + ')',
            ha='center',
            va='bottom',
            fontsize=8,
            color=color
        )

    ax2.set_yticks([])
    ax2.set_yticklabels([])
    ax2.set_xlabel('Time (ms)')
    ax2.set_ylabel('Jobs')
    ax2.set_ylim(-1, spacing * len(job_interval_df))
    ax2.grid(True, axis='x', which='both', linestyle='--', alpha=0.5)
    ax2.minorticks_on()
    ax2.tick_params(axis='x', which='both', direction='out', top=True)

    plt.tight_layout()
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, "p95_latency_plot_" + run_number + ".png")
    plt.savefig(file_path, dpi=300)
    plt.close()


def main(folder):
    export_graph(folder, "run_1")
    export_graph(folder, "run_2")
    export_graph(folder, "run_3")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
