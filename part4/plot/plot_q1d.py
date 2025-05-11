import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
import numpy as np
import glob

path = "../part4_task1_d_results/"


def get_p95_latencies(folder_path, run):
    file_list = glob.glob(os.path.join(folder_path + run, "mcperf_results*.txt"))

    if len(file_list) == 0:
        raise Exception("No txt file starting with mcperf_results* found.")

    result_path = file_list[0]

    header = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target ts_start ts_end"""
    column_names = header.split()

    latencies_df = pd.read_csv(result_path, sep=r'\s+', engine='python')
    latencies_df.columns = column_names

    result_df = latencies_df.copy()
    result_df = result_df.sort_values(by="QPS")

    return result_df[['p95', 'QPS', 'ts_start', 'ts_end']]


def get_cpu_usage(ts_start, ts_end):
    result_path = path + "cpu_usage"
    cpu_usage_df = pd.read_csv(result_path, header=None)
    cpu_usage_df.columns = ["time", "core1", "core2", "core3", "core4"]
    cpu_usage_df["time"] = cpu_usage_df["time"].astype(float) * 1000
    cpu_usage_df["core1"] = cpu_usage_df["core1"].str.replace('[', '', regex=False).astype(float)
    cpu_usage_df["core4"] = cpu_usage_df["core4"].str.replace(']', '', regex=False).astype(float)

    cpu_usage_df = cpu_usage_df[(cpu_usage_df["time"] >= ts_start) & (cpu_usage_df["time"] <= ts_end)]
    return cpu_usage_df


def transform_time_QPS(cpu_df, qps_df):
    qps_df = qps_df.dropna(subset=["ts_start"])
    cpu_times = cpu_df["time"].astype(float).values
    qps_times = qps_df["ts_start"].astype(float).values
    qps_values = qps_df["QPS"].values

    indices = np.abs(cpu_times[:, None] - qps_times).argmin(axis=1)

    cpu_df["QPS"] = qps_values[indices]

    cpu_df = cpu_df.sort_values(by="QPS")
    
    return cpu_df


def export_plot(C):
    
    p95_df = get_p95_latencies(path, f"T2_C{C}/run_1")

    x_axis = np.arange(0, 230000, 5000)

    fig, ax1 = plt.subplots(figsize=(12, 6))

    color = 'tab:red'
    ax1.set_xlabel('Achieved Queries per Second (QPS)')
    ax1.set_ylabel('95th Percentile Latency (µs)')
    
    data1_x_values = p95_df["QPS"]
    data1 = p95_df["p95"]

    ax1.scatter(data1_x_values, data1, color=color, marker='o', label='95th Percentile Latency (µs)', zorder=5)
    ax1.plot(data1_x_values, data1, color=color, linestyle='-', alpha=0.6)
    ax1.tick_params(axis='y')
    ax1.set_xticks(x_axis[::5])

    plt.axhline(y=800, color='black', linestyle='dotted')
    ax1.set_xlim(0, 230000)

    ax2 = ax1.twinx()  # instantiate a second Axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('CPU Utilization (%)')  # we already handled the x-label with ax1

    cpu_df = get_cpu_usage(p95_df["ts_start"].iloc[0], p95_df["ts_end"].iloc[-3])

    data2_x_values = transform_time_QPS(cpu_df, p95_df)["QPS"]
    data2 = cpu_df["core1"] if C == 1 else cpu_df["core1"] + cpu_df["core2"]
    ax2.scatter(data2_x_values, data2, color=color, marker='v', label=f'CPU Utilization (%)', zorder=5)
    ax2.plot(data2_x_values, data2, color=color, linestyle='-', alpha=0.6)
    ax2.tick_params(axis='y')
    ax2.set_ylim(0, 100 if C == 1 else 200)
    ax2.set_xlim(0, 230000)

    ax1.grid(True, linestyle='--', alpha=0.4)
    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1))
    ax2.legend(loc='upper left', bbox_to_anchor=(0, 0.95))

    plt.title("95th Percentile Memcached Latency vs. CPU Utilization for " + f"{C} {"Core" if C == 1 else "Cores"}")
    fig.tight_layout()  # otherwise the right y-label is slightly clipped

    os.makedirs("task1_d", exist_ok=True)
    file_path = os.path.join("task1_d", "p95_latency_and_cpu_utilization_for_" + f"{C}_{"core" if C == 1 else "cores"}" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()

def main():
    export_plot(C=1)
    export_plot(C=2)

if __name__ == "__main__":
    main()
