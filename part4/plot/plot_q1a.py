import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os

path = "../results/"

colors = ["red", "blue", "green", "orange"]

def get_p95_latencies(folder_path, run_number):
    result_path = folder_path + run_number + "/mcperf_results_local.txt"

    header = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target ts_start ts_end"""
    column_names = header.split()

    latencies_df = pd.read_csv(result_path, sep=r'\s+', engine='python')
    latencies_df.columns = column_names

    result_df = latencies_df.copy()

    return result_df[['p95', 'QPS']]


def export_plot(folder):
    folder_path = path + folder + '/'

    plt.figure(figsize=(10,5))

    for T in range(1,3):
        for C in range(1,3):
    
            p95_run1 = get_p95_latencies(folder_path, f"{T}_{C}_run_1")
            p95_run2 = get_p95_latencies(folder_path, f"{T}_{C}_run_2")
            p95_run3 = get_p95_latencies(folder_path, f"{T}_{C}_run_3")

            combined = pd.concat([p95_run1, p95_run2, p95_run3], axis=1)

            combined.columns = [
                'p95_1', 'QPS_1',
                'p95_2', 'QPS_2',
                'p95_3', 'QPS_3'
            ]

            combined['p95_mean'] = combined[['p95_1', 'p95_2', 'p95_3']].mean(axis=1)
            combined['p95_std'] = combined[['p95_1', 'p95_2', 'p95_3']].std(axis=1)

            combined['QPS_mean'] = combined[['QPS_1', 'QPS_2', 'QPS_3']].mean(axis=1)
            combined['QPS_std'] = combined[['QPS_1', 'QPS_2', 'QPS_3']].std(axis=1)

            plt.errorbar(combined['QPS_mean'], combined['p95_mean'], xerr=combined['QPS_std'], yerr=combined['p95_std'], 
                    fmt='-o', capsize=5, label=f"T={T}, C={C}", color=colors[T+C-1])

    # Labels and grid
    plt.xlabel("Achieved Queries per Second (QPS)")
    plt.ylabel("95th Percentile Latency (µs)")
    plt.title("95th Percentile Memcached Latency (average on 3 runs)")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()

    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, "p95_latency_plot" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()

def main(folder):
    export_plot(folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
