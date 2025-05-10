import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import glob
import os

path = "../part4_task1_results/"

colors = ["red", "blue", "green", "orange"]

def get_p95_latencies(folder_path, run):
    print(folder_path + run)
    file_list = glob.glob(os.path.join(folder_path + run, "mcperf_results*.txt"))

    if len(file_list) == 0:
        raise Exception("No txt file starting with mcperf_results* found.")

    result_path = file_list[0]
        
    header = """type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target ts_start ts_end"""
    column_names = header.split()

    latencies_df = pd.read_csv(result_path, sep=r'\s+', engine='python')
    latencies_df.columns = column_names

    result_df = latencies_df.copy()

    return result_df[['p95', 'QPS']]


def export_plot():
    folder_path = path

    print(path)

    plt.figure(figsize=(10,5))

    for T in range(1,3):
        for C in range(1,3):
    
            p95_run1 = get_p95_latencies(folder_path, f"T{T}_C{C}/run_1")
            p95_run2 = get_p95_latencies(folder_path, f"T{T}_C{C}/run_2")
            p95_run3 = get_p95_latencies(folder_path, f"T{T}_C{C}/run_3")

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
                    fmt='-o', capsize=5, label=f"T={T}, C={C}", color=colors[2*(T-1) + C - 1])

    # Labels and grid
    plt.xlabel("Achieved Queries per Second (QPS)")
    plt.ylabel("95th Percentile Latency (µs)")
    plt.title("95th Percentile Memcached Latency (average on 3 runs)")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()

    os.makedirs("task1", exist_ok=True)
    file_path = os.path.join("task1", "p95_latency_plot" + ".png")
    plt.savefig(file_path, dpi=300)

    plt.close()

def main():
    export_plot()

if __name__ == "__main__":
    main()
