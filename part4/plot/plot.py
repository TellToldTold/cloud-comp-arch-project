import matplotlib as plt
import pandas as pd
import argparse

path = "../results/"

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
    
    p95_run1 = get_p95_latencies(folder_path, "run_1")

    print(p95_run1)


def main(folder):
    export_plot(folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
