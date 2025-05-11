import pandas as pd
import re
import argparse

## Path to the last experiment (hard-coded)
path = "../results/"

def get_time(folder, run_number):
    with open(path + folder + '/' + run_number + "/parsec_times.txt" , "r") as file:
        duration = file.read()
    
    # Find job names and their durations
    jobs = re.findall(r"Job:\s+([^\n]+)", duration)
    durations = re.findall(r"Job time:\s+([0-9:]+)", duration)

    total_time = re.findall(r"Total time:\s+([0-9:]+)", duration)
    durations.append(total_time[0])

    # Remove "memcached" or any non-parsec job if needed
    job_names = [j.replace("parsec-", "") for j in jobs if "parsec-" in j]

    # Convert durations to seconds
    def to_seconds(time_str):
        h, m, s = map(int, time_str.split(":"))
        return h * 3600 + m * 60 + s

    durations_sec = [to_seconds(d) for d in durations]

    # Create DataFrame
    df = pd.DataFrame({
        "job": job_names + ["Total"],
        "duration_sec_" + run_number: durations_sec
    })

    return df

def main(folder):
    run_1 = get_time(folder, "run_1")
    run_2 = get_time(folder, "run_2")
    run_3 = get_time(folder, "run_3")

    merged = pd.merge(run_1, run_2, left_on="job", right_on="job")
    duration_df = pd.merge(merged, run_3, left_on="job", right_on="job")

    duration_columns = ['duration_sec_run_1', 'duration_sec_run_2', 'duration_sec_run_3']
    
    duration_df['duration_mean'] = duration_df[duration_columns].mean(axis=1)
    duration_df['duration_std'] = duration_df[duration_columns].std(axis=1)

    print(duration_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("folder", help="Folder containing run subdirectories")
    args = parser.parse_args()

    main(args.folder)
