import pandas as pd
import re
import argparse

## Path to the last experiment (hard-coded)
path = "../part4_task3_results/"

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
    task_times['duration'] = (task_times['end'] - task_times['start']) / 1000
    task_times['start'] = task_times['start'] / 1000
    task_times['end'] = task_times['end'] / 1000

    return task_times, task_times['start'].min(), task_times['end'].max()

def get_time(task, run_number):
    logger_df = get_logger_df(f"../part4_task{task}_results/" + run_number + '/logger_out')
    job_time, min, max = extract_start_end(logger_df)

    job_time.loc[-1] = ['Total', min, max, max - min]

    return job_time[['task', 'duration']]

def main(task):
    run_1 = get_time(task, "run_1")
    run_2 = get_time(task, "run_2")
    run_3 = get_time(task, "run_3")

    merged = pd.merge(run_1, run_2, left_on="task", right_on="task")
    duration_df = pd.merge(merged, run_3, left_on="task", right_on="task")

    duration_df.columns = ['task', 'duration_run_1', 'duration_run_2', 'duration_run_3']
    duration_columns = ['duration_run_1', 'duration_run_2', 'duration_run_3']
    duration_df['duration_mean'] = duration_df[duration_columns].mean(axis=1)
    duration_df['duration_std'] = duration_df[duration_columns].std(axis=1)

    print(duration_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process runs from a specified folder.")
    parser.add_argument("task", help="Numer of the Task containing run subdirectories")
    args = parser.parse_args()

    main(args.task)
