import subprocess, time, json
import os
from utils import run_command

# node-a-2core : e2-highmem-2
# node-b-2core : n2-highcpu-2
# node-c-4core : c3-highcpu-4
# node-d-4core : n2-standard-4

def modify_yaml_for_scheduling(
        benchmark,
        node_type,
        threads,
        cpuset,
        workdir
    ):
    """
    Reads the PARSEC job template for `benchmark`, applies scheduling parameters,
    and writes a modified YAML into `workdir`.
    
    Parameters
    ----------
    benchmark : str
        Name of the PARSEC benchmark (e.g., "radix").
    node_type : str
        Node label to schedule the job onto (e.g., "node-a-2core").
    threads : int
        Number of threads to request.
    cpuset : str
        CPU set to pin the job to (e.g., "0-3"). Set to "" to disable CPU
        pinning.
    workdir : str
        Directory to write the modified YAML file to.
    
    Returns
    -------
    str
        Path to the modified YAML file.
    """
    template_path = os.path.join(
        "./parsec-benchmarks",
        f"parsec-{benchmark}.yaml"
    )
    with open(template_path) as f:
        content = f.read()

    # Substitute placeholders in the YAML:
    content = content.replace("NODE_TYPE", node_type)
    content = content.replace("THREAD_COUNT", f"{threads}")
    if cpuset != "":
        content = content.replace("CPUSET_PREFIX", f"taskset -c {cpuset} ")
    else:
        content = content.replace("CPUSET_PREFIX", "")

    # Write modified YAML into workdir
    os.makedirs(workdir, exist_ok=True)
    out_path = os.path.join(
        workdir,
        f"parsec-{benchmark}-{node_type}-{threads}.yaml"
    )
    with open(out_path, "w") as f:
        f.write(content)
    return out_path

def launch_jobs(configs, workdir):
    """
    Launches multiple PARSEC jobs with specified scheduling parameters,
    respecting dependencies and delays.
    
    Parameters
    ----------
    configs : list of tuples
        Each tuple should contain 6 elements:
        - benchmark: str - Name of the benchmark to run
        - node_type: str - Target node type to schedule onto
        - threads: int - Number of threads to use
        - cpuset: str - CPU set to pin job to
        - delay: int - Seconds to wait before launching job (from function start)
        - dependencies: list[str] - Job names that must complete before launching
    workdir : str
        Directory where modified YAMLs will be written.
    
    Returns
    -------
    list of str
        List of job names launched (metadata.name from each YAML).
    """
    # Record function start time for absolute delays
    start_time = time.time()
    
    # Prepare the launch times file
    launch_times_path = os.path.join(workdir, "launch_times.txt")
    with open(launch_times_path, "w") as _:
        pass
    
    # Track all job names for return value
    all_job_names = []
    
    # Initialize pending_jobs as all jobs
    pending_jobs = []
    for bench, node_type, thr, cpu, delay, dependencies in configs:
        # Standardize job name to match Kubernetes naming convention
        job_name = f"parsec-{bench}"  
        all_job_names.append(job_name)
        
        standardized_dependencies = [f"parsec-{dep}" for dep in dependencies]
        
        pending_jobs.append({
            "bench": bench,  # Keep original benchmark name for logging/YAML creation
            "node_type": node_type,
            "threads": thr,
            "cpuset": cpu,
            "delay": delay,
            "delay_until": start_time + delay,
            "dependencies": standardized_dependencies, 
            "job_name": job_name  
        })
    
    # Track completed jobs
    completed_jobs = set()
    
    # For periodic status reporting
    last_status_time = time.time() - 30
    
    # Launch jobs that meet criteria (delay passed, dependencies satisfied)
    while pending_jobs:
        # Check and update completed jobs
        out = subprocess.check_output(["kubectl", "get", "jobs", "-o", "json"])
        data = json.loads(out)
        for item in data["items"]:
            job_name = item["metadata"]["name"]
            if job_name in all_job_names and item["status"].get("succeeded", 0) >= 1:
                completed_jobs.add(job_name)
        
        current_time = time.time()
        
        # Periodically print status update
        if current_time - last_status_time >= 10:
            print("[STATUS] launch_jobs: Current job status:")
            print(f"  - Completed jobs: {len(completed_jobs)}/{len(all_job_names)}")
            print(f"  - Pending jobs: {len(pending_jobs)}")
            
            # Report on jobs waiting for dependencies
            for job in pending_jobs:
                missing_deps = [dep for dep in job["dependencies"] if dep not in completed_jobs]
                if missing_deps:
                    print(f"  - Job {job['job_name']} is waiting for dependencies: {', '.join(missing_deps)}")
            
            run_command("kubectl get jobs", check=False)
            last_status_time = current_time
        
        # Try to launch jobs that meet criteria
        for job in pending_jobs[:]:  # Use a copy of the list for iteration
            # Check if delay time has passed
            delay_satisfied = current_time >= job["delay_until"]
            
            # Check if dependencies are satisfied
            deps_satisfied = all(dep in completed_jobs for dep in job["dependencies"])
            
            if delay_satisfied and deps_satisfied:
                # Launch the job
                yaml_path = modify_yaml_for_scheduling(
                    job["bench"],
                    job["node_type"],
                    job["threads"],
                    job["cpuset"],
                    workdir
                )
                run_command(f"kubectl create -f {yaml_path}", check=True)
                print(
                    f"[STATUS] launch_jobs: Launched {job['bench']} on {job['node_type']} " + 
                    f"with {job['threads']} threads"
                )
                
                # Record the launch timestamp in milliseconds
                launch_ms = int(time.time() * 1000)
                with open(launch_times_path, "a") as lt:
                    lt.write(f"Job:  {job['job_name']}\n")
                    lt.write(f"Start time:  {launch_ms}\n")
                
                # Mark as launched but keep in pending list until completion
                pending_jobs.remove(job)
        
        # Pause before next iteration
        if pending_jobs:
            time.sleep(2)
    
    print("[STATUS] launch_jobs: All jobs launched")
    return all_job_names

def wait_for_jobs(jobs, poll_interval=5, timeout=300):
    """
    Polls the Kubernetes API until all specified Jobs have completed
    successfully or a timeout is reached.
    
    This function:
    1) Periodically retrieves the list of Job objects in JSON format.
    2) Filters for the provided `jobs` whose `.status.succeeded` count is at
       least 1.
    3) Repeats the check every `poll_interval` seconds until all jobs are done
       or `timeout` seconds have passed.
    
    Parameters
    ----------
    jobs : list of str
        Names of the Kubernetes Job resources to wait for.
    poll_interval : int, optional
        Number of seconds to wait between successive polls (default is 5).
    timeout : int, optional
        Maximum number of seconds to wait before returning (default is 300).
        Set to 0 or None to disable the timeout.
    
    Returns
    -------
    None
    """
    print("[STATUS] wait_for_jobs: Waiting for jobs to complete...")
    start_time = time.time()
    last_status_time = time.time() - 30

    job_set = set(jobs) # Use a set for efficient lookup

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time

        # Check for timeout
        if timeout and elapsed_time > timeout:
            print(f"[WARNING] wait_for_jobs: Timeout of {timeout} seconds reached.")
            out = subprocess.check_output(["kubectl","get","jobs","-o","json"])
            data = json.loads(out)
            done_jobs = {j["metadata"]["name"] for j in data["items"] 
                         if j["metadata"]["name"] in job_set 
                         and j["status"].get("succeeded",0) >= 1}
            pending_jobs = job_set - done_jobs
            if pending_jobs:
                print(f"  - The following jobs did not complete: {', '.join(pending_jobs)}")
            else:
                print("  - All expected jobs completed just before timeout.")
            break

        # Check for job completion
        out = subprocess.check_output(["kubectl","get","jobs","-o","json"])
        data = json.loads(out)
        done_jobs = {j["metadata"]["name"] for j in data["items"] 
                     if j["metadata"]["name"] in job_set 
                     and j["status"].get("succeeded",0) >= 1}
        
        if len(done_jobs) == len(jobs):
            print("[STATUS] wait_for_jobs: All jobs completed successfully.")
            break

        # Periodically print job status every 30 seconds
        if current_time - last_status_time >= 30:
            pending_count = len(jobs) - len(done_jobs)
            print(f"[STATUS] wait_for_jobs: {len(done_jobs)}/{len(jobs)} jobs completed. Waiting for {pending_count} more...")
            run_command("kubectl get jobs", check = False)
            last_status_time = current_time

        time.sleep(poll_interval)

def collect_parsec_times(output_dir):
    """
    Gathers start and completion times for all pods and parses them into a
    summary file.
    
    This function:
    1) Executes `kubectl get pods -o json` and writes the full output to
       'pods.json'.
    2) Invokes the provided `get_time.py` script to parse timing information
       from 'pods.json'.
    3) Writes the parsed timing results to the specified `output_file`.
    
    Parameters
    ----------
    output_dir : str
        Path to the directory where pods.json and the parsed timing results will
        be saved.
    
    Returns
    -------
    None
    """
    json_file = os.path.join(output_dir, "pods.json")
    output_file = os.path.join(output_dir, "parsec_times.txt")

    # Make sure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    subprocess.run(
        ["kubectl","get","pods","-o","json"],
        stdout = open(json_file, "w")
    )
    subprocess.run(
        ["python3","../get_time.py",json_file],
        stdout = open(output_file, "w")
    )
    print(
        f"[STATUS] collect_parsec_times: Collected PARSEC times into " +
        f"{output_file}"
    )

def delete_all_parsec_jobs():
    """
    Deletes all PARSEC jobs from the Kubernetes cluster.
    
    This function:
    1) Executes `kubectl delete jobs --all` to remove all parsec jobs.
    2) Executes `kubectl delete pods --all` to remove all parsec pods.
    
    Returns
    -------
    None
    """
    print(
        "[STATUS] delete_all_parsec_jobs: Deleting all PARSEC jobs and pods..."
    )
    run_command("kubectl delete jobs -l app=parsec", check = True)
    run_command("kubectl delete pods -l app=parsec", check = True)