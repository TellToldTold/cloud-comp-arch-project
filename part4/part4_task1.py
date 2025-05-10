#!/usr/bin/env python3

import os
import time
from datetime import datetime
from cluster_manager import setup_cluster, deploy_memcached, get_node_name, get_memcached_ip
from mcperf_manager import setup_mcperf_agents, preload, start_load_agent, run_mcperf_load, stop_mcperf_agents, restart_mcperf_agent

def main():
    """
    Script for Part 4 Task 1:
    Runs benchmarks for memcached with different thread and core configurations.
    For each configuration, performs 3 runs and saves the results.
    """
    # Define cluster parameters
    state_store = "gs://cca-eth-2025-group-092-fbaldin/"
    cluster_config_yaml = "part4.yaml"
    ssh_key_path = os.path.expanduser("~/.ssh/cloud-computing")
    
    # Define memcached configurations to test
    # Format: [(thread_count, core_count, core_list), ...]
    memcached_configs = [
        (1, 1, "0"),       # T=1, C=1 (core 0)
        (1, 2, "0,1"),     # T=1, C=2 (cores 0,1)
        (2, 1, "0"),       # T=2, C=1 (core 0)
        (2, 2, "0,1"),     # T=2, C=2 (cores 0,1)
    ]
    
    # Number of runs per configuration
    runs_per_config = 3
    
    # Create output directory structure
    base_output_dir = "part4_task1_results"
    os.makedirs(base_output_dir, exist_ok=True)
    
    print("Setting up and running memcached benchmarks...")
    
    # Setup the cluster (uncomment if not already set up)
    # setup_cluster(state_store, cluster_config_yaml)
    
    # Get memcached server IP (deploy first if needed)
    memcached_ip = get_memcached_ip(ssh_key_path)
    if not memcached_ip:
        print("Memcached IP not found. Deploying memcached...")
        memcached_ip = deploy_memcached(thread_count=1, memory_limit=1024, cpuset="0")
        if not memcached_ip:
            print("Failed to deploy memcached. Exiting.")
            return
    
    print(f"Using memcached at IP: {memcached_ip}")
    
    # Setup mcperf agents initially
    print("Setting up mcperf agents...")
    clients_info = setup_mcperf_agents()
    if not clients_info:
        print("Failed to setup mcperf agents. Exiting.")
        return
    
    # Preload memcached with data once
    print("Preloading memcached with data...")
    preload(clients_info, memcached_ip)
    
    # Run benchmarks for each configuration with multiple runs per config
    for config in memcached_configs:
        thread_count, core_count, core_list = config
        config_name = f"T{thread_count}_C{core_count}"
        config_dir = os.path.join(base_output_dir, config_name)
        os.makedirs(config_dir, exist_ok=True)
        
        print(f"\n{'='*80}")
        print(f"Running benchmarks for configuration: {config_name}")
        print(f"Threads: {thread_count}, Cores: {core_count}, Core List: {core_list}")
        print(f"{'='*80}")
        
        # Deploy memcached with this configuration
        print(f"Deploying memcached with {thread_count} threads on cores {core_list}...")
        memcached_ip = deploy_memcached(
            thread_count=thread_count, 
            memory_limit=1024, 
            cpuset=core_list
        )
        
        if not memcached_ip:
            print(f"Failed to deploy memcached for config {config_name}. Skipping...")
            continue
        
        # Run multiple times for this configuration
        for run_num in range(1, runs_per_config + 1):
            run_dir = os.path.join(config_dir, f"run_{run_num}")
            os.makedirs(run_dir, exist_ok=True)
            
            print(f"\nRunning benchmark for {config_name}, Run {run_num}/{runs_per_config}...")
            
            # Stop any existing agents and restart them
            print("Restarting mcperf agents...")
            restart_mcperf_agent(clients_info)
            
            # Run mcperf with the correct parameters
            print(f"Running mcperf benchmark (scan from 5K to 220K QPS)...")
            results_file = run_mcperf_load(
                clients_info,
                memcached_ip,
                run_dir,
                scan="5000:220000:5000",
                duration=5
            )
            
            if results_file:
                print(f"Benchmark completed. Results saved to {results_file}")
            else:
                print(f"Failed to run benchmark for {config_name}, Run {run_num}.")
            
            # Brief pause between runs
            time.sleep(10)
    
    # Stop mcperf agents at the end
    print("\nStopping mcperf agents...")
    stop_mcperf_agents()
    
    print("\nAll benchmarks completed successfully!")
    print(f"Results saved to {base_output_dir}/")

if __name__ == "__main__":
    main() 