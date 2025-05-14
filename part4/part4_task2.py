#!/usr/bin/env python3

import os
from cluster_manager import get_node_name, deploy_memcached
from mcperf_manager import setup_mcperf_agents, preload, start_load_agent, run_mcperf_dynamic_load, stop_mcperf_agents
from controller_manager import copy_files_only, launch_controller

def main():
    """
    Script for Part 4 Task 2:
    Sets up memcached, starts mcperf agents, and launches the dynamic scheduler controller.
    Assumes the cluster is already running.
    """
    # Hardcoded configuration
    scheduler_script = "concurrent_scheduler.py"
    ssh_key_path = os.path.expanduser("~/.ssh/cloud-computing")
    output_dir = "part4_task2_results"
    memcached_threads = 2
    memcached_cores = "0"
    memcached_memory = 1024
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print("Setting up Part 4 Task 2...")
    
    # Auto-discover memcached node name
    node_name = get_node_name("memcache-server")
    if not node_name:
        print("ERROR: Could not find memcached node")
        return
    
    print(f"Found memcached server: {node_name}")
    
    # Deploy memcached with specified configuration
    print(f"Deploying memcached on core {memcached_cores}...")
    memcached_ip = deploy_memcached(
        thread_count=memcached_threads,
        memory_limit=memcached_memory,
        cpuset=memcached_cores
    )
    
    if not memcached_ip:
        print("ERROR: Failed to deploy memcached")
        return
    
    print(f"Memcached running at: {memcached_ip}")
    
    # Setup mcperf agents
    print("Setting up mcperf agents...")
    clients_info = setup_mcperf_agents()
    if not clients_info:
        print("ERROR: Failed to setup mcperf agents")
        return
    
    # Preload memcached with data
    print("Preloading memcached...")
    preload(clients_info, memcached_ip)
    
    # Start mcperf load agent
    print("Starting mcperf load agent...")
    start_load_agent(clients_info)
    
    # Setup remote node with scheduler files
    print(f"Setting up remote node with scheduler files...")
    if not copy_files_only(node_name, ssh_key_path):
        print("ERROR: Failed to set up remote node")
        return
    
    # Run dynamic mcperf load in background
    print("Starting dynamic mcperf load...")
    results_file = run_mcperf_dynamic_load(
        clients_info,
        memcached_ip,
        output_dir,
        duration=780,
        qps_seed=2333
    )
    
    print(f"mcperf load running, results: {results_file}")
    
    # Launch the controller on the remote node
    print(f"Launching controller with script: {scheduler_script}")
    if not launch_controller(node_name, ssh_key_path, scheduler_script):
        print("ERROR: Failed to launch controller")
        return
    
    print(f"mcperf results: {results_file}")

if __name__ == "__main__":
    main() 