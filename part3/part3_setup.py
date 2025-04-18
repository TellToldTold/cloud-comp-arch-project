#!/usr/bin/env python3

import os
import sys
import time
import json
import subprocess
import argparse
from pathlib import Path

def run_command(command, shell=True, check=True, capture_output=False):
    """Run a shell command and return the output if capture_output is True."""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=shell, check=check, capture_output=capture_output, text=True)
    if capture_output:
        return result.stdout.strip()
    return None

def setup_cluster(state_store, part3_yaml_path):
    """Setup the Kubernetes cluster using kops."""
    # Set the KOPS_STATE_STORE environment variable
    os.environ["KOPS_STATE_STORE"] = state_store
    print(f"Set KOPS_STATE_STORE to {state_store}")
    
    # Get the project name
    project = run_command("gcloud config get-value project", capture_output=True)
    print(f"Project: {project}")
    
    # Create the cluster
    run_command(f"kops create -f {part3_yaml_path}")
    
    # Update the cluster
    run_command("kops update cluster --name part3.k8s.local --yes --admin")
    
    # Validate the cluster (wait up to 10 minutes)
    run_command("kops validate cluster --wait 10m")
    
    # Get the nodes
    nodes_info = run_command("kubectl get nodes -o wide", capture_output=True)
    print(f"Cluster nodes:\n{nodes_info}")

def get_memcached_ip():
    """Get the memcached pod IP if it exists."""
    # Check if memcached pod exists
    pod_check = run_command("kubectl get pods -o wide | grep memcached", capture_output=True, check=False)
    
    if pod_check:
        pod_info_lines = pod_check.split("\n")
        pod_ip = None
        for line in pod_info_lines:
            if "memcached" in line:
                pod_ip = line.split()[5]  # IP should be in the 6th column
                break
        
        if pod_ip:
            print(f"Found existing memcached pod IP: {pod_ip}")
            return pod_ip
    
    print("No existing memcached pod found")
    return None

def setup_memcached(node_type, thread_count, cpuset):
    """Setup memcached on the specified node with the specified thread count."""
    # Check if memcached pod already exists
    existing_ip = get_memcached_ip()
    if existing_ip:
        print("Memcached is already deployed, returning existing IP")
        return existing_ip
    
    # Create the memcached yaml from template
    with open("memcache/memcached-p3.yaml", "r") as f:
        memcached_yaml = f.read()
    
    # Replace placeholders
    memcached_yaml = memcached_yaml.replace("NODETYPE", node_type)
    memcached_yaml = memcached_yaml.replace("THREADCOUNT", str(thread_count))
    memcached_yaml = memcached_yaml.replace("CPUSET", cpuset)
    
    # Write the modified yaml to a file
    with open("memcached-deploy.yaml", "w") as f:
        f.write(memcached_yaml)
    
    # Deploy memcached
    run_command("kubectl create -f memcached-deploy.yaml")
    
    # Expose memcached
    run_command("kubectl expose pod memcached --name memcached-11211 --type LoadBalancer --port 11211 --protocol TCP")
    
    # Wait for the service to get an external IP
    print("Waiting for memcached service to get an external IP...")
    time.sleep(60)
    
    # Get the service info
    run_command("kubectl get service memcached-11211")
    
    # Get the pod info
    memcached_pod_info = run_command("kubectl get pods -o wide", capture_output=True)
    print(f"Memcached pod info:\n{memcached_pod_info}")
    
    # Extract the pod IP
    pod_info_lines = memcached_pod_info.split("\n")
    pod_ip = None
    for line in pod_info_lines:
        if "memcached" in line:
            pod_ip = line.split()[5]  # IP should be in the 6th column
            break
    
    if pod_ip:
        print(f"Memcached pod IP: {pod_ip}")
        return pod_ip
    else:
        print("Could not find memcached pod IP")
        return None

def setup_mcperf_clients():
    """Setup mcperf on client-agent and client-measure nodes."""
    # Get node information
    nodes_output = run_command("kubectl get nodes -o json", capture_output=True)
    nodes_data = json.loads(nodes_output)
    
    client_agent_a = None
    client_agent_b = None
    client_measure = None
    
    # Find the client nodes
    for node in nodes_data["items"]:
        node_name = node["metadata"]["name"]
        if "client-agent-a" in node_name:
            client_agent_a = {
                "name": node_name,
                "internal_ip": node["status"]["addresses"][0]["address"],
                "external_ip": node["status"]["addresses"][1]["address"]
            }
        elif "client-agent-b" in node_name:
            client_agent_b = {
                "name": node_name,
                "internal_ip": node["status"]["addresses"][0]["address"],
                "external_ip": node["status"]["addresses"][1]["address"]
            }
        elif "client-measure" in node_name:
            client_measure = {
                "name": node_name,
                "internal_ip": node["status"]["addresses"][0]["address"],
                "external_ip": node["status"]["addresses"][1]["address"]
            }
    
    if not (client_agent_a and client_agent_b and client_measure):
        print("Could not find all required client nodes")
        return None
    
    print(f"Client Agent A: {client_agent_a}")
    print(f"Client Agent B: {client_agent_b}")
    print(f"Client Measure: {client_measure}")
    
    # Setup mcperf on each node
    setup_commands = [
        "sudo sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources",
        "sudo apt-get update",
        "sudo apt-get install libevent-dev libzmq3-dev git make g++ --yes",
        "sudo apt-get build-dep memcached --yes",
        "cd && git clone https://github.com/eth-easl/memcache-perf-dynamic.git",
        "cd ~/memcache-perf-dynamic && make"
    ]
    
    ssh_key_path = os.path.expanduser("~/.ssh/cloud-computing")
    
    for node in [client_agent_a, client_agent_b, client_measure]:
        for cmd in setup_commands:
            ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node['name']} --zone europe-west1-b --command \"{cmd}\""
            run_command(ssh_cmd, check=False)  # Don't check as some commands might fail but still be ok
    
    return {
        "client_agent_a": client_agent_a,
        "client_agent_b": client_agent_b,
        "client_measure": client_measure
    }

def restart_mcperf_agents(clients_info):
    """Restart mcperf agents on client nodes to fix synchronization issues."""
    if not clients_info:
        print("Error: No client info provided")
        return
        
    ssh_key_path = os.path.expanduser("~/.ssh/cloud-computing")
    
    # Kill any running mcperf processes
    kill_cmd = "pkill -f mcperf || true"
    
    # Restart agents on both client-agent nodes
    for agent_key in ['client_agent_a', 'client_agent_b']:
        # Kill any existing mcperf processes
        ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info[agent_key]['name']} --zone europe-west1-b --command \"{kill_cmd}\""
        run_command(ssh_cmd, check=False)
        
    # Also kill on measure node to be safe
    ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_measure']['name']} --zone europe-west1-b --command \"{kill_cmd}\""
    run_command(ssh_cmd, check=False)
    
    print("Killed existing mcperf processes, waiting for cleanup...")
    time.sleep(5)
    
    # Restart the mcperf agent on client-agent-a
    agent_a_cmd = f"cd ~/memcache-perf-dynamic && ./mcperf -T 2 -A"
    ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_agent_a']['name']} --zone europe-west1-b --command \"{agent_a_cmd}\" &"
    run_command(ssh_cmd, check=False)
    
    # Restart the mcperf agent on client-agent-b
    agent_b_cmd = f"cd ~/memcache-perf-dynamic && ./mcperf -T 4 -A"
    ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_agent_b']['name']} --zone europe-west1-b --command \"{agent_b_cmd}\" &"
    run_command(ssh_cmd, check=False)
    
    print("Restarted mcperf agents")
    
    print(f"You can now run the load test with:")
    print(f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_measure']['name']} --zone europe-west1-b --command \"~/start_load.sh\" > mcperf_results_local.txt")

def start_mcperf_load(clients_info, memcached_ip):
    """Start the mcperf load generator on the client nodes."""
    if not memcached_ip:
        print("Error: No memcached IP provided")
        return
        
    ssh_key_path = os.path.expanduser("~/.ssh/cloud-computing")
    
    # Start the mcperf agent on client-agent-a
    agent_a_cmd = f"cd ~/memcache-perf-dynamic && ./mcperf -T 2 -A"
    ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_agent_a']['name']} --zone europe-west1-b --command \"{agent_a_cmd}\" &"
    run_command(ssh_cmd, check=False)
    
    # Start the mcperf agent on client-agent-b
    agent_b_cmd = f"cd ~/memcache-perf-dynamic && ./mcperf -T 4 -A"
    ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_agent_b']['name']} --zone europe-west1-b --command \"{agent_b_cmd}\" &"
    run_command(ssh_cmd, check=False)
    
    # Load the database on client-measure
    load_cmd = f"cd ~/memcache-perf-dynamic && ./mcperf -s {memcached_ip} --loadonly"
    ssh_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_measure']['name']} --zone europe-west1-b --command \"{load_cmd}\""
    run_command(ssh_cmd, check=False)
    
    # Create a script on client-measure to start the load
    start_load_script = f"""#!/bin/bash
cd ~/memcache-perf-dynamic
./mcperf -s {memcached_ip} -a {clients_info['client_agent_a']['internal_ip']} -a {clients_info['client_agent_b']['internal_ip']} --noload -T 6 -C 4 -D 4 -Q 1000 -c 4 -t 10 --scan 30000:30500:5
"""
    
    with open("start_load.sh", "w") as f:
        f.write(start_load_script)
    
    # Copy the script to client-measure
    scp_cmd = f"gcloud compute scp --ssh-key-file {ssh_key_path} start_load.sh ubuntu@{clients_info['client_measure']['name']}:~ --zone europe-west1-b"
    run_command(scp_cmd, check=False)
    
    # Make the script executable
    chmod_cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_measure']['name']} --zone europe-west1-b --command \"chmod +x ~/start_load.sh\""
    run_command(chmod_cmd, check=False)
    
    print(f"SSH to client-measure with: gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_measure']['name']} --zone europe-west1-b")
    
    print("Setup completed! To run the load test and get results directly on your machine, use:")
    print(f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{clients_info['client_measure']['name']} --zone europe-west1-b --command \"~/start_load.sh\" > mcperf_results_local.txt")

def main():
    parser = argparse.ArgumentParser(description="Setup script for Part 3 of the CCA project")
    parser.add_argument("--state-store", default="gs://cca-eth-2025-group-092-fbaldin/", help="GCP state store for kops (gs://...)")
    parser.add_argument("--part3-yaml", default="part3.yaml", help="Path to part3.yaml file")
    parser.add_argument("--node-type", default="node-a-2core", choices=["node-a-2core", "node-b-2core", "node-c-4core", "node-d-4core"], help="Node type to run memcached on")
    parser.add_argument("--thread-count", type=int, default=2, help="Number of memcached threads")
    parser.add_argument("--cpuset", default="0", help="CPU cores to pin memcached to (e.g., '0,1')")
    parser.add_argument("--setup-cluster", action="store_true", help="Setup the Kubernetes cluster")
    parser.add_argument("--setup-mcperf", action="store_true", help="Setup mcperf on client nodes")
    parser.add_argument("--setup-memcached", action="store_true", help="Setup memcached on the specified node")
    parser.add_argument("--restart-mcperf", action="store_true", help="Restart mcperf agents to fix synchronization issues")
    
    args = parser.parse_args()
    
    # Setup the cluster if requested
    if args.setup_cluster:
        setup_cluster(args.state_store, args.part3_yaml)
    
    # Variable to hold memcached_ip
    memcached_ip = None
    
    # Setup memcached if requested
    if args.setup_memcached:
        memcached_ip = setup_memcached(args.node_type, args.thread_count, args.cpuset)
    
    # Variable to hold clients_info
    clients_info = None
    
    # Setup mcperf if requested
    if args.setup_mcperf:
        # If we didn't set up memcached in this run, try to get the IP
        if not memcached_ip:
            memcached_ip = get_memcached_ip()
            
        # Make sure we have the memcached IP before proceeding
        if not memcached_ip:
            print("Error: Could not determine memcached IP. Make sure memcached is deployed before setting up mcperf.")
            sys.exit(1)
            
        clients_info = setup_mcperf_clients()
        if clients_info:
            start_mcperf_load(clients_info, memcached_ip)
    
    # Restart mcperf agents if requested
    if args.restart_mcperf:
        # Get client info if not already available
        if not clients_info:
            clients_info = setup_mcperf_clients()
            
        if clients_info:
            restart_mcperf_agents(clients_info)

if __name__ == "__main__":
    main() 