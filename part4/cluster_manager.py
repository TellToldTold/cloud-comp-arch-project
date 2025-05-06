#!/usr/bin/env python3

from utils import run_command
import subprocess
import time
import os
import yaml

def setup_cluster(state_store, cluster_config_yaml):
    """
    Sets up a Kubernetes cluster using kops.
    This function configures the Kubernetes cluster by setting the required 
    environment variables, creating the cluster using a provided configuration 
    file, updating the cluster, validating it, and retrieving information about 
    the cluster nodes.

    Parameters
    ----------
    state_store (str)
        The GCS bucket URI to be used as the KOPS state store.
        This is where kops stores its configuration and state files.
    cluster_config_yaml (str)
        The file path to the YAML configuration file that defines the cluster 
        specifications.

    Returns
    -------
    None
    """
    # Set the KOPS_STATE_STORE environment variable
    os.environ["KOPS_STATE_STORE"] = state_store
    print(f"[STATUS] setup_cluster: Set KOPS_STATE_STORE to {state_store}")
    
    # Get the project name
    project = run_command("gcloud config get-value project", capture_output=True)
    print(f"[STATUS] setup_cluster: Project name: {project}")
    
    # Create the cluster
    run_command(f"kops create -f {cluster_config_yaml}")

    # Determine cluster name from config (supports multi-document YAML)
    with open(cluster_config_yaml) as f:
        docs = list(yaml.safe_load_all(f))
    if not docs:
        raise ValueError(
            f"[ERROR] setup_cluster: No documents found in" + 
            f" {cluster_config_yaml}"
        )
    cluster_cfg = docs[0]
    cluster_name = cluster_cfg.get("metadata", {}).get("name")
    if not cluster_name:
        raise ValueError(
            f"[ERROR] setup_cluster: Could not determine cluster name from" + 
            f" {cluster_config_yaml}"
        )
    
    # Update the cluster
    run_command(f"kops update cluster --name {cluster_name} --yes --admin")
    
    # Validate the cluster (wait up to 10 minutes)
    run_command("kops validate cluster --wait 10m")
    
    # Get the nodes
    nodes_info = run_command("kubectl get nodes -o wide", capture_output=True)
    print(f"[STATUS] setup_cluster: Cluster nodes:\n{nodes_info}")

def get_memcached_ip():
    """Get the memcached pod IP if it exists."""
    # Check if memcached pod exists
    pod_check = run_command(
        "kubectl get pods -o wide | grep memcache",
        capture_output=True,
        check=False
    )
    
    if pod_check:
        pod_info_lines = pod_check.split("\n")
        pod_ip = None
        for line in pod_info_lines:
            if "memcache" in line:
                pod_ip = line.split()[5]  # IP should be in the 6th column
                break
        
        if pod_ip:
            print(
                f"[STATUS] get_memcached_ip: Found existing memcached pod " + 
                f"IP: {pod_ip}"
            )
            return pod_ip
    
    print("[ERROR] get_memcached_ip: No existing memcached pod found")
    return None

def deploy_memcached(thread_count = 1, memory_limit = 1024, cpuset = None):
    """
    Deploys and configures memcached directly on the specified VM.
    This function SSHes into the memcached server VM, installs memcached,
    updates its configuration for memory, threads, and listen address,
    and restarts the service. Finally, it retrieves and returns the internal IP.

    Parameters
    ----------
    thread_count : int
        The number of memcached threads (to set -t). Default is 1.
    memory_limit : int
        The memory limit for memcached in MB (to set -m). Default is 1024MB.
    cpuset : str, optional
        Comma-delimited list of CPU cores to pin memcached to via taskset before
        service start. Default is None (no pinning).
        
    Returns
    -------
    str
        The internal IP address of the memcached server.
        Returns None if the deployment fails.
    """
    # Discover the memcached server node via Kubernetes
    nodes_output = run_command(
        "kubectl get nodes -o wide | grep memcache-server",
        capture_output=True,
        check=False
    ).strip()
    if not nodes_output:
        print("[ERROR] deploy_memcached: Could not find memcache-server node")
        return None
    node_type = nodes_output.split()[0]
    print(f"[STATUS] deploy_memcached: deploying to node {node_type}")

    # Build SSH prefix
    ssh_key = os.path.expanduser("~/.ssh/cloud-computing")
    base_ssh = (
        f"gcloud compute ssh --quiet --ssh-key-file {ssh_key} "
        f"ubuntu@{node_type} --zone europe-west1-b --command"
    )

    # Install memcached and tools
    run_command(base_ssh +
        " \"sudo apt-get update && "
        "sudo apt-get install -y memcached libmemcached-tools\""
    )

    # Verify memcached service is running
    status = run_command(
        base_ssh + " \"sudo systemctl is-active memcached\"",
        capture_output=True,
        check=False
    ).strip()
    if status == "active":
        print(
            f"[STATUS] deploy_memcached: memcached service is active on " + 
            f"{node_type}"
        )
    else:
        print(
            f"[ERROR] deploy_memcached: memcached service is NOT active " + 
            f"(status: {status})"
        )
        return None

    # Configure memcached: set memory to 1024MB, threads, and listen on internal
    # Retrieve the VM's internal IP
    ip = run_command(
        base_ssh + " \"hostname -I | awk '{print $1}'\"",
        capture_output=True
    ).strip()
    if not ip:
        print(
            f"[ERROR] deploy_memcached: Could not determine internal IP of " + 
            f"{node_type}"
        )
        return None

    # Update memcached.conf
    run_command(base_ssh +
        f" \"sudo sed -i 's/^-m .*/-m {memory_limit}/' /etc/memcached.conf && "
        f"sudo sed -i 's/^-l .*/-l {ip}/' /etc/memcached.conf && "
        f"sudo sed -i '/^-t /d' /etc/memcached.conf && "
        f"echo '-t {thread_count}' | sudo tee -a /etc/memcached.conf\""
    )

    # Optionally pin to specific cores
    if cpuset:
        # Append cpuset to systemd config override
        override = (
            "[Service]\n"
            f"ExecStart=\n"
            f"ExecStart=/usr/bin/taskset -c {cpuset} "
            f"/usr/bin/memcached -u memcache -m {memory_limit} "
            f"-p 11211 -t {thread_count}\n"
        )
        # Write override file
        run_command(
            base_ssh +
            " \"printf '%s' '" + override.replace("'", "'\"'\"'") +
            "' | sudo tee /etc/systemd/system/memcached.service.d/"
            "override.conf\""
        )
        # Reload systemd units
        run_command(base_ssh + " \"sudo systemctl daemon-reload\"")

    # Restart memcached
    run_command(base_ssh + " \"sudo systemctl restart memcached\"")

    # Wait until memcached is listening on port 11211
    time.sleep(5)
    print(
        f"[STATUS] deploy_memcached: memcached installed & started on " + 
        f"{node_type} ({ip}) with {thread_count} threads, {memory_limit}MB " +
        f"memory, and cpuset {cpuset}"
    )
    return ip

def update_memcached_resources(
        thread_count = None,
        memory_limit = None,
        cpuset = None
    ):
    """
    Update memcached resource allocations on the memcache-server VM.

    This function SSHes into the memcached server VM, and for any provided
    parameter will update:
      - number of threads (-t)
      - memory limit in MB (-m)
      - CPU core affinity (via systemd override with taskset)

    Parameters
    ----------
    thread_count : int or None
        New number of memcached threads. If None, leave unchanged.
    memory_limit : int or None
        New memory limit in MB. If None, leave unchanged.
    cpuset : str or None
        Comma-delimited core list or range to pin memcached to. If None,
        existing affinity remains unchanged.

    Returns
    -------
    bool
        True if update commands succeeded, False otherwise.
    """
    # Discover the memcached server node
    nodes_output = run_command(
        "kubectl get nodes -o wide | grep memcache-server",
        capture_output=True, check=False
    ).strip()
    if not nodes_output:
        print(
            "[ERROR] update_memcached_resources: Cannot find memcache-server " +
            "node"
        )
        return False
    node_type = nodes_output.split()[0]
    ssh_key = os.path.expanduser("~/.ssh/cloud-computing")
    base_ssh = (
        f"gcloud compute ssh --quiet --ssh-key-file {ssh_key} "
        f"ubuntu@{node_type} --zone europe-west1-b --command"
    )

    # Update memcached.conf settings
    cmd_parts = []
    if memory_limit is not None:
        cmd_parts.append(
            f"sudo sed -i 's/^-m .*/-m {memory_limit}/' /etc/memcached.conf"
        )
    if thread_count is not None:
        # remove old -t lines and append new
        cmd_parts.append("sudo sed -i '/^-t /d' /etc/memcached.conf")
        cmd_parts.append(
            f"echo '-t {thread_count}' | sudo tee -a /etc/memcached.conf"
        )
    if cmd_parts:
        run_command(
            base_ssh + " \"" + " && ".join(cmd_parts) + "\""
        )

    # Update systemd override for cpuset if requested
    if cpuset is not None:
        # Build the override header
        lines = ["[Service]", "ExecStart="]

        # Always set the taskset prefix
        cmd = (f"/usr/bin/taskset -c {cpuset} /usr/bin/memcached -u memcache")

        # Append memory flag only if requested
        if memory_limit is not None:
            cmd += f" -m {memory_limit}"

        # Append thread flag only if requested
        if thread_count is not None:
            cmd += f" -t {thread_count}"

        # Listen port is always needed
        cmd += " -p 11211"

        lines.append(f"ExecStart={cmd}")

        override = "\n".join(lines) + "\n"

        # Ensure systemd drop-in directory exists
        run_command(
            base_ssh + " \"sudo mkdir -p /etc/systemd/system/memcached.service.d\""
        )
        # Write override file
        run_command(
            base_ssh +
            " \"printf '%s' '" + override.replace("'", "'\"'\"'") +
            "' | sudo tee /etc/systemd/system/memcached.service.d/override.conf\""
        )
        run_command(base_ssh + " \"sudo systemctl daemon-reload\"")

    # Restart memcached service to apply changes
    try:
        run_command(base_ssh + " \"sudo systemctl restart memcached\"", check=True)
        print(f"[STATUS] update_memcached_resources: memcached on {node_type} "
              f"restarted with threads={thread_count}, memory={memory_limit}, cpuset={cpuset}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] update_memcached_resources: Failed to restart memcached: {e}")
        return False

def delete_all_k8s_jobs():
    """Delete all jobs in the Kubernetes cluster."""
    run_command("kubectl delete jobs --all", check=False)
    print("[STATUS] delete_all_jobs: Deleted all jobs in the cluster")

if __name__ == "__main__":
    # Example usage
    # state_store = "gs://cca-eth-2025-group-092-fbaldin/"
    # cluster_config_yaml = "part4.yaml"
    
    # # Setup the cluster
    # setup_cluster(state_store, cluster_config_yaml)
    
    # # Deploy memcached
    memcached_ip = deploy_memcached(thread_count=4, memory_limit=2048)
    
    # Update memcached resources
    update_memcached_resources(thread_count=6, memory_limit=1024, cpuset="0-3")