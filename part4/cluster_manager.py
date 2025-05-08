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

def get_node_name(node_type="memcache-server"):
    """
    Discover a node in the Kubernetes cluster by its label or name pattern.
    
    Args:
        node_type (str): String pattern to match in node name or label
    
    Returns:
        str: The node name if found, None otherwise
    """
    try:
        # Get nodes matching the node_type pattern
        nodes_output = run_command(
            f"kubectl get nodes -o wide | grep {node_type}",
            capture_output=True,
            check=False
        ).strip()
        
        if not nodes_output:
            print(f"[ERROR] Could not find {node_type} node")
            return None
            
        # Extract the node name (first column)
        node_name = nodes_output.split()[0]
        print(f"[STATUS] Found node: {node_name}")
        return node_name
        
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to get node name: {str(e)}")
        return None

def get_memcached_ip(ssh_key_path):
    """
    Get the internal IP address of the memcached server.
    
    Args:
        node_name (str): The name of the node running memcached
        ssh_key_path (str): Path to the SSH key
    
    Returns:
        str: Internal IP address of the memcached server, or None if not found
    """
    try:
        # Get the internal IP address
        cmd = f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{get_node_name('memcache-server')} " \
              f"--zone europe-west1-b --command \"hostname -I | awk '{{print $1}}'\""
        
        ip_address = run_command(cmd, capture_output=True)
        if ip_address:
            print(f"[STATUS] Found memcached internal IP: {ip_address}")
            return ip_address
        else:
            print("[ERROR] Could not determine memcached internal IP")
            return None
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to get memcached IP: {str(e)}")
        return None

def deploy_memcached(thread_count = 2, memory_limit = 1024):
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
    node_type = get_node_name("memcache-server")
    if not node_type:
        print("[ERROR] deploy_memcached: Could not find memcache-server node")
        return None
    
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

    # # Optionally pin to specific cores
    # if cpuset:
    #     # Append cpuset to systemd config override
    #     override = (
    #         "[Service]\n"
    #         f"ExecStart=\n"
    #         f"ExecStart=/usr/bin/taskset -c {cpuset} "
    #         f"/usr/bin/memcached -u memcache -m {memory_limit} "
    #         f"-p 11211 -t {thread_count}\n"
    #     )
    #     # Write override file
    #     run_command(
    #         base_ssh +
    #         " \"printf '%s' '" + override.replace("'", "'\"'\"'") +
    #         "' | sudo tee /etc/systemd/system/memcached.service.d/"
    #         "override.conf\""
    #     )
    #     # Reload systemd units
    #     run_command(base_ssh + " \"sudo systemctl daemon-reload\"")

    # # Restart memcached
    run_command(base_ssh + " \"sudo systemctl restart memcached\"")

    # Wait until memcached is listening on port 11211
    time.sleep(5)
    print(
        f"[STATUS] deploy_memcached: memcached installed & started on " + 
        f"{node_type} ({ip}) with {thread_count} threads, {memory_limit}MB "
    )
    return ip

if __name__ == "__main__":
    # Example usage
    state_store = "gs://cca-eth-2025-group-092-fbaldin/"
    cluster_config_yaml = "part4.yaml"
    
    # Setup the cluster
    setup_cluster(state_store, cluster_config_yaml)
    
    # # Deploy memcached
    memcached_ip = deploy_memcached(thread_count=2, memory_limit=1024)