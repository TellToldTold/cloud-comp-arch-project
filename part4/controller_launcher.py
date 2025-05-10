#!/usr/bin/env python3

import argparse
import os
import subprocess
from pathlib import Path
from utils import run_command
from cluster_manager import get_node_name, get_memcached_ip
from mcperf_manager import setup_mcperf_agents, preload, start_load_agent, restart_mcperf_agent, run_mcperf_load, stop_mcperf_agents

def setup_remote_node(node_name, ssh_key_path):
    """
    Copy the controller files to the remote node and set them up.
    
    Args:
        node_name (str): The name of the remote node
        ssh_key_path (str): Path to the SSH key
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"[STATUS] Setting up remote node {node_name}...")
        
        # Create a directory for the controller files
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"mkdir -p ~/dynamic_scheduler\""
        )
        
        # List of files to copy
        files_to_copy = [
            "scheduler_logger.py",
            "resource_monitor.py",
            "container_manager.py",
            "memcached_manager.py",
            "scheduler_controller.py",
            "test_scheduler.py",
            "setup_scheduler.sh",
            "utils.py"
        ]
        
        # Copy each file to the remote node
        for file in files_to_copy:
            local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_scripts", file)
            if os.path.exists(local_path):
                print(f"[STATUS] Copying {file} to remote node...")
                run_command(
                    f"gcloud compute scp --ssh-key-file {ssh_key_path} "
                    f"{local_path} ubuntu@{node_name}:~/dynamic_scheduler/ "
                    f"--zone europe-west1-b"
                )
            else:
                print(f"[WARNING] File {file} not found, skipping...")
        
        # Create dpkg config to prevent interactive prompts
        print("[STATUS] Setting up noninteractive environment on remote node...")
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"echo 'Dpkg::Options \\\"--force-confold\\\";' | sudo tee /etc/apt/apt.conf.d/local > /dev/null\""
        )
        
        # Make the setup script executable and run it with noninteractive frontend
        print("[STATUS] Running setup script on remote node...")
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"cd ~/dynamic_scheduler && "
            f"chmod +x setup_scheduler.sh && DEBIAN_FRONTEND=noninteractive sudo -E ./setup_scheduler.sh\""
        )
        
        print("[STATUS] Remote node setup complete!")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to set up remote node: {str(e)}")
        return False

def launch_controller(node_name, ssh_key_path, memcached_ip):
    """
    Launch the scheduler controller on the remote node.
    
    Args:
        node_name (str): The name of the remote node
        ssh_key_path (str): Path to the SSH key
        memcached_ip (str): Internal IP of the memcached server
    
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        scheduler_script = "scheduler_controller.py"
        
        print(f"[STATUS] Launching controller on remote node with script {scheduler_script}...")
        # Run the controller directly
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"cd ~/dynamic_scheduler && "
            f"python3 {scheduler_script}\""
        )
        
        print(f"[STATUS] Controller launched in screen session 'controller'")
        print(f"[INFO] To attach to the controller session, ssh to the node and run: screen -r controller")
        print(f"[INFO] To detach from the session without stopping it, press Ctrl+A followed by D")
        
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to launch controller: {str(e)}")
        return False

def stop_controller(node_name, ssh_key_path):
    """
    Stop the running controller on the remote node.
    
    Args:
        node_name (str): The name of the remote node
        ssh_key_path (str): Path to the SSH key
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"[STATUS] Stopping controller on remote node...")
        
        # Kill the screen session
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"screen -X -S controller quit || true\""
        )
        
        print(f"[STATUS] Controller stopped")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to stop controller: {str(e)}")
        return False


def main():
    """Main function to parse arguments and execute commands."""
    parser = argparse.ArgumentParser(description="Launch and manage the dynamic scheduler controller")
    
    # Action to perform (the only required argument)
    parser.add_argument("--action", choices=["setup", "launch", "stop", "logs", "mcperf-setup", "mcperf-run", "mcperf-stop"], required=True,
                        help="Action to perform: setup the node, launch controller, stop controller, get logs, setup mcperf, run mcperf benchmark, or stop mcperf")
    
    # Optional arguments
    parser.add_argument("--ssh-key", default="~/.ssh/cloud-computing", 
                        help="Path to SSH key (default: ~/.ssh/cloud-computing)")
    parser.add_argument("--output-dir", default="logs", 
                        help="Directory to save logs to (default: logs)")
    parser.add_argument("--force-install", action="store_true",
                        help="Force reinstallation of mcperf (default: False)")
    
    args = parser.parse_args()
    
    # Expand the SSH key path
    ssh_key_path = os.path.expanduser(args.ssh_key)
    
    # Auto-discover node name
    node_name = get_node_name("memcache-server")
    if not node_name:
        print("[ERROR] Could not auto-discover memcached node. Cannot continue.")
        return
    
    # Perform the requested action
    if args.action == "setup":
        setup_remote_node(node_name, ssh_key_path)
        
    elif args.action == "launch":
        # Get memcached IP for information purposes only
        memcached_ip = get_memcached_ip(ssh_key_path)
        if memcached_ip:
            print(f"[INFO] Memcached is running at IP: {memcached_ip}")
            launch_controller(node_name, ssh_key_path, memcached_ip)
    
    elif args.action == "stop":
        stop_controller(node_name, ssh_key_path)
    
    # elif args.action == "logs":
    #     get_controller_logs(node_name, ssh_key_path, args.output_dir)
    

if __name__ == "__main__":
    main() 