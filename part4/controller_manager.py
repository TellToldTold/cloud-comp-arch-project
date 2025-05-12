#!/usr/bin/env python3

import argparse
import os
import subprocess
from pathlib import Path
from utils import run_command
from cluster_manager import get_node_name


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
        
        # Copy all files from node_scripts directory
        node_scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_scripts")
        print(f"[STATUS] Copying all files from {node_scripts_dir} to remote node...")
        run_command(
            f"gcloud compute scp --ssh-key-file {ssh_key_path} "
            f"{node_scripts_dir}/* ubuntu@{node_name}:~/dynamic_scheduler/ "
            f"--zone europe-west1-b"
        )
        
        # Create dpkg config to prevent interactive prompts
        print("[STATUS] Setting up noninteractive environment on remote node...")
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"echo 'Dpkg::Options "
            f"\\\"--force-confold\\\";' | sudo tee /etc/apt/apt.conf.d/local > "
            f"/dev/null\""
        )
        
        # Make the setup script executable and run it with noninteractive frontend
        print("[STATUS] Running setup script on remote node...")
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"cd ~/dynamic_scheduler && "
            f"chmod +x setup_scheduler.sh && DEBIAN_FRONTEND=noninteractive "
            f"sudo -E ./setup_scheduler.sh\""
        )
        
        print("[STATUS] Remote node setup complete!")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to set up remote node: {str(e)}")
        return False

def copy_files_only(node_name, ssh_key_path):
    """
    Copy only the controller files to the remote node without running setup scripts.
    
    Args:
        node_name (str): The name of the remote node
        ssh_key_path (str): Path to the SSH key
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"[STATUS] Copying files to remote node {node_name}...")
        
        # Create a directory for the controller files
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"mkdir -p ~/dynamic_scheduler\""
        )
        
        # Copy all files from node_scripts directory
        node_scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_scripts")
        print(f"[STATUS] Copying all files from {node_scripts_dir} to remote node...")
        run_command(
            f"gcloud compute scp --ssh-key-file {ssh_key_path} "
            f"{node_scripts_dir}/* ubuntu@{node_name}:~/dynamic_scheduler/ "
            f"--zone europe-west1-b"
        )
        
        print("[STATUS] File copying complete!")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to copy files: {str(e)}")
        return False

def launch_controller(
        node_name,
        ssh_key_path,
        scheduler_script
    ):
    """
    Launch the scheduler controller on the remote node.
    
    Args
    ----
    node_name (str):
        The name of the remote node
    ssh_key_path (str):
        Path to the SSH key
    scheduler_script (str):
        The name of the scheduler script, e.g., "scheduler_controller.py"
        
    Returns
    -------
    bool:
        True if successful, False otherwise
    """
    try:
        print(
            f"[STATUS] Launching controller on remote node with script " 
            f"{scheduler_script}..."
        )
        # Run the controller directly
        run_command(
            f"gcloud compute ssh --ssh-key-file {ssh_key_path} ubuntu@{node_name} "
            f"--zone europe-west1-b --command \"cd ~/dynamic_scheduler && "
            f"sudo python3 {scheduler_script}\""
        )
        print(f"[STATUS] Controller completed!")
        
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to launch controller: {str(e)}")
        return False

def main():
    """Main function to parse arguments and execute commands."""
    parser = argparse.ArgumentParser(
        description="Launch and manage the dynamic scheduler controller"
    )
    
    # Action to perform
    parser.add_argument(
        "--action", 
        choices=[
            "setup", "launch", "copy-files"
        ], 
        required=True,
        help=(
            "Action to perform: setup the node, launch controller, stop "
            "controller, get logs, setup mcperf, run mcperf benchmark, "
            "stop mcperf, or copy files only"
        )
    )

    parser.add_argument(
        "--scheduler-script", 
        required=False,
        help="Scheduler script to run (required for launch action)."
    )
    
    # Optional arguments
    parser.add_argument(
        "--ssh-key", 
        default="~/.ssh/cloud-computing", 
        help="Path to SSH key (default: ~/.ssh/cloud-computing)"
    )
    
    args = parser.parse_args()
    
    # Expand the SSH key path
    ssh_key_path = os.path.expanduser(args.ssh_key)
    
    # Auto-discover node name
    node_name = get_node_name("memcache-server")
    if not node_name:
        print(
            "[ERROR] Could not auto-discover memcached node. Cannot continue."
        )
        return
    
    # Perform the requested action
    if args.action == "setup":
        setup_remote_node(node_name, ssh_key_path)
        
    elif args.action == "copy-files":
        copy_files_only(node_name, ssh_key_path)
        
    elif args.action == "launch":
        if not args.scheduler_script:
            print("[ERROR] --scheduler-script is required for launch action")
            return
        launch_controller(node_name, ssh_key_path, args.scheduler_script)


if __name__ == "__main__":
    main()