#!/usr/bin/env python3

import subprocess
import psutil
from typing import Dict, List, Optional


# def get_local_ip() -> str:
#     """
#     Get the local IP address of the machine.
    
#     Returns:
#         str: Local IP address
#     """
#     try:
#         cmd = "hostname -I | awk '{print $1}'"
#         output = subprocess.check_output(cmd, shell=True, text=True).strip()
#         return output
#     except subprocess.CalledProcessError:
#         # Fallback to localhost if we can't get the IP
        # return "127.0.0.1"


# def get_memcached_stats() -> Optional[Dict]:
#     """
#     Get memcached server statistics.
    
#     Args:
#         memcached_ip (str, optional): IP address of the memcached server. If None, local IP is used.
#         port (int): Port of the memcached server (default: 11211)
    
#     Returns:
#         Optional[Dict]: Dictionary of memcached statistics or None if failed
#     """
#     memcached_ip = get_local_ip()
        
#     try:
#         cmd = f"echo 'stats' | nc {memcached_ip} {11211}"
#         output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        
#         # Parse the stats output
#         stats = {}
#         for line in output.strip().split('\n'):
#             if line.startswith('STAT '):
#                 parts = line.split()
#                 if len(parts) >= 3:
#                     key = parts[1]
#                     try:
#                         value = int(parts[2])
#                     except ValueError:
#                         try:
#                             value = float(parts[2])
#                         except ValueError:
#                             value = parts[2]
#                     stats[key] = value
#         return stats
#     except subprocess.CalledProcessError:
#         return None


def get_memcached_pid() -> Optional[int]:
    """
    Get the process ID of the memcached server.
    
    Returns:
        Optional[int]: PID of memcached or None if not found
    """
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] == 'memcached':
            return proc.info['pid']
    return None


def get_memcached_cpu_affinity() -> List[int]:
    """
    Get the CPU affinity of the memcached process.
    
    Returns:
        List[int]: List of CPU cores memcached is allowed to run on, or empty list if process not found
    """
    pid = get_memcached_pid()
    if not pid:
        return []
    
    try:
        process = psutil.Process(pid)
        return process.cpu_affinity()
    except (psutil.NoSuchProcess, AttributeError):
        return []


def get_memcached_cpu_percent() -> float:
    """
    Get the CPU usage percentage of memcached.
    
    Returns:
        float: CPU usage percentage (0-100%) or 0 if process not found
    """
    pid = get_memcached_pid()
    if not pid:
        return 0.0
    
    try:
        process = psutil.Process(pid)
        return process.cpu_percent(interval=0.1)
    except psutil.NoSuchProcess:
        return 0.0


def set_memcached_affinity(cores: List[int]) -> bool:
    """
    Set CPU affinity for the memcached process.
    
    Args:
        cores (List[int]): List of CPU core IDs to use
        logger (SchedulerLogger): Logger to log events
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Find all memcached processes (main process and its threads)
        cmd = "pgrep memcached"
        output = subprocess.check_output(cmd, shell=True, text=True).strip()
        if not output:
            # logger.custom_event(Job.MEMCACHED, "Memcached process not found")
            return False
        
        # Get the main PID
        pids = output.split('\n')
        main_pid = pids[0]
        
        # Format cores for taskset
        cores_str = ','.join(map(str, cores))
        
        # Set affinity for all memcached threads using taskset
        taskset_cmd = f"taskset -a -pc {cores_str} {main_pid}"
        subprocess.check_call(taskset_cmd, shell=True)
        
        # Log the cores update
        # logger.update_cores(Job.MEMCACHED, cores)
        
        return True
    except subprocess.CalledProcessError as e:
        # logger.custom_event(Job.MEMCACHED, f"Error setting affinity: {str(e)}")
        print(f"Error setting memcached affinity: {str(e)}")
        return False 