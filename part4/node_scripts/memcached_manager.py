#!/usr/bin/env python3

import subprocess
import psutil
from typing import Dict, List, Optional


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
    Get the CPU usage percentage of the memcached process.
    
    Returns:
        float: CPU usage percentage (0-100% per core) or 0 if process not found
    """
    pid = get_memcached_pid()
    if not pid:
        return 0.0
    
    try:
        process = psutil.Process(pid)
        # Initialize CPU monitoring
        process.cpu_percent(interval=None)
        
        # Wait a short interval for accurate measurement
        import time
        time.sleep(0.1)
        
        # Get actual CPU percentage
        return process.cpu_percent(interval=None)
    except psutil.NoSuchProcess:
        return 0.0


def get_memcached_cpu_percent_per_core() -> Dict[int, float]:
    """
    Get the CPU usage percentage of memcached per core.
    
    Returns:
        Dict[int, float]: Dictionary mapping core IDs to usage percentages (0-100%)
    """
    pid = get_memcached_pid()
    if not pid:
        return {}
    
    # Get cores that memcached is allowed to run on
    affinity = get_memcached_cpu_affinity()
    if not affinity:
        return {}
    
    # Initialize CPU usage dict with zeros for each core
    cpu_usage = {core: 0.0 for core in affinity}
    
    try:
        process = psutil.Process(pid)
        import time
        
        # Get system-wide per-CPU times before and after a small interval
        cpu_times_before = psutil.cpu_times_percent(interval=0.1, percpu=True)
        cpu_times_after = psutil.cpu_times_percent(interval=0.3, percpu=True)
        
        # Get total process CPU usage
        total_proc_cpu = process.cpu_percent(interval=None)
        
        if total_proc_cpu > 0:
            # Estimate distribution across cores
            for core_idx in affinity:
                if core_idx < len(cpu_times_after):
                    # Use the system usage on this core as a proportion
                    core_usage = 100.0 - cpu_times_after[core_idx].idle
                    
                    # Estimate how much of this core usage is from memcached
                    cpu_usage[core_idx] = min(core_usage, total_proc_cpu / len(affinity))
        
        return cpu_usage
    except (psutil.NoSuchProcess, IndexError):
        return cpu_usage


def set_memcached_affinity(cores: List[int]) -> bool:
    """
    Set CPU affinity for the memcached process.
    
    Args:
        cores (List[int]): List of CPU core IDs to use
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        pid = get_memcached_pid()
        if not pid:
            return False
        
        # Format cores for taskset
        cores_str = ','.join(map(str, cores))
        
        # Set affinity using taskset
        taskset_cmd = f"taskset -pc {cores_str} {pid}"
        subprocess.check_call(taskset_cmd, shell=True)
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error setting memcached affinity: {str(e)}")
        return False 