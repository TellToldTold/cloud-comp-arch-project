#!/usr/bin/env python3

import subprocess
import psutil
import os
from typing import Dict, List, Optional, Tuple


def get_memcached_pids() -> List[int]:
    """
    Get the process IDs of all memcached servers.
    
    Returns:
        List[int]: List of PIDs of memcached processes
    """
    pids = []
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] == 'memcached':
            pids.append(proc.info['pid'])
    return pids


def get_memcached_pid() -> Optional[int]:
    """
    Get the process ID of the memcached server.
    
    Returns:
        Optional[int]: PID of memcached or None if not found
    """
    pids = get_memcached_pids()
    return pids[0] if pids else None


def get_memcached_cpu_affinity() -> Dict[int, List[int]]:
    """
    Get the CPU affinity of all memcached processes.
    
    Returns:
        Dict[int, List[int]]: Dictionary mapping PIDs to list of CPU cores they're allowed to run on
    """
    result = {}
    for pid in get_memcached_pids():
        try:
            process = psutil.Process(pid)
            result[pid] = process.cpu_affinity()
        except (psutil.NoSuchProcess, AttributeError):
            continue
    return result


def get_memcached_cpu_percent() -> Dict[int, float]:
    """
    Get the CPU usage percentage of all memcached processes.
    
    Returns:
        Dict[int, float]: Dictionary mapping PIDs to CPU usage percentage (0-100% per core)
    """
    result = {}
    for pid in get_memcached_pids():
        try:
            process = psutil.Process(pid)
            # Initialize CPU monitoring
            process.cpu_percent(interval=None)
            
            # Wait a short interval for accurate measurement
            import time
            time.sleep(0.1)
            
            # Get actual CPU percentage
            result[pid] = process.cpu_percent(interval=None)
        except psutil.NoSuchProcess:
            continue
    return result


def get_memcached_cpu_percent_per_core() -> Dict[int, Dict[int, float]]:
    """
    Get the CPU usage percentage of memcached per core for all processes.
    
    Returns:
        Dict[int, Dict[int, float]]: Dictionary mapping PIDs to dictionaries that map core IDs to usage percentages
    """
    result = {}
    # Get system-wide per-CPU times before a small interval
    import time
    cpu_times_before = psutil.cpu_times_percent(interval=0.1, percpu=True)
    
    for pid in get_memcached_pids():
        try:
            process = psutil.Process(pid)
            
            # Get cores that this memcached process is allowed to run on
            try:
                affinity = process.cpu_affinity()
            except AttributeError:
                continue
                
            if not affinity:
                continue
            
            # Initialize CPU usage dict with zeros for each core
            cpu_usage = {core: 0.0 for core in affinity}
            
            # Get system-wide per-CPU times after the interval
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
            
            result[pid] = cpu_usage
        except (psutil.NoSuchProcess, IndexError):
            continue
            
    return result


def get_memcached_thread_ids(pid: int) -> List[str]:
    """
    Get all thread IDs (TIDs) for a given memcached process.
    
    Args:
        pid (int): Process ID of memcached
    
    Returns:
        List[str]: List of thread IDs as strings
    """
    try:
        if not os.path.exists(f'/proc/{pid}/task'):
            return []
        return os.listdir(f'/proc/{pid}/task')
    except (FileNotFoundError, PermissionError) as e:
        print(f"Error getting thread IDs for PID {pid}: {str(e)}")
        return []


def bind_memcached_threads(cores: List[int], pid: Optional[int] = None) -> Dict[str, bool]:
    """
    Set CPU affinity for all threads of a memcached process.
    
    Args:
        cores (List[int]): List of CPU core IDs to use
        pid (Optional[int]): Specific memcached PID to bind, or None to bind all
    
    Returns:
        Dict[str, bool]: Dictionary mapping thread IDs to success status
    """
    results = {}
    
    # Get main memcached PID(s)
    if pid is not None:
        pids = [pid]
    else:
        pids = get_memcached_pids()
    
    if not pids:
        return results
    
    try:
        # Format cores for taskset
        cores_str = ','.join(map(str, cores))
        
        for main_pid in pids:
            # Get all thread IDs for this process
            thread_ids = get_memcached_thread_ids(main_pid)
            
            for tid in thread_ids:
                try:
                    # Set affinity using taskset for each thread
                    subprocess.check_call(['taskset', '-pc', cores_str, tid], 
                                          stderr=subprocess.PIPE)  # Suppress stderr
                    results[tid] = True
                except subprocess.CalledProcessError as e:
                    print(f"Error setting affinity for thread {tid}: {str(e)}")
                    results[tid] = False
        
        return results
    except Exception as e:
        print(f"Error in bind_memcached_threads: {str(e)}")
        thread_ids = []
        for pid in pids:
            thread_ids.extend(get_memcached_thread_ids(pid))
        return {tid: False for tid in thread_ids}


def set_memcached_affinity(cores: List[int]) -> Dict[int, bool]:
    """
    Set CPU affinity for all memcached processes and their threads.
    
    Args:
        cores (List[int]): List of CPU core IDs to use
    
    Returns:
        Dict[int, bool]: Dictionary mapping PIDs to success status
    """
    # First, bind all threads of all memcached processes
    thread_results = bind_memcached_threads(cores)
    
    # Convert results to the expected format (per main PID)
    pids = get_memcached_pids()
    if not pids:
        return {}
    
    # If any thread of a process was successfully bound, consider the process as successfully bound
    results = {}
    for pid in pids:
        thread_ids = get_memcached_thread_ids(pid)
        # Check if any thread for this pid succeeded
        success = any(thread_results.get(tid, False) for tid in thread_ids)
        results[pid] = success
    
    return results 