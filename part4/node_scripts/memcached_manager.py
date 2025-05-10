#!/usr/bin/env python3

import subprocess
import psutil
import os
from typing import Dict, List, Optional, Tuple


def get_memcached_pid() -> Optional[int]:
    """
    Get the process ID of the main memcached server.
    
    Returns:
        Optional[int]: PID of memcached or None if not found
    """
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] == 'memcached':
            return proc.info['pid']
    return None


def get_memcached_all_thread_ids(pid: Optional[int] = None) -> List[str]:
    """
    Get all thread IDs (TIDs) for memcached including main thread.
    
    Args:
        pid (Optional[int]): Process ID of memcached, or None to auto-detect
    
    Returns:
        List[str]: List of all thread IDs as strings
    """
    if pid is None:
        pid = get_memcached_pid()
        
    if pid is None:
        return []
        
    try:
        ps_cmd = f"ps -L -p {pid} -o tid | grep -v TID"
        output = subprocess.check_output(ps_cmd, shell=True, universal_newlines=True)
        return [tid.strip() for tid in output.splitlines() if tid.strip()]
    except subprocess.SubprocessError as e:
        print(f"Error getting thread IDs for PID {pid}: {str(e)}")
        return []


def get_memcached_thread_ids(pid: Optional[int] = None) -> List[str]:
    """
    Get worker thread IDs (TIDs) for memcached.
    
    Args:
        pid (Optional[int]): Process ID of memcached, or None to auto-detect
    
    Returns:
        List[str]: List of worker thread IDs as strings
    """
    if pid is None:
        pid = get_memcached_pid()
        
    if pid is None:
        return []
        
    try:
        ps_cmd = f"ps -L -p {pid} -o tid,comm | grep mc-worker | awk '{{print $1}}'"
            
        output = subprocess.check_output(ps_cmd, shell=True, universal_newlines=True)
        return [tid.strip() for tid in output.splitlines() if tid.strip()]
    except subprocess.SubprocessError as e:
        print(f"Error getting worker thread IDs for PID {pid}: {str(e)}")
        return []


def get_memcached_cpu_affinity() -> List[int]:
    """
    Get the CPU affinity of memcached main process.
    
    Returns:
        List[int]: List of CPU cores the process is allowed to run on
    """
    pid = get_memcached_pid()
    
    if pid is None:
        return []
    
    try:
        # Use taskset to get the affinity of the main process
        output = subprocess.check_output(['taskset', '-p', str(pid)], 
                                      stderr=subprocess.PIPE, 
                                      universal_newlines=True)
        
        # Parse the output to extract the CPU list
        # Example output: "pid 12345's current affinity mask: f" (hex)
        if 'current affinity mask:' in output:
            mask_hex = output.split('current affinity mask:')[1].strip()
            # Convert hex mask to list of CPU cores
            mask_int = int(mask_hex, 16)
            cores = []
            for i in range(32):  # Assume max 32 cores
                if mask_int & (1 << i):
                    cores.append(i)
            return cores
    except (subprocess.CalledProcessError, ValueError) as e:
        print(f"Error getting affinity for process {pid}: {str(e)}")
    
    return []


def get_memcached_cpu_percent() -> Dict[str, Tuple[float, int]]:
    """
    Get the CPU usage percentage of memcached threads using pidstat.
    
    Returns:
        Dict[str, float]: Dictionary mapping thread IDs to CPU usage percentage (0-100% per core)
    """
    pid = get_memcached_pid()
    if pid is None:
        return {}
    
    try:
        # Use pidstat with -t flag to get per-thread usage, 1 interval for a single sample
        cmd = f"pidstat -t -p {pid} 1 1"
        output = subprocess.check_output(cmd, shell=True, universal_newlines=True)
        
        # Process output lines
        lines = output.splitlines()
        
        # Find where the data starts (after headers)
        data_start = 0
        for i, line in enumerate(lines):
            if 'Command' in line and '%CPU' in line:
                data_start = i + 1
                break
        
        # Skip the main process line and process thread lines
        result = {}
        in_threads = False
        
        for line in lines[data_start:]:
            if not line.strip():
                continue
                
            # Check if it's a thread line (has |__ prefix in command)
            if '|__mc-worker' in line:
                parts = line.split()
                if len(parts) >= 8:
                    # TID is at position 2 for thread entries
                    tid = parts[3].strip()
                    # %CPU is the sum of %usr, %system, %guest
                    try:
                        cpu_percent = float(parts[8].strip())
                        cores = int(parts[9].strip())
                        result[tid] = (cpu_percent, cores)
                    except (ValueError, IndexError):
                        pass
        
        return result
    except subprocess.SubprocessError as e:
        print(f"Error getting CPU usage: {str(e)}")
        return {}


def set_memcached_affinity(cores: List[int], pid: Optional[int] = None) -> Dict[str, bool]:
    """
    Set CPU affinity for all threads of a memcached process.
    
    Args:
        cores (List[int]): List of CPU core IDs to use
        pid (Optional[int]): Specific memcached PID to bind, or None to auto-detect
    
    Returns:
        Dict[str, bool]: Dictionary mapping thread IDs to success status
    """
    results = {}
    
    if pid is None:
        pid = get_memcached_pid()
        
    if pid is None:
        return results
    
    try:
        # Format cores for taskset
        cores_str = ','.join(map(str, cores))
        
        # Get all thread IDs for this process
        thread_ids = get_memcached_all_thread_ids(pid)
        
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
        print(f"Error in set_memcached_affinity: {str(e)}")
        return {tid: False for tid in get_memcached_all_thread_ids(pid) or []}
