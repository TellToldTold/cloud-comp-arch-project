import psutil
import time
import os
from typing import Dict, List, Tuple


def get_cpu_usage_per_core() -> List[float]:
    """
    Get the current CPU usage percentage for each core.
    
    Returns:
        List[float]: List of CPU usage percentages for each core (0-100%).
    """
    # Get CPU usage with a small interval (0.1s) for more accurate instantaneous reading
    return psutil.cpu_percent(interval=0.2, percpu=True)


def get_total_cpu_usage() -> float:
    """
    Get the total CPU usage percentage.
    
    Returns:
        float: Total CPU usage percentage (0-100%).
    """
    return psutil.cpu_percent(interval=0.1)


def get_memory_usage() -> Tuple[float, float]:
    """
    Get the current memory usage.
    
    Returns:
        Tuple[float, float]: (Used memory in MB, Total memory in MB)
    """
    mem = psutil.virtual_memory()
    used_mb = mem.used / (1024 * 1024)
    total_mb = mem.total / (1024 * 1024)
    return (used_mb, total_mb)


def get_memory_percent() -> float:
    """
    Get the current memory usage as a percentage.
    
    Returns:
        float: Memory usage percentage (0-100%)
    """
    return psutil.virtual_memory().percent


def get_process_cpu_affinity(pid: int) -> List[int]:
    """
    Get the CPU affinity of a process.
    
    Args:
        pid (int): Process ID
    
    Returns:
        List[int]: List of CPU cores the process is allowed to run on
    """
    try:
        process = psutil.Process(pid)
        return process.cpu_affinity()
    except (psutil.NoSuchProcess, AttributeError):
        return []


def get_process_cpu_percent(pid: int) -> float:
    """
    Get the CPU usage percentage of a specific process.
    
    Args:
        pid (int): Process ID
    
    Returns:
        float: CPU usage percentage (0-100%) or 0 if process not found
    """
    try:
        process = psutil.Process(pid)
        return process.cpu_percent(interval=0.1)
    except psutil.NoSuchProcess:
        return 0.0 