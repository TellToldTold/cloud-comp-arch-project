import psutil
import time
import os
import subprocess
from typing import Dict, List, Tuple, Optional


def get_cpu_usage_per_core() -> List[float]:
    """
    Get the current CPU usage percentage for each core.
    
    Returns:
        List[float]: List of CPU usage percentages for each core (0-100%).
    """
    # Get CPU usage with a small interval (0.1s) for more accurate instantaneous reading
    return psutil.cpu_percent(interval=0.1, percpu=True)


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


def get_memcached_stats(memcached_ip: str, port: int = 11211) -> Optional[Dict]:
    """
    Get memcached server statistics.
    
    Args:
        memcached_ip (str): IP address of the memcached server
        port (int): Port of the memcached server (default: 11211)
    
    Returns:
        Optional[Dict]: Dictionary of memcached statistics or None if failed
    """
    try:
        cmd = f"echo 'stats' | nc {memcached_ip} {port}"
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        
        # Parse the stats output
        stats = {}
        for line in output.strip().split('\n'):
            if line.startswith('STAT '):
                parts = line.split()
                if len(parts) >= 3:
                    key = parts[1]
                    try:
                        value = int(parts[2])
                    except ValueError:
                        try:
                            value = float(parts[2])
                        except ValueError:
                            value = parts[2]
                    stats[key] = value
        return stats
    except subprocess.CalledProcessError:
        return None


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


def get_container_stats(container_id: str) -> Optional[Dict]:
    """
    Get resource usage statistics for a Docker container.
    
    Args:
        container_id (str): ID or name of the Docker container
    
    Returns:
        Optional[Dict]: Dictionary with container stats or None if failed
    """
    try:
        cmd = f"docker stats {container_id} --no-stream --format '{{{{.CPUPerc}}}} {{{{.MemUsage}}}}'"
        output = subprocess.check_output(cmd, shell=True, text=True).strip()
        
        parts = output.split()
        if len(parts) >= 3:
            cpu_percent = float(parts[0].rstrip('%'))
            mem_used = parts[1]
            mem_limit = parts[3]
            
            return {
                'cpu_percent': cpu_percent,
                'memory_used': mem_used,
                'memory_limit': mem_limit
            }
        return None
    except subprocess.CalledProcessError:
        return None


def is_container_running(container_id: str) -> bool:
    """
    Check if a container is running.
    
    Args:
        container_id (str): ID or name of the Docker container
    
    Returns:
        bool: True if container is running, False otherwise
    """
    try:
        cmd = f"docker container inspect -f '{{{{.State.Running}}}}' {container_id}"
        output = subprocess.check_output(cmd, shell=True, text=True).strip().lower()
        return output == 'true'
    except subprocess.CalledProcessError:
        return False 