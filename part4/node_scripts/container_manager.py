#!/usr/bin/env python3

import subprocess
import docker
from docker.models.containers import Container
from typing import List, Dict, Optional, Tuple, Union
from scheduler_logger import SchedulerLogger, Job


# Initialize Docker client
client = docker.from_env()


def get_container_by_id_or_name(container_id_or_name: str) -> Optional[Container]:
    """
    Get a container object by its ID or name.
    
    Args:
        container_id_or_name (str): ID or name of the container
    
    Returns:
        Optional[Container]: Container object if found, None otherwise
    """
    try:
        return client.containers.get(container_id_or_name)
    except Exception as e:
        print(f"Error getting container {container_id_or_name}: {str(e)}")
        return None


def run_batch_job(
    job_name: str, 
    cores: List[int], 
    threads: int
) -> Optional[Container]:
    """
    Run a batch job on specific CPU cores with a given number of threads.
    
    Args:
        job_name (str): Name of the job (e.g., 'blackscholes', 'canneal')
        cores (List[int]): List of CPU core IDs to use
        threads (int): Number of threads to use
        logger (SchedulerLogger): Logger to log job events
    
    Returns:
        Optional[Container]: Docker container object if successful, None otherwise
    """
    try:
        # Map job_name to Job enum
        job_enum = getattr(Job, job_name.upper())
        
        # Define image mapping
        image_mapping = {
            'blackscholes': 'anakli/cca:parsec_blackscholes',
            'canneal': 'anakli/cca:parsec_canneal',
            'dedup': 'anakli/cca:parsec_dedup',
            'ferret': 'anakli/cca:parsec_ferret',
            'freqmine': 'anakli/cca:parsec_freqmine',
            'radix': 'anakli/cca:splash2x_radix',
            'vips': 'anakli/cca:parsec_vips',
        }
        
        # Format the list of cores as a Docker-compatible string
        cpuset = ','.join(map(str, cores))
        
        # Get the appropriate Docker image
        image = image_mapping.get(job_name.lower())
        if not image:
            return None
        
        # Format the command to run the PARSEC benchmark
        command = f"./run -a run -S {('splash2x' if job_name.lower() == 'radix' else 'parsec')} -p {job_name.lower()} -i native -n {threads}"
        
        # Run the container
        container = client.containers.run(
            image=image,
            command=command,
            detach=True,
            name=f"parsec_{job_name.lower()}",
            cpuset_cpus=cpuset
        )
        
        
        return container
    except Exception as e:
        print(f"Error starting job {job_name}: {str(e)}")
        return None


def get_container_cores(container: Union[Container, str]) -> Optional[List[int]]:
    """
    Get the CPU cores currently assigned to a container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
    
    Returns:
        Optional[List[int]]: List of CPU core IDs assigned to the container or None if failed
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return None
        
        container.reload()  # Refresh container state
        cpuset = container.attrs['HostConfig']['CpusetCpus']
        
        # If cpuset is empty, return None
        if not cpuset:
            return None
            
        # Parse the cpuset string (e.g., "0,1,2" or "0-2,4") into a list of integers
        cores = []
        for segment in cpuset.split(','):
            if '-' in segment:
                # Handle range (e.g., "0-3")
                start, end = map(int, segment.split('-'))
                cores.extend(range(start, end + 1))
            else:
                # Handle single value
                cores.append(int(segment))
        
        return cores
    except Exception as e:
        print(f"Error getting container cores: {str(e)}")
        return None 


def update_container_cores(
    container: Union[Container, str], 
    cores: List[int], 
    job_name: str
) -> bool:
    """
    Update the CPU cores assigned to a container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
        cores (List[int]): New list of CPU core IDs to use
        job_name (str): Name of the job
        logger (SchedulerLogger): Logger to log job events
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        # Map job_name to Job enum
        job_enum = getattr(Job, job_name.upper())
        
        # Format the list of cores as a Docker-compatible string
        cpuset = ','.join(map(str, cores))
        
        # Update the container's CPU set
        container.update(cpuset_cpus=cpuset)
        
        return True
    except Exception as e:
        print(f"Error updating cores for {job_name}: {str(e)}")
        return False


def pause_container(container: Union[Container, str], job_name: str) -> bool:
    """
    Pause a running container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
        job_name (str): Name of the job
        logger (SchedulerLogger): Logger to log job events
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        # Map job_name to Job enum
        job_enum = getattr(Job, job_name.upper())
        
        # Pause the container
        container.pause()
        
        return True
    except Exception as e:
        print(f"Error pausing job {job_name}: {str(e)}")
        return False


def unpause_container(container: Union[Container, str], job_name: str) -> bool:
    """
    Unpause a paused container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
        job_name (str): Name of the job
        logger (SchedulerLogger): Logger to log job events
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        # Map job_name to Job enum
        job_enum = getattr(Job, job_name.upper())
        
        # Unpause the container
        container.unpause()
        
        return True
    except Exception as e:
        print(f"Error unpausing job {job_name}: {str(e)}")
        return False


def stop_container(container: Union[Container, str], job_name: str) -> bool:
    """
    Stop a running container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
        job_name (str): Name of the job
        logger (SchedulerLogger): Logger to log job events
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        # Map job_name to Job enum
        job_enum = getattr(Job, job_name.upper())
        
        # Stop the container
        container.stop(timeout=10)  # Allow 10 seconds for graceful shutdown
        
        return True
    except Exception as e:
        print(f"Error stopping job {job_name}: {str(e)}")
        return False


def remove_container(container: Union[Container, str], job_name: str, force: bool = False) -> bool:
    """
    Remove a container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
        job_name (str): Name of the job
        force (bool): Force removal of the container
        logger (SchedulerLogger): Logger to log job events
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        # Map job_name to Job enum
        job_enum = getattr(Job, job_name.upper())
        
        # Remove the container
        container.remove(force=force)
        
        return True
    except Exception as e:
        print(f"Error removing job {job_name}: {str(e)}")
        return False


def get_container_logs(container: Union[Container, str], tail: int = 100) -> str:
    """
    Get the logs from a container.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
        tail (int): Number of lines to retrieve from the end
    
    Returns:
        str: Container logs
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return ""
        
        return container.logs(tail=tail).decode('utf-8')
    except Exception as e:
        print(f"Error getting logs: {str(e)}")
        return ""


def is_container_running(container: Union[Container, str]) -> bool:
    """
    Check if a container is running.
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
    
    Returns:
        bool: True if running, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        container.reload()  # Refresh container state
        return container.status == 'running'
    except Exception:
        return False


def is_container_exited(container: Union[Container, str]) -> bool:
    """
    Check if a container has exited (completed or failed).
    
    Args:
        container (Union[Container, str]): Docker container object or container ID/name
    
    Returns:
        bool: True if exited, False otherwise
    """
    try:
        # Get Container object if a string was provided
        if isinstance(container, str):
            container = get_container_by_id_or_name(container)
            if not container:
                return False
        
        container.reload()  # Refresh container state
        return container.status == 'exited'
    except Exception:
        return False


def get_all_containers() -> List[Container]:
    """
    Get all containers.
    
    Returns:
        List[Container]: List of container objects
    """
    try:
        return client.containers.list(all=True)
    except Exception as e:
        print(f"Error listing containers: {str(e)}")
        return []


def get_container_stats(container_id_or_name: str) -> Optional[Dict]:
    """
    Get resource usage statistics for a Docker container.
    
    Args:
        container_id_or_name (str): ID or name of the Docker container
    
    Returns:
        Optional[Dict]: Dictionary with container stats or None if failed
    """
    try:
        cmd = f"docker stats {container_id_or_name} --no-stream --format '{{{{.CPUPerc}}}} {{{{.MemUsage}}}}'"
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

