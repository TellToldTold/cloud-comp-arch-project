#!/usr/bin/env python3

import time
import sys
import os
import subprocess
from typing import List, Dict, Optional, Set
from container_manager import (
    run_batch_job, 
    is_container_running, 
    is_container_completed,
    is_container_exited, 
    stop_container
)
from memcached_manager import (
    get_memcached_thread_affinity, 
    get_memcached_affinity,
    set_memcached_affinity,
    get_memcached_cpu_percent,
    get_memcached_thread_ids
)
from resource_monitor import get_cpu_usage_per_core
from scheduler_logger import SchedulerLogger, Job

# List of batch jobs to run
BATCH_JOBS = [
    "blackscholes",
    "canneal", 
    "dedup", 
    "ferret", 
    "freqmine", 
    "radix",
    "vips"
]

def get_local_ip():
    """Get the local IP address using hostname command"""
    try:
        output = subprocess.check_output("hostname -I | awk '{print $1}'", shell=True, text=True).strip()
        return output
    except subprocess.CalledProcessError:
        return "127.0.0.1"  # Fallback to localhost

def main():
    """Main function to run the scheduler controller."""
    # Initialize logger
    logger = SchedulerLogger(scheduler_name = "simple_sequential")
    
    try:
        # Get local IP (memcached IP)
        memcached_ip = get_local_ip()       
        print(memcached_ip)

        # Set initial memcached CPU affinity to core 0
        set_memcached_affinity([0])

        # Check initial memcached thread CPU affinity
        thread_affinity = get_memcached_thread_affinity()
        for tid, cores in thread_affinity.items():
            assert cores == [0], f"Thread {tid} should only be on core 0, but is on {cores}"

        # Get union of CPU affinity of all memcached threads
        memcached_cores = get_memcached_affinity()

        # Log start of memcached
        # TODO: Check if get_memcached_thread_ids actually returns the correct number of threads
        memcached_thread_ids = get_memcached_thread_ids()
        num_threads = len(memcached_thread_ids)
        logger.job_start(Job.MEMCACHED, memcached_cores, num_threads)
        
        
    
    except KeyboardInterrupt:
        print("Controller interrupted")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # End the scheduler
        logger.end()

if __name__ == "__main__":
    main() 