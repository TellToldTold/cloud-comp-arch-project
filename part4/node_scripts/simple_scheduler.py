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
    get_memcached_cpu_affinity, 
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

        # Check initial memcached CPU affinity
        memcached_cores = get_memcached_cpu_affinity()
        for tid, cores in memcached_cores.items():
            assert cores == [0], f"Thread {tid} should only be on core 0, but is on {cores}"

        # Log start of memcached
        # TODO: Check if get_memcached_thread_ids actually returns the correct number of threads
        memcached_thread_ids = get_memcached_thread_ids()
        num_threads = len(memcached_thread_ids)
        logger.job_start(Job.MEMCACHED, memcached_cores, num_threads)
        
        # Get all available cores
        all_cores = list(range(4))  # Assuming 4 core machine
        
        # Calculate batch job cores (cores not used by memcached)
        batch_cores = [core for core in all_cores if core not in memcached_cores]
        print(batch_cores)

        # Run jobs sequentially
        for job_name in BATCH_JOBS:
            print(job_name)            
            # Map job name to Job enum
            job_enum = getattr(Job, job_name.upper())
            
            # Run the batch job with all available non-memcached cores
            container = run_batch_job(job_name, batch_cores, len(batch_cores))
            if not container:
                print("Failed to start job")
                continue
                
            # Log job start
            logger.job_start(job_enum, batch_cores, len(batch_cores))
            
            # Wait for job to complete, periodically checking CPU usage
            check_interval = 10  # seconds
            while is_container_running(container):
                # Check memcached CPU usage
                memcached_cpu_percent = get_memcached_cpu_percent()
                per_core_usage = get_memcached_cpu_percent_per_core()
                
                print("Memcached CPU usage:")
                print(memcached_cpu_percent)
                print(per_core_usage)
                
                # Sleep for the check interval
                time.sleep(check_interval)
            
            # Log job completion
            if is_container_completed(container):
                print("Job completed")
            else:
                print("Job exited with errors")
            
            logger.job_end(job_enum)
            
            # Clean up the container (just to be sure)
            if is_container_exited(container):
                try:
                    container.remove()
                except Exception as e:
                    print(f"Error removing container: {str(e)}")
        
        print("All jobs completed")
    
    except KeyboardInterrupt:
        print("Controller interrupted")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # End the scheduler
        logger.end()

if __name__ == "__main__":
    main() 