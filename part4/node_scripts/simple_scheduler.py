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
    get_all_containers,
    stop_container,
    remove_container
)
from memcached_manager import (
    get_memcached_affinity,
    set_memcached_affinity,
    get_memcached_worker_thread_ids
)
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

def main():
    """Main function to run the scheduler controller."""
    # Initialize logger
    logger = SchedulerLogger(scheduler_name = "simple_sequential")
    
    try:
        # Clean up any existing containers
        print("Cleaning up existing containers...")
        existing_containers = get_all_containers()
        for container in existing_containers:
            stop_container(container)
            remove_container(container)

        # Set memcached CPU affinity to cores 0-1
        memcached_cores = [0, 1]
        print(f"Setting memcached affinity to cores {memcached_cores}")
        set_memcached_affinity(memcached_cores)

        # Verify memcached cores
        actual_memcached_cores = get_memcached_affinity()
        print(f"memcached_cores: {actual_memcached_cores}")

        # Log start of memcached
        memcached_thread_ids = get_memcached_worker_thread_ids()
        num_memcached_threads = len(memcached_thread_ids)
        logger.job_start(Job.MEMCACHED, memcached_cores, num_memcached_threads)
        
        # Set batch job cores to 2-3
        batch_cores = [2, 3]
        print(f"batch_cores: {batch_cores}")

        # Run all batch jobs sequentially
        for job_name in BATCH_JOBS:
            print(f"Running job: {job_name}...")

            # Always use 2 threads for batch jobs
            num_threads = 2
                
            # Run the batch job
            container = run_batch_job(job_name, batch_cores, num_threads)
            if not container:
                print("Failed to start job")
                continue

            # Log job start
            logger.job_start(Job(job_name), batch_cores, num_threads)
            
            # Wait for the job to finish
            while is_container_running(job_name):
                time.sleep(1)
            
            # Check if the job completed successfully
            logger.job_end(Job(job_name))
            if is_container_completed(container):
                print(f"Job {job_name} completed successfully.")
            elif is_container_exited(container):
                print(f"Job {job_name} exited with error.")
            else:
                print(f"Job {job_name} is not running but has an unknown state.")

            try:
                container.remove()
            except Exception as e:
                print(f"Error removing container: {str(e)}")

        # All jobs completed
        print("All batch jobs completed.")
    
    except KeyboardInterrupt:
        print("Controller interrupted")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # End the scheduler
        logger.end()

if __name__ == "__main__":
    main() 