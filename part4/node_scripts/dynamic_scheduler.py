#!/usr/bin/env python3

import time
import os
import sys
from typing import List, Dict, Optional, Set
from container_manager import (
    run_batch_job, 
    is_container_running, 
    is_container_completed,
    is_container_exited,
    update_container_cores
)
from memcached_manager import (
    get_memcached_affinity,
    set_memcached_affinity,
    get_memcached_worker_thread_ids,
    get_memcached_cpu_percent
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

# Thresholds for CPU usage
HIGH_THRESHOLD = 90.0  # When to scale up memcached cores
LOW_THRESHOLD = 50.0   # When to scale down memcached cores

def main():
    """Main function to run the dynamic scheduler controller."""
    # Initialize logger
    logger = SchedulerLogger(scheduler_name = "dynamic_scheduler")
    
    try:
        # Set initial memcached CPU affinity to core 0
        print("[STATUS] Setting initial memcached affinity to core 0")
        memcached_cores = [0]
        set_memcached_affinity(memcached_cores)
        
        # Log start of memcached
        memcached_thread_ids = get_memcached_worker_thread_ids()
        num_threads = len(memcached_thread_ids)
        logger.job_start(Job.MEMCACHED, memcached_cores, num_threads)
        
        # All available cores
        all_cores = list(range(os.cpu_count()))
        
        # Initial batch cores (all except core 0)
        batch_cores = [core for core in all_cores if core not in memcached_cores]
        print(f"[STATUS] Initial batch_cores: {batch_cores}")
        
        # Run all batch jobs sequentially
        for job_name in BATCH_JOBS:
            print(f"[STATUS] Running job: {job_name}...")
            
            # Run the batch job
            container = run_batch_job(job_name, batch_cores, len(batch_cores))
            if not container:
                print(f"[ERROR] Failed to start job {job_name}")
                continue
            
            # Log job start
            logger.job_start(Job(job_name), batch_cores, len(batch_cores))
            
            # Wait for the job to finish, monitoring memcached CPU usage
            while is_container_running(job_name):
                # Check CPU usage on core 0
                cpu_usage = get_cpu_usage_per_core()
                core0_usage = cpu_usage[0]
                print(f"[STATUS] Core 0 usage: {core0_usage:.1f}%")
                
                # Check if we need to adjust core allocation
                if len(memcached_cores) == 1 and core0_usage > HIGH_THRESHOLD:
                    # Scale up memcached to 2 cores (0 and 1)
                    print("[STATUS] High memcached usage detected, scaling up to 2 cores")
                    memcached_cores = [0, 1]
                    set_memcached_affinity(memcached_cores)

                    # Log the change
                    logger.job_update(Job.MEMCACHED, memcached_cores, num_threads)
                    
                    # Update batch job to use remaining cores
                    batch_cores = [core for core in all_cores if core not in memcached_cores]
                    print(f"[STATUS] Updating batch cores to: {batch_cores}")
                    update_container_cores(container, batch_cores, job_name)
                    
                    # Log the change
                    logger.job_update(Job(job_name), batch_cores, len(batch_cores))
                
                elif len(memcached_cores) == 2 and core0_usage < LOW_THRESHOLD:
                    # Scale down memcached to 1 core (0)
                    print("[STATUS] Low memcached usage detected, scaling down to 1 core")
                    memcached_cores = [0]
                    set_memcached_affinity(memcached_cores)

                    # Log the change
                    logger.job_update(Job.MEMCACHED, memcached_cores, num_threads)
                    
                    # Update batch job to use remaining cores
                    batch_cores = [core for core in all_cores if core not in memcached_cores]
                    print(f"[STATUS] Updating batch cores to: {batch_cores}")
                    update_container_cores(container, batch_cores, job_name)
                    
                    # Log the change
                    logger.job_update(Job(job_name), batch_cores, len(batch_cores))
                
                # Wait before checking again
                time.sleep(2)
            
            # Check if the job completed successfully
            if is_container_completed(job_name):
                print(f"[STATUS] Job {job_name} completed successfully.")
                logger.job_end(Job(job_name))
            elif is_container_exited(job_name):
                print(f"[STATUS] Job {job_name} exited with error.")
                logger.job_end(Job(job_name))
            else:
                print(f"[STATUS] Job {job_name} is still running or has an unknown state.")
            
            # Clean up the container
            if is_container_exited(container):
                try:
                    container.remove()
                except Exception as e:
                    print(f"[STATUS] Error removing container: {str(e)}")
        
        # All jobs completed
        print("[STATUS] All batch jobs completed.")
    
    except KeyboardInterrupt:
        print("[STATUS] Controller interrupted")
    except Exception as e:
        print(f"[STATUS] Error: {str(e)}")
    finally:
        # End the scheduler
        logger.end()

if __name__ == "__main__":
    main() 