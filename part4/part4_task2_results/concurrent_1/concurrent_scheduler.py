#!/usr/bin/env python3

import time
import os
import datetime
from typing import Dict, Optional, List
from container_manager import (
    run_batch_job, 
    is_container_running, 
    is_container_completed,
    is_container_exited,
    update_container_cores,
    get_all_containers,
    stop_container,
    remove_container
)
from memcached_manager import (
    get_memcached_affinity,
    set_memcached_affinity,
    get_memcached_worker_thread_ids,
    get_memcached_cpu_percent

)
from resource_monitor import get_cpu_usage_per_core
from scheduler_logger import SchedulerLogger, Job
from jobs_timer import JobsTimer

# List of batch jobs to run
BATCH_JOBS = [
    "freqmine",
    "ferret",
    "dedup", 
    "vips",
    "canneal",
    "blackscholes",
    "radix",
]


# Thresholds for CPU usage
HIGH_THRESHOLD_ONLY_CORE0 = 90.0  # Moving from ONLY_CORE0 to COLOCATED
LOW_THRESHOLD_COLOCATED = 50.0   # Moving from COLOCATED to ONLY_CORE0
HIGH_THRESHOLD_COLOCATED = 80.0  # Moving from COLOCATED to DEDICATED_TWO_CORES
LOW_THRESHOLD_DEDICATED_TWO_CORES = 50.0   # Moving from DEDICATED_TWO_CORES to COLOCATED

# Colocation states
MEMCACHED_ONLY_CORE0 = "memcached_only_core0"           # Memcached on core 0, containers on 2
MEMCACHED_COLOCATED = "memcached_colocated"             # Memcached on cores 0,1, containers on 2
MEMCACHED_DEDICATED_TWO_CORES = "memcached_two_cores"   # Memcached on cores 0,1, containers on 2

# Output log file
OUTPUT_LOG_FILE = "dynamic_scheduler_output.log"

def log_message(message, log_file=OUTPUT_LOG_FILE):
    timestamp = time.time()
    datetime_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # Format with fixed width timestamp field for consistent indentation
    formatted_message = f"[{datetime_str} | {timestamp:.6f}] {message}"
    
    # Print to console
    print(formatted_message)
    
    # Write to log file
    with open(log_file, 'a') as f:
        f.write(formatted_message + "\n")

def main():
    with open(OUTPUT_LOG_FILE, 'w') as f:
        f.write(f"Dynamic Scheduler started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
    
    # Initialize jobs timer
    timer = JobsTimer()

    # Initialize logger
    logger = SchedulerLogger(scheduler_name = "dynamic_scheduler")
    
    try:
        log_message("Cleaning up existing containers...")
        existing_containers = get_all_containers()
        for container in existing_containers:
            stop_container(container)
            remove_container(container)

        # Set initial memcached CPU affinity to core 0
        log_message("Setting initial memcached affinity to core 0")
        memcached_cores = [0]
        set_memcached_affinity(memcached_cores)
        
        logger.job_start(Job.MEMCACHED, memcached_cores, 2)
        
        # Initial batch job cores - cores 1, 2, 3
        batch_cores = [1, 2, 3]
        log_message(f"Initial batch_cores: {batch_cores}")
        
        # Track current colocation state
        current_state = MEMCACHED_ONLY_CORE0
        
        # Keep track of running jobs and their containers
        running_jobs = []  # List of (job_name, container, cores, threads) tuples
        
        for job_name in BATCH_JOBS:
            log_message(f"Starting {job_name} with 2 threads on cores {batch_cores}")
            container = run_batch_job(job_name, batch_cores, 2)
            
            timer.start(job_name)
            logger.job_start(Job(job_name), batch_cores, 2)
            running_jobs.append((job_name, container, batch_cores, 2))
            log_message(f"Job {job_name} started successfully")
            
            # Small delay between job starts to avoid resource contention during startup
            time.sleep(0.1)
        
        # Jobs that have been moved off core 1
        jobs_moved_off_core1 = []
    
        # Main monitoring loop
        while running_jobs:
            # Check CPU usage on core 0
            cpu_usage = get_cpu_usage_per_core()
            
            core0_usage = cpu_usage[0]
            log_message(f"Core 0 usage: {core0_usage:.1f}%")
            
            # Check if we need to adjust core allocation based on current state
            if current_state == MEMCACHED_ONLY_CORE0 and core0_usage > HIGH_THRESHOLD_ONLY_CORE0:
                log_message("High memcached usage detected, scaling up to cores 0,1 (colocated)")
                memcached_cores = [0, 1]
                set_memcached_affinity(memcached_cores)
                
                # Update state
                current_state = MEMCACHED_COLOCATED
                
                # Log the change
                logger.update_cores(Job.MEMCACHED, memcached_cores)
                logger.custom_event(Job.MEMCACHED, "colocated_with_jobs_on_core1")
            
            elif current_state == MEMCACHED_COLOCATED:
                if core0_usage < LOW_THRESHOLD_COLOCATED:
                    if len(jobs_moved_off_core1) == 0:
                        # Scale down memcached back to core 0 only
                        log_message("Low memcached usage detected, scaling down to core 0 only")
                        memcached_cores = [0]
                        set_memcached_affinity(memcached_cores)
                        
                        # Update state
                        current_state = MEMCACHED_ONLY_CORE0
                        
                        # Log the change
                        logger.update_cores(Job.MEMCACHED, memcached_cores)
                        logger.custom_event(Job.MEMCACHED, "removed_from_core1")
                    else:
                        # Move one job back to core 1
                        job_to_move = jobs_moved_off_core1.pop()
                        for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                            if job_name == job_to_move:
                                log_message(f"Moving job {job_name} back to core 1")
                                new_cores = [1, 2, 3]
                                update_container_cores(container, new_cores)
                                running_jobs[i] = (job_name, container, new_cores, threads)
                                
                                # Log the change
                                logger.update_cores(Job(job_name), new_cores)
                                logger.custom_event(Job(job_name), "expanded_to_core1")

                                break
                
                elif core0_usage > HIGH_THRESHOLD_COLOCATED:
                    # Move one job off core 1
                    for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                        
                        if job_cores[0] == 1:
                            log_message(f"Moving job {job_name} off core 1")
                            new_cores = [2, 3]
                            update_container_cores(container, new_cores)
                            running_jobs[i] = (job_name, container, new_cores, threads)
                            
                            # Log the change
                            logger.update_cores(Job(job_name), new_cores)
                            logger.custom_event(Job(job_name), "moved_off_core1")
                            
                            # Mark this job as moved
                            jobs_moved_off_core1.append(job_name)
                        # Only move one job at a time
                        break

                    # If all jobs are moved off core 1, update state
                    if len(jobs_moved_off_core1) == len(running_jobs):
                        current_state = MEMCACHED_DEDICATED_TWO_CORES
                            
            
            elif current_state == MEMCACHED_DEDICATED_TWO_CORES and core0_usage < LOW_THRESHOLD_DEDICATED_TWO_CORES:
                # Scale back to colocated state
                log_message("Low memcached usage detected, returning to colocated state")
                
                job_to_move = jobs_moved_off_core1.pop()
                
                for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                    if job_name == job_to_move:
                        log_message(f"Moving job {job_name} back to core 1")
                        new_cores = [1, 2, 3]
                        update_container_cores(container, new_cores)
                        running_jobs[i] = (job_name, container, new_cores, threads)
                        
                        # Log the change
                        logger.update_cores(Job(job_name), new_cores)
                        logger.custom_event(Job(job_name), "expanded_to_core1")

                        current_state = MEMCACHED_COLOCATED
                        break
            
            # Check for completed jobs
            i = 0
            
            while i < len(running_jobs):
                job_name, container, job_cores, threads = running_jobs[i]
                
                if not is_container_running(container):
                    # Job completed, remove from running jobs
                    log_message(f"Job {job_name} completed")
                    running_jobs.pop(i)
                    
                    # Remove from jobs_moved_off_core1 list if it's there
                    if job_name in jobs_moved_off_core1:
                        jobs_moved_off_core1.remove(job_name)
                    
                    # Stop the timer and log completion
                    timer.stop(job_name)
                    logger.job_end(Job(job_name))
                    job_time = timer.get_job_time(job_name)
                    logger.custom_event(
                        Job(job_name),
                        f"execution_time_{job_time:.2f}_seconds"
                    )
                    
                    # Clean up the container
                    if is_container_exited(container):
                        try:
                            container.remove()
                        except Exception as e:
                            log_message(f"Error removing container: {str(e)}")
                else:
                    i += 1
            
            # Wait before checking again
            time.sleep(0.8)
        
        # All jobs completed
        log_message("All batch jobs completed.")

        # Log total execution time
        total_time = timer.get_total_time()
        logger.custom_event(
            Job.ALL,
            f"total_execution_time_{total_time:.2f}_seconds"
        )
    
    except KeyboardInterrupt:
        log_message("Controller interrupted")
    except Exception as e:
        log_message(f"Error: {str(e)}")
    finally:
        # End the scheduler
        logger.end()
        log_message("Dynamic scheduler completed")

if __name__ == "__main__":
    main() 