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
    "blackscholes",
    "canneal", 
    "dedup", 
    "radix",
    "vips"
]


# Thresholds for CPU usage
HIGH_THRESHOLD_SINGLE_CORE = 88.0  # When to scale up memcached cores
LOW_THRESHOLD_SINGLE_CORE = 50.0   # When to scale down memcached cores
HIGH_THRESHOLD_TWO_CORES = 75.0  # When to scale up memcached cores
LOW_THRESHOLD_TWO_CORES = 50.0   # When to scale down memcached cores

# Colocation states
MEMCACHED_ONLY_CORE0 = "memcached_only_core0"           # Memcached on core 0, containers on 2
MEMCACHED_COLOCATED = "memcached_colocated"             # Memcached on cores 0,1, containers on 2
MEMCACHED_DEDICATED_TWO_CORES = "memcached_two_cores"   # Memcached on cores 0,1, containers on 2
SPECIAL_JOBS_COMPLETED = "special_jobs_completed"       # Special jobs done, use core 3 for regular jobs

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
        
        # Initial regular job cores - just core 2
        batch_cores = [1, 2, 3]
        log_message(f"Initial batch_cores: {batch_cores}")
        
        # Track current colocation state
        current_state = MEMCACHED_ONLY_CORE0
        
        # Keep track of running jobs and their containers
        running_jobs = []  # List of (job_name, container, cores, threads) tuples
        job_queue = list(BATCH_JOBS)  # Copy the regular jobs list
        
        log_message(f"Starting Freqmine with 2 threads on cores {batch_cores}")
        freqmine_container = run_batch_job('freqmine', batch_cores, 2)
        
        timer.start('freqmine')
        logger.job_start(Job('freqmine'), batch_cores, 2)
        running_jobs.append(('freqmine', freqmine_container, batch_cores, 2))
        log_message(f"Special job freqmine started successfully")
        
        log_message(f"Starting ferret with 2 threads on cores {batch_cores}")
        ferret_container = run_batch_job('ferret', batch_cores, 2)
    
        timer.start('ferret')
        logger.job_start(Job('ferret'), batch_cores, 2)
        running_jobs.append(('ferret', ferret_container, batch_cores, 2))
        log_message(f"Special job ferret started successfully")
    
        # Main monitoring loop
        while running_jobs or job_queue:
            # Check CPU usage on core 0
            cpu_usage = get_cpu_usage_per_core()
            
            core0_usage = cpu_usage[0]
            log_message(f"Core 0 usage: {core0_usage:.1f}%")
            
            # Check if we need to adjust core allocation based on current state
            if current_state == MEMCACHED_ONLY_CORE0 and core0_usage > HIGH_THRESHOLD_SINGLE_CORE:
                log_message("High memcached usage detected, scaling up to cores 0,1 (colocated)")
                memcached_cores = [0, 1]
                set_memcached_affinity(memcached_cores)
                
                # Update state
                current_state = MEMCACHED_COLOCATED
                
                # Log the change
                logger.update_cores(Job.MEMCACHED, memcached_cores)
                logger.custom_event(Job.MEMCACHED, "colocated_with_jobs_on_core1")
            
            elif current_state == MEMCACHED_COLOCATED:
                if core0_usage < LOW_THRESHOLD_SINGLE_CORE:
                    # Scale down memcached back to core 0 only
                    log_message("Low memcached usage detected, scaling down to core 0 only")
                    memcached_cores = [0]
                    set_memcached_affinity(memcached_cores)
                    
                    # Update state
                    current_state = MEMCACHED_ONLY_CORE0
                    
                    # Log the change
                    logger.update_cores(Job.MEMCACHED, memcached_cores)
                    logger.custom_event(Job.MEMCACHED, "removed_from_core1")
                
                elif core0_usage > HIGH_THRESHOLD_TWO_CORES:
                    for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                        log_message(f"Moving job {job_name} off core 1")
                        new_cores = [2, 3]
                        update_container_cores(container, new_cores)
                        running_jobs[i] = (job_name, container, new_cores, threads)
                        
                        # Log the change
                        logger.update_cores(Job(job_name), new_cores)
                        logger.custom_event(Job(job_name), "moved_off_core1")
                        
                        # Update state if no more batch jobs on core 1
                        if all(job_cores[0] != 1 for _, _, job_cores, _ in running_jobs):
                            current_state = MEMCACHED_DEDICATED_TWO_CORES

                        # Break after moving the first job to move jobs one by one
                        break
            
            elif current_state == MEMCACHED_DEDICATED_TWO_CORES and core0_usage < LOW_THRESHOLD_TWO_CORES:
                # Scale back to colocated state
                log_message("Low memcached usage detected, returning to colocated state")
                
                # Move regular jobs back to use core 1 and 2
                for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                    log_message(f"Moving job {job_name} on core 1")
                    new_cores = [1,2,3]
                    update_container_cores(container, new_cores)
                    running_jobs[i] = (job_name, container, new_cores, threads)
                    
                    # Log the change
                    logger.update_cores(Job(job_name), new_cores)
                    logger.custom_event(Job(job_name), "expanded_to_core1")
                
                # Update state
                current_state = MEMCACHED_COLOCATED
            
            # Check for completed jobs and start new ones
            i = 0
            
            while i < len(running_jobs):
                job_name, container, job_cores, threads = running_jobs[i]
                
                if not is_container_running(container):
                    # Job completed, remove from running jobs
                    log_message(f"Job {job_name} completed")
                    running_jobs.pop(i)
                    
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
                    
                    # If it was a special job, start the next special job if available
                    if job_queue:
                        next_job = job_queue.pop(0)
                        cores_to_use = [1,2,3]
                        if current_state == MEMCACHED_DEDICATED_TWO_CORES:
                            cores_to_use = [2,3]
                            
                        log_message(f"Starting next regular job: {next_job} with 2 threads on cores {cores_to_use}")
                        next_container = run_batch_job(next_job, cores_to_use, 4)
                        
                        timer.start(next_job)
                        logger.job_start(Job(next_job), cores_to_use, 4)
                        running_jobs.append((next_job, next_container, cores_to_use, 4))
                        log_message(f"Job {next_job} started successfully")
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