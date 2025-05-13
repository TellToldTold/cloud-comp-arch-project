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

# Special jobs to run with 2 threads on cores 2,3
SPECIAL_JOBS = ["freqmine", "ferret"]

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
        batch_cores = [1,2]
        log_message(f"Initial batch_cores: {batch_cores}")
        
        # Track current colocation state
        current_state = MEMCACHED_ONLY_CORE0
        
        # Keep track of running jobs and their containers
        running_jobs = []  # List of (job_name, container, cores, threads) tuples
        job_queue = list(BATCH_JOBS)  # Copy the regular jobs list
        special_job_queue = list(SPECIAL_JOBS)  # Copy the special jobs list
        special_job_cores = [2, 3]  # Cores for special jobs
        
        # Start first special job (freqmine)
        special_job = special_job_queue.pop(0)  # freqmine
        log_message(f"Starting special job: {special_job} with 2 threads on cores {special_job_cores}")
        special_container = run_batch_job(special_job, special_job_cores, 2)
        
        timer.start(special_job)
        logger.job_start(Job(special_job), special_job_cores, 2)
        running_jobs.append((special_job, special_container, special_job_cores, 2))
        log_message(f"Special job {special_job} started successfully")
        
        regular_job = job_queue.pop(0)
        log_message(f"Starting regular job: {regular_job} with 2 threads on cores {batch_cores}")
        regular_container = run_batch_job(regular_job, batch_cores, 2)
    
        timer.start(regular_job)
        logger.job_start(Job(regular_job), batch_cores, 2)
        running_jobs.append((regular_job, regular_container, batch_cores, 2))
        log_message(f"Regular job {regular_job} started successfully")
    
        # Main monitoring loop
        while running_jobs or job_queue or special_job_queue:
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
                    # Move regular jobs to core 2 only if they're using core 1
                    for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                        # Don't move special jobs (freqmine/ferret)
                        if job_name in SPECIAL_JOBS:
                            continue
                            
                        if 1 in job_cores:
                            log_message(f"Moving job {job_name} off core 1")
                            new_cores = batch_cores
                            new_cores.remove(1)
                            update_container_cores(container, new_cores)
                            running_jobs[i] = (job_name, container, new_cores, threads)
                            
                            # Log the change
                            logger.update_cores(Job(job_name), new_cores)
                            logger.custom_event(Job(job_name), "moved_off_core1")
                            
                            # Update state
                            current_state = MEMCACHED_DEDICATED_TWO_CORES
                            break
            
            elif current_state == MEMCACHED_DEDICATED_TWO_CORES and core0_usage < LOW_THRESHOLD_TWO_CORES:
                # Scale back to colocated state
                log_message("Low memcached usage detected, returning to colocated state")
                
                # Move regular jobs back to use core 1 and 2
                for i, (job_name, container, job_cores, threads) in enumerate(running_jobs):
                    # Don't move special jobs (freqmine/ferret)
                    if job_name in SPECIAL_JOBS:
                        continue
                        
                    if is_container_running(container) and job_cores == [2]:
                        new_cores = batch_cores
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
                    if job_name in SPECIAL_JOBS:
                        if len(special_job_queue) > 0:  # If ferret is still in queue
                            next_special_job = special_job_queue.pop(0)  # ferret
                            log_message(f"Starting next special job: {next_special_job} with 2 threads on cores {special_job_cores}")
                            next_container = run_batch_job(next_special_job, special_job_cores, 2)
                            
                            timer.start(next_special_job)
                            logger.job_start(Job(next_special_job), special_job_cores, 2)
                            running_jobs.append((next_special_job, next_container, special_job_cores, 2))
                            log_message(f"Special job {next_special_job} started successfully")
                        else:
                            # Check if this was the last special job
                            if not any(job[0] in SPECIAL_JOBS for job in running_jobs):
                                log_message("All special jobs completed, expanding regular jobs to core 3")
                                
                                # Update all running regular jobs to use core 3
                                for j, (reg_job_name, reg_container, reg_job_cores, reg_threads) in enumerate(running_jobs):
                                    if is_container_running(reg_container):
                                        new_cores = reg_job_cores + [3]
                                        batch_cores = [1, 2, 3]
                                        log_message(f"Expanding job {reg_job_name} to cores {new_cores}")
                                        update_container_cores(reg_container, new_cores)
                                        running_jobs[j] = (reg_job_name, reg_container, new_cores, reg_threads)
                                        
                                        # Log the change
                                        logger.update_cores(Job(reg_job_name), new_cores)
                                        logger.custom_event(Job(reg_job_name), "expanded_to_core3")
                                
                                # Update state
                                current_state = SPECIAL_JOBS_COMPLETED
                                
                            # Start a regular job on cores 2,3 if available
                            if job_queue:
                                next_job = job_queue.pop(0)
                                cores_to_use = batch_cores
                                if current_state == MEMCACHED_DEDICATED_TWO_CORES:
                                    cores_to_use.remove(1)

                                log_message(f"Starting regular job on cores {cores_to_use}: {next_job} with 2 threads")
                                next_container = run_batch_job(next_job, cores_to_use, 2)
                                
                                timer.start(next_job)
                                logger.job_start(Job(next_job), cores_to_use, 2)
                                running_jobs.append((next_job, next_container, cores_to_use, 2))
                                log_message(f"Job {next_job} started successfully")
                    else:
                        if job_queue:
                            next_job = job_queue.pop(0)
                            cores_to_use = batch_cores
                            if current_state == MEMCACHED_DEDICATED_TWO_CORES:
                                cores_to_use.remove(1)
                                
                            log_message(f"Starting next regular job: {next_job} with 2 threads on cores {cores_to_use}")
                            next_container = run_batch_job(next_job, cores_to_use, 2)
                            
                            timer.start(next_job)
                            logger.job_start(Job(next_job), cores_to_use, 2)
                            running_jobs.append((next_job, next_container, cores_to_use, 2))
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