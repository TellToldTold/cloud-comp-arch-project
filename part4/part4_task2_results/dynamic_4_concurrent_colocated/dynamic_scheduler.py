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
    "ferret", 
    "freqmine", 
    "radix",
    "vips"
]

# Thresholds for CPU usage
HIGH_THRESHOLD = 90.0  # When to scale up memcached cores
LOW_THRESHOLD = 60.0   # When to scale down memcached cores

# Colocation states
MEMCACHED_ONLY_CORE0 = "memcached_only_core0"           # Memcached on core 0, containers on 1,2,3
MEMCACHED_COLOCATED = "memcached_colocated"             # Memcached on cores 0,1, containers on 1,2,3
MEMCACHED_DEDICATED_TWO_CORES = "memcached_two_cores"   # Memcached on cores 0,1, containers on 2,3

# Output log file
OUTPUT_LOG_FILE = "dynamic_scheduler_output.log"

def log_message(message, log_file=OUTPUT_LOG_FILE):
    """
    Log a message to both console and file with timestamp.
    
    Args:
        message (str): Message to log
        log_file (str): Path to log file
    """
    # Get current time
    timestamp = time.time()
    datetime_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # Format with fixed width timestamp field for consistent indentation
    formatted_message = f"[{datetime_str} | {timestamp:.6f}] {message}"
    
    # Print to console
    print(formatted_message)
    
    # Write to log file
    with open(log_file, 'a') as f:
        f.write(formatted_message + "\n")

def save_cpu_usage(cpu_usage: List[float], filename: str = "cpu_usage_log.csv"):
    """
    Save CPU usage per core to a CSV file with timestamps.
    
    Args:
        cpu_usage (List[float]): List of CPU usage percentages per core
        filename (str): Name of the file to save to
    """
    # Get current time
    timestamp = time.time()
    datetime_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # Check if file exists to add header
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a') as f:
        # Write header if file doesn't exist
        if not file_exists:
            header = "timestamp,datetime," + ",".join([f"core_{i}" for i in range(len(cpu_usage))])
            f.write(header + "\n")
        
        # Write data
        data = f"{timestamp},{datetime_str}," + ",".join([f"{usage:.2f}" for usage in cpu_usage])
        f.write(data + "\n")

def main():
    """Main function to run the dynamic scheduler controller."""
    # Initialize output log file
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
        
        # Log start of memcached
        memcached_thread_ids = get_memcached_worker_thread_ids()
        num_threads = len(memcached_thread_ids)
        logger.job_start(Job.MEMCACHED, memcached_cores, num_threads)
        
        # Initial batch cores (1, 2, 3)
        batch_cores = [1, 2, 3]
        log_message(f"Initial batch_cores: {batch_cores}")
        
        # Track current colocation state
        current_state = MEMCACHED_ONLY_CORE0
        
        # Keep track of running jobs and their containers
        running_jobs = []  # List of (job_name, container, cores) tuples
        job_queue = list(BATCH_JOBS)  # Copy the jobs list to use as a queue
        max_concurrent_jobs = 2  # Maximum number of jobs to run concurrently
        
        # Start initial jobs (up to max_concurrent_jobs)
        for _ in range(max_concurrent_jobs):
            if job_queue:
                job_name = job_queue.pop(0)
                log_message(f"Starting job: {job_name}")
                
                # Start the job with 4 threads on batch cores
                num_threads = 4
                container = run_batch_job(job_name, batch_cores, num_threads)
                
                if container:
                    # Record job start
                    timer.start(job_name)
                    logger.job_start(Job(job_name), batch_cores, num_threads)
                    running_jobs.append((job_name, container, batch_cores))
                    log_message(f"Job {job_name} started successfully")
                else:
                    log_message(f"Failed to start job {job_name}")
        
        # Main monitoring loop
        while running_jobs or job_queue:
            # Check CPU usage on core 0
            cpu_usage = get_cpu_usage_per_core()
            
            # Save CPU usage to file
            save_cpu_usage(cpu_usage)
            
            core0_usage = cpu_usage[0]
            log_message(f"Core 0 usage: {core0_usage:.1f}%")
            
            # Check if we need to adjust core allocation based on current state
            if current_state == MEMCACHED_ONLY_CORE0 and core0_usage > HIGH_THRESHOLD:
                # Scale up memcached to include core 1 (colocated with jobs)
                log_message("High memcached usage detected, scaling up to cores 0,1 (colocated)")
                memcached_cores = [0, 1]
                set_memcached_affinity(memcached_cores)
                
                # Jobs keep cores 1,2,3
                # No need to update container cores as they remain the same
                
                # Update state
                current_state = MEMCACHED_COLOCATED
                
                # Log the change
                logger.update_cores(Job.MEMCACHED, memcached_cores)
                logger.custom_event(Job.MEMCACHED, "colocated_with_jobs_on_core1")
            
            elif current_state == MEMCACHED_COLOCATED:
                if core0_usage < LOW_THRESHOLD:
                    # Scale down memcached back to core 0 only
                    log_message("Low memcached usage detected, scaling down to core 0 only")
                    memcached_cores = [0]
                    set_memcached_affinity(memcached_cores)
                    
                    # Update state
                    current_state = MEMCACHED_ONLY_CORE0
                    
                    # Log the change
                    logger.update_cores(Job.MEMCACHED, memcached_cores)
                    logger.custom_event(Job.MEMCACHED, "removed_from_core1")
                
                elif core0_usage > HIGH_THRESHOLD:
                    # Still high usage while colocated, move jobs off core 1 one by one
                    if len(running_jobs) > 0 and [1] in [job[2] for job in running_jobs]:
                        # Find first job that's using core 1
                        for i, (job_name, container, job_cores) in enumerate(running_jobs):
                            if 1 in job_cores:
                                log_message(f"Moving job {job_name} off core 1")
                                new_cores = [2, 3]
                                update_container_cores(container, new_cores)
                                running_jobs[i] = (job_name, container, new_cores)
                                
                                # Log the change
                                logger.update_cores(Job(job_name), new_cores)
                                logger.custom_event(Job(job_name), "moved_off_core1")
                                
                                # Update state if this was the first job moved
                                current_state = MEMCACHED_DEDICATED_TWO_CORES
                                break
            
            elif current_state == MEMCACHED_DEDICATED_TWO_CORES and core0_usage < LOW_THRESHOLD:
                # Scale back to colocated state
                log_message("Low memcached usage detected, returning to colocated state")
                
                # Move jobs back to cores 1,2,3
                for i, (job_name, container, job_cores) in enumerate(running_jobs):
                    if is_container_running(container) and job_cores == [2, 3]:
                        new_cores = [1, 2, 3]
                        update_container_cores(container, new_cores)
                        running_jobs[i] = (job_name, container, new_cores)
                        
                        # Log the change
                        logger.update_cores(Job(job_name), new_cores)
                        logger.custom_event(Job(job_name), "returned_to_core1")
                
                # Update state
                current_state = MEMCACHED_COLOCATED
            
            # Check for completed jobs and start new ones
            i = 0
            while i < len(running_jobs):
                job_name, container, job_cores = running_jobs[i]
                
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
                    
                    # Start a new job if there are any left in the queue
                    if job_queue:
                        next_job = job_queue.pop(0)
                        log_message(f"Starting next job: {next_job}")
                        
                        # Use the same cores as the job that just completed
                        next_container = run_batch_job(next_job, job_cores, 4)
                        
                        if next_container:
                            # Record job start
                            timer.start(next_job)
                            logger.job_start(Job(next_job), job_cores, 4)
                            running_jobs.append((next_job, next_container, job_cores))
                            log_message(f"Job {next_job} started successfully")
                        else:
                            log_message(f"Failed to start job {next_job}")
                else:
                    # Job still running, move to next job
                    i += 1
            
            # Wait before checking again
            time.sleep(0.5)
        
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