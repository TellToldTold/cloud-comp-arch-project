#!/usr/bin/env python3

import time
import sys
import argparse
import signal
import os
from typing import Dict, List, Set, Optional, Tuple
import subprocess

# Import our modules
import resource_monitor as rm
import container_manager as cm
import memcached_manager as mm
from scheduler_logger import SchedulerLogger, Job


class SchedulerController:
    def __init__(self, memcached_ip: str, memcached_port: int = 11211):
        """
        Initialize the scheduler controller.
        
        Args:
            memcached_ip (str): IP address of the memcached server
            memcached_port (int): Port of the memcached server
        """
        self.logger = SchedulerLogger()
        self.memcached_ip = memcached_ip
        self.memcached_port = memcached_port
        
        # Track running jobs
        self.running_jobs: Dict[str, Tuple[object, List[int], int]] = {}  # job_name -> (container, cores, threads)
        
        # Track completed jobs
        self.completed_jobs: Set[str] = set()
        
        # Track currently allocated cores
        self.allocated_cores: Dict[str, List[int]] = {}  # job_name -> list of cores
        
        # Get total number of cores available
        self.total_cores = len(rm.get_cpu_usage_per_core())
        print(f"Total cores available: {self.total_cores}")
        
        # Set up memcached
        self.memcached_pid = mm.get_memcached_pid()
        if not self.memcached_pid:
            print("Error: Memcached process not found!")
            self.logger.custom_event(Job.SCHEDULER, "Memcached process not found on startup")
        else:
            # Get initial memcached affinity
            self.memcached_cores = mm.get_memcached_cpu_affinity()
            if not self.memcached_cores:
                self.memcached_cores = [0]  # Default to core 0
                self.set_memcached_cores(self.memcached_cores)
            
            self.logger.job_start(Job.MEMCACHED, self.memcached_cores, len(self.memcached_cores))
            self.allocated_cores['memcached'] = self.memcached_cores
            print(f"Memcached running on cores: {self.memcached_cores}")
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """Handle termination signals for clean shutdown."""
        print("\nShutting down scheduler...")
        self.cleanup()
        sys.exit(0)
    
    def set_memcached_cores(self, cores: List[int]) -> bool:
        """
        Set the CPU cores for memcached to use.
        
        Args:
            cores (List[int]): List of core IDs
        
        Returns:
            bool: True if successful, False otherwise
        """
        success = mm.set_memcached_affinity(cores, self.logger)
        if success:
            self.memcached_cores = cores
            self.allocated_cores['memcached'] = cores
        return success
    
    def start_batch_job(self, job_name: str, cores: List[int], threads: int) -> bool:
        """
        Start a batch job with specified resources.
        
        Args:
            job_name (str): Name of the job
            cores (List[int]): List of core IDs to use
            threads (int): Number of threads to use
        
        Returns:
            bool: True if successful, False otherwise
        """
        if job_name in self.running_jobs:
            print(f"Job {job_name} is already running")
            return False
        
        if job_name in self.completed_jobs:
            print(f"Job {job_name} has already completed")
            return False
        
        # Check if any of the requested cores are already allocated
        for core in cores:
            for job, allocated_cores in self.allocated_cores.items():
                if core in allocated_cores and job != 'memcached':  # Memcached can share cores with batch jobs
                    print(f"Core {core} is already allocated to job {job}")
                    return False
        
        # Start the job
        container = cm.run_batch_job(job_name, cores, threads, self.logger)
        if container:
            self.running_jobs[job_name] = (container, cores, threads)
            self.allocated_cores[job_name] = cores
            print(f"Started job {job_name} on cores {cores} with {threads} threads")
            return True
        else:
            print(f"Failed to start job {job_name}")
            return False
    
    def update_job_cores(self, job_name: str, new_cores: List[int]) -> bool:
        """
        Update the CPU cores for a running job.
        
        Args:
            job_name (str): Name of the job
            new_cores (List[int]): New list of core IDs
        
        Returns:
            bool: True if successful, False otherwise
        """
        if job_name not in self.running_jobs:
            print(f"Job {job_name} is not running")
            return False
        
        # Check if any of the requested cores are already allocated to other jobs
        for core in new_cores:
            for job, allocated_cores in self.allocated_cores.items():
                if job != job_name and core in allocated_cores and job != 'memcached':
                    print(f"Core {core} is already allocated to job {job}")
                    return False
        
        # Update the job's cores
        container, _, threads = self.running_jobs[job_name]
        
        if job_name == 'memcached':
            success = self.set_memcached_cores(new_cores)
        else:
            success = cm.update_container_cores(container, new_cores, job_name, self.logger)
        
        if success:
            self.running_jobs[job_name] = (container, new_cores, threads)
            self.allocated_cores[job_name] = new_cores
            print(f"Updated job {job_name} to use cores {new_cores}")
            return True
        else:
            print(f"Failed to update cores for job {job_name}")
            return False
    
    def pause_job(self, job_name: str) -> bool:
        """
        Pause a running job.
        
        Args:
            job_name (str): Name of the job
        
        Returns:
            bool: True if successful, False otherwise
        """
        if job_name not in self.running_jobs:
            print(f"Job {job_name} is not running")
            return False
        
        if job_name == 'memcached':
            print("Cannot pause memcached")
            return False
        
        container, cores, threads = self.running_jobs[job_name]
        success = cm.pause_container(container, job_name, self.logger)
        
        if success:
            print(f"Paused job {job_name}")
            return True
        else:
            print(f"Failed to pause job {job_name}")
            return False
    
    def unpause_job(self, job_name: str) -> bool:
        """
        Unpause a paused job.
        
        Args:
            job_name (str): Name of the job
        
        Returns:
            bool: True if successful, False otherwise
        """
        if job_name not in self.running_jobs:
            print(f"Job {job_name} is not running")
            return False
        
        container, cores, threads = self.running_jobs[job_name]
        success = cm.unpause_container(container, job_name, self.logger)
        
        if success:
            print(f"Unpaused job {job_name}")
            return True
        else:
            print(f"Failed to unpause job {job_name}")
            return False
    
    def stop_job(self, job_name: str) -> bool:
        """
        Stop a running job.
        
        Args:
            job_name (str): Name of the job
        
        Returns:
            bool: True if successful, False otherwise
        """
        if job_name not in self.running_jobs:
            print(f"Job {job_name} is not running")
            return False
        
        if job_name == 'memcached':
            print("Cannot stop memcached")
            return False
        
        container, cores, threads = self.running_jobs[job_name]
        success = cm.stop_container(container, job_name, self.logger)
        
        if success:
            self.completed_jobs.add(job_name)
            del self.running_jobs[job_name]
            if job_name in self.allocated_cores:
                del self.allocated_cores[job_name]
            print(f"Stopped job {job_name}")
            return True
        else:
            print(f"Failed to stop job {job_name}")
            return False
    
    def check_job_status(self):
        """
        Check the status of all running jobs and update internal state.
        """
        jobs_to_remove = []
        
        for job_name, (container, cores, threads) in self.running_jobs.items():
            if job_name == 'memcached':
                continue  # Skip memcached
            
            if cm.is_container_completed(container):
                print(f"Job {job_name} has completed successfully")
                self.logger.job_end(getattr(Job, job_name.upper()))
                self.completed_jobs.add(job_name)
                jobs_to_remove.append(job_name)
            elif cm.is_container_exited(container) and not cm.is_container_completed(container):
                print(f"Job {job_name} has failed")
                self.logger.job_end(getattr(Job, job_name.upper()))
                self.completed_jobs.add(job_name)
                jobs_to_remove.append(job_name)
        
        # Remove completed jobs from running_jobs
        for job_name in jobs_to_remove:
            del self.running_jobs[job_name]
            if job_name in self.allocated_cores:
                del self.allocated_cores[job_name]
    
    def get_available_cores(self) -> List[int]:
        """
        Get a list of cores that are not currently allocated to batch jobs.
        
        Returns:
            List[int]: List of available core IDs
        """
        all_cores = set(range(self.total_cores))
        batch_allocated = set()
        
        for job, cores in self.allocated_cores.items():
            if job != 'memcached':  # Ignore memcached allocation for this purpose
                batch_allocated.update(cores)
        
        return sorted(list(all_cores - batch_allocated))
    
    def get_job_status(self) -> Dict:
        """
        Get the status of all jobs.
        
        Returns:
            Dict: Dictionary with job status information
        """
        status = {
            'running_jobs': list(self.running_jobs.keys()),
            'completed_jobs': list(self.completed_jobs),
            'allocated_cores': self.allocated_cores,
            'available_cores': self.get_available_cores(),
            'memcached_cores': self.memcached_cores
        }
        return status
    
    def cleanup(self):
        """
        Clean up resources and stop all running jobs.
        """
        print("Cleaning up resources...")
        
        # Stop all running batch jobs
        for job_name, (container, _, _) in list(self.running_jobs.items()):
            if job_name != 'memcached':
                try:
                    self.stop_job(job_name)
                except Exception as e:
                    print(f"Error stopping job {job_name}: {str(e)}")
        
        # End the logger
        self.logger.end()
        print("Cleanup complete")


def main():
    """
    Main function to run the scheduler.
    """
    parser = argparse.ArgumentParser(description="Dynamic Resource Scheduler for PARSEC and Memcached")
    parser.add_argument("--memcached-ip", required=True, help="IP address of the memcached server")
    parser.add_argument("--memcached-port", type=int, default=11211, help="Port of the memcached server")
    args = parser.parse_args()
    
    # Create the scheduler controller
    controller = SchedulerController(args.memcached_ip, args.memcached_port)
    
    # Example: Start a batch job on available cores
    available_cores = controller.get_available_cores()
    if available_cores:
        controller.start_batch_job("blackscholes", available_cores[:2], 2)
    
    try:
        # Main loop: Monitor and adjust resources
        while True:
            # Check job status
            controller.check_job_status()
            
            # Example: Print job status every 5 seconds
            print(f"Job status: {controller.get_job_status()}")
            
            # Sleep for a while
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        controller.cleanup()


if __name__ == "__main__":
    main() 