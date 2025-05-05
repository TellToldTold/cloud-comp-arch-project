#!/usr/bin/env python3

import time
import sys
import os
import argparse

# Import our modules
import resource_monitor as rm
import docker_manager as dm
from scheduler_logger import SchedulerLogger, Job


def test_resource_monitor(memcached_ip):
    """Test resource monitoring functions."""
    print("\n=== Testing Resource Monitor ===")
    
    # Test CPU usage monitoring
    print(f"CPU usage per core: {rm.get_cpu_usage_per_core()}")
    print(f"Total CPU usage: {rm.get_total_cpu_usage():.2f}%")
    
    # Test memory usage monitoring
    used_mb, total_mb = rm.get_memory_usage()
    print(f"Memory usage: {used_mb:.2f}MB / {total_mb:.2f}MB ({rm.get_memory_percent():.2f}%)")
    
    # Test memcached stats
    print(f"Memcached stats: {rm.get_memcached_stats(memcached_ip)}")
    
    # Test process monitoring
    pid = rm.get_memcached_pid()
    if pid:
        print(f"Memcached PID: {pid}")
        print(f"Memcached CPU affinity: {rm.get_process_cpu_affinity(pid)}")
        print(f"Memcached CPU usage: {rm.get_process_cpu_percent(pid):.2f}%")
    else:
        print("Memcached not found")


def test_docker_manager(memcached_ip):
    """Test docker management functions."""
    print("\n=== Testing Docker Manager ===")
    
    # Initialize logger
    logger = SchedulerLogger()
    
    try:
        # Log memcached as already running
        pid = rm.get_memcached_pid()
        if pid:
            cores = rm.get_process_cpu_affinity(pid)
            if not cores:
                cores = [0]  # Default to core 0
            logger.job_start(Job.MEMCACHED, cores, len(cores))
            print(f"Logged memcached as running on cores {cores}")
        
        # Start a batch job
        print("Starting blackscholes on core 0...")
        container = dm.run_batch_job("blackscholes", [0], 1, logger)
        if container:
            print("Blackscholes started successfully")
            
            # Wait a bit
            print("Waiting 5 seconds...")
            time.sleep(5)
            
            # Check if running
            print(f"Container running: {dm.is_container_running(container)}")
            
            # Update cores
            print("Updating to use core 1...")
            if dm.update_container_cores(container, [1], "blackscholes", logger):
                print("Updated successfully")
            else:
                print("Update failed")
            
            # Wait a bit more
            print("Waiting 5 more seconds...")
            time.sleep(5)
            
            # Pause the container
            print("Pausing container...")
            if dm.pause_container(container, "blackscholes", logger):
                print("Paused successfully")
            else:
                print("Pause failed")
            
            # Wait a bit
            print("Waiting 5 seconds...")
            time.sleep(5)
            
            # Unpause the container
            print("Unpausing container...")
            if dm.unpause_container(container, "blackscholes", logger):
                print("Unpaused successfully")
            else:
                print("Unpause failed")
            
            # Wait a bit
            print("Waiting 5 seconds...")
            time.sleep(5)
            
            # Stop the container
            print("Stopping container...")
            if dm.stop_container(container, "blackscholes", logger):
                print("Stopped successfully")
            else:
                print("Stop failed")
        else:
            print("Failed to start blackscholes")
        
        # Test memcached affinity
        print("\nTesting memcached affinity...")
        if dm.set_memcached_affinity([0, 1], logger):
            print("Set memcached affinity to cores 0,1")
        else:
            print("Failed to set memcached affinity")
        
        # Wait a bit
        print("Waiting 5 seconds...")
        time.sleep(5)
        
        # Reset memcached affinity
        if dm.set_memcached_affinity([0], logger):
            print("Reset memcached affinity to core 0")
        else:
            print("Failed to reset memcached affinity")
    
    finally:
        # End the logger
        logger.end()
        print("Test completed and logger closed")


def main():
    """Main function to run the tests."""
    parser = argparse.ArgumentParser(description="Test the scheduler components")
    parser.add_argument("--memcached-ip", required=True, help="IP address of the memcached server")
    parser.add_argument("--test", choices=["monitor", "docker", "all"], default="all", 
                        help="Which component to test (default: all)")
    args = parser.parse_args()
    
    if args.test in ["monitor", "all"]:
        test_resource_monitor(args.memcached_ip)
    
    if args.test in ["docker", "all"]:
        test_docker_manager(args.memcached_ip)


if __name__ == "__main__":
    main() 