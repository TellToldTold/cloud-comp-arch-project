# Dynamic Resource Scheduler for Part 4

This directory contains the implementation of a dynamic resource scheduler for Part 4 of the Cloud Computing Architecture project. The scheduler is designed to manage resources between a latency-sensitive memcached service and batch jobs from the PARSEC benchmark suite.

## Overview

The scheduler monitors system resources (particularly CPU usage) and dynamically adjusts the allocation of resources between memcached and batch jobs to meet the following objectives:

1. Maintain memcached's tail latency SLO of 0.8ms at the 95th percentile.
2. Complete all batch jobs as quickly as possible by opportunistically using resources when memcached load is low.

## Files

- `scheduler_logger.py`: Logging utility to record events in the required format.
- `resource_monitor.py`: Utility to monitor system resources (CPU, memory, processes, etc.).
- `docker_manager.py`: Utility to manage Docker containers for batch jobs, as well as memcached resource allocation.
- `scheduler_controller.py`: Main controller that makes scheduling decisions and manages resources.
- `test_scheduler.py`: Test script to verify the functionality of individual components.
- `setup_scheduler.sh`: Script to set up the environment (install dependencies, etc.).
- `README.md`: This file.

## Setup

1. Copy these files to the memcached server VM where you want to run the scheduler.

2. Make the setup script executable and run it:
   ```
   chmod +x setup_scheduler.sh
   ./setup_scheduler.sh
   ```

3. The setup script will:
   - Install required Python packages (psutil, docker, netaddr)
   - Install Docker if not already installed
   - Install netcat (for memcached stats)
   - Make the Python scripts executable

## Testing

To verify that the components are working correctly:

```
./test_scheduler.py --memcached-ip <MEMCACHED_IP>
```

Replace `<MEMCACHED_IP>` with the internal IP address of your memcached server (typically the same as the VM where you're running the scheduler).

You can also test specific components:

```
./test_scheduler.py --memcached-ip <MEMCACHED_IP> --test monitor  # Test resource monitoring only
./test_scheduler.py --memcached-ip <MEMCACHED_IP> --test docker   # Test Docker management only
```

## Running the Scheduler

To run the scheduler controller:

```
./scheduler_controller.py --memcached-ip <MEMCACHED_IP>
```

The current implementation includes a simple example that starts the blackscholes benchmark on available cores. You'll need to customize the scheduling policy in the `main()` function of `scheduler_controller.py` to implement your desired strategy.

### Customizing the Scheduling Policy

To implement your own scheduling policy, you should modify the `main()` function in `scheduler_controller.py`. The `SchedulerController` class provides methods to:

- Start batch jobs: `controller.start_batch_job(job_name, cores, threads)`
- Update job cores: `controller.update_job_cores(job_name, new_cores)`
- Pause/unpause jobs: `controller.pause_job(job_name)`, `controller.unpause_job(job_name)`
- Stop jobs: `controller.stop_job(job_name)`
- Get resource status: `controller.get_available_cores()`, `controller.get_job_status()`

Your policy should continuously monitor system resources and mcperf latency metrics to make decisions about job scheduling and resource allocation.

## Docker Container Limitations

For Docker containers, note that:

1. `--cpuset-cpus` specifies which physical CPU cores a container may use. It doesn't guarantee exclusive access to those cores, just that the container will be scheduled on those cores.

2. Memcached runs outside of Docker, and its CPU affinity is managed using `taskset`.

## Notes

- The scheduler will create a log file in the format `logYYYYMMDD_HHMMSS.txt` in the current directory when it runs. This log contains events in the required format.

- You need to be a member of the docker group to run Docker commands without sudo. The setup script adds your user to this group, but you may need to log out and back in for the changes to take effect. Alternatively, run `newgrp docker` to apply the group changes in your current session.

- The scheduler handles signals to gracefully shut down when interrupted (e.g., with Ctrl+C).

- You can monitor memcached's performance by connecting to it using the mcperf tool as described in the project instructions.
