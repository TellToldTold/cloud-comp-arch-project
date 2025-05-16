import time
from typing import Dict

class JobsTimer:
    def __init__(self):
        self.job_timers: Dict[str, Dict[str, float]] = {}
        self.first_job_start = None
        self.last_job_end = None

    def start(self, job_name: str):
        """Start the timer for job with name job_name."""
        if job_name in self.job_timers:
            raise Exception(f"Job {job_name} has already been started.")
        t0 = time.time()
        self.job_timers[job_name] = {
            "start_time": t0,
            "accumulated_pause": 0.0,
            "pause_start": None
        }
        if self.first_job_start is None:
            self.first_job_start = t0

    def pause(self, job_name: str):
        """Pause the timer for job with name job_name."""
        if job_name not in self.job_timers:
            raise Exception(f"Job {job_name} has not been started.")
        if self.job_timers[job_name]["pause_start"] is not None:
            raise Exception(f"Job {job_name} is already paused.")
        self.job_timers[job_name]["pause_start"] = time.time()

    def resume(self, job_name: str):
        """Resume the timer for job with name job_name."""
        if job_name not in self.job_timers:
            raise Exception(f"Job {job_name} has not been started.")
        if self.job_timers[job_name]["pause_start"] is None:
            raise Exception(f"Job {job_name} is not paused.")
        pause_duration = time.time() - self.job_timers[job_name]["pause_start"]
        self.job_timers[job_name]["accumulated_pause"] += pause_duration
        self.job_timers[job_name]["pause_start"] = None

    def stop(self, job_name: str):
        """Stop the timer for job with name job_name."""
        if job_name not in self.job_timers:
            raise Exception(f"Job {job_name} has not been started.")
        if self.job_timers[job_name]["pause_start"] is not None:
            self.resume(job_name)
        t1 = time.time()
        self.job_timers[job_name]["end_time"] = t1
        self.last_job_end = t1

    def get_job_time(self, job_name: str) -> float:
        """Get the total time for job with name job_name."""
        if job_name not in self.job_timers:
            raise Exception(f"Job {job_name} has not been started.")
        if "end_time" not in self.job_timers[job_name]:
            raise Exception(f"Job {job_name} has not been stopped.")
        total_time = self.job_timers[job_name]["end_time"] - self.job_timers[job_name]["start_time"]
        total_time -= self.job_timers[job_name]["accumulated_pause"]
        return total_time
    
    def get_total_time(self) -> float:
        """Get the total time for all jobs."""
        if self.first_job_start is None or self.last_job_end is None:
            raise Exception("No jobs have been started or stopped.")
        return self.last_job_end - self.first_job_start
    
    def get_job_times(self) -> Dict[str, float]:
        """Get the total time for all jobs."""
        job_times = {}
        for job_name in self.job_timers:
            job_times[job_name] = self.get_job_time(job_name)
        return job_times