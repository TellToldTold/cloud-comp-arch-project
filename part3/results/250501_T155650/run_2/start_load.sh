#!/bin/bash
        cd ~/memcache-perf-dynamic
        ./mcperf -s 100.96.1.9 -a 10.0.16.7 -a 10.0.16.4 --noload         -T 6 -C 4 -D 4 -Q 1000 -c 4 -t 10 --scan 30000:30500:5
    