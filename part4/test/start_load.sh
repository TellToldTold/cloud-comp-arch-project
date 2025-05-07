#!/bin/bash
        cd ~/memcache-perf-dynamic
        ./mcperf -s 10.0.16.6 100.96.2.1 -a 10.0.16.5 --noload         -T 8 -C 8 -D 4 -Q 1000 -c 8 -t 10 --qps_interval 2 --qps_min 5000 --qps_max 180000
    