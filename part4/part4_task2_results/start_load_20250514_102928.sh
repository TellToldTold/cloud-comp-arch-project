#!/bin/bash
        cd ~/memcache-perf-dynamic
        ./mcperf -s 10.0.16.5 172.17.0.1 -a 10.0.16.3 \
        --noload -T 8 -C 8 -D 4 -Q 1000 -c 8 -t 780 \
        --qps_interval 10 --qps_min 5000 --qps_max 180000 \
    --qps_seed 2333