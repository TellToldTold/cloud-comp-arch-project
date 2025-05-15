#!/bin/bash
        cd ~/memcache-perf-dynamic
        ./mcperf -s 10.0.16.5 100.96.1.1 100.96.1.1 100.96.1.1 172.17.0.1 -a 10.0.16.4 \
        --noload -T 8 -C 8 -D 4 -Q 1000 -c 8 -t 780 \
        --qps_interval 8 --qps_min 5000 --qps_max 180000 \
    --qps_seed 2333