#!/bin/bash
        cd ~/memcache-perf-dynamic
        ./mcperf -s 10.0.16.6 100.96.1.1 100.96.1.1 -a 10.0.16.4 \
        --noload -T 8 -C 8 -D 4 -Q 1000 -c 8 -t 5 \
        --scan 5000:220000:5000
    