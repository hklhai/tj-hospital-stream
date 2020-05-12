#!/bin/bash

#find /data/tmp/ -type d -mtime 1 -name "*" | xargs rm -rf

cd /data/tmp/
ls --full-time | grep `date -d 'yesterday' +%Y-%m-%d`  | awk '{print $9}' | xargs rm -rf