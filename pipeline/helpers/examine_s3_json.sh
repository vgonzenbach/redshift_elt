#!/bin/bash
# Quick scripts to inspect contents of files in s3

# song data
aws s3 ls s3://udacity-dend/song_data --recursive | awk 'NR==2{print $4}' | xargs -n1 bash -c 'aws s3 cp s3://udacity-dend/$0 -' | head -n1 | jq
aws s3 ls s3://udacity-dend/log_data --recursive | awk 'NR==2{print $4}' | xargs -n1 bash -c 'aws s3 cp s3://udacity-dend/$0 -' | head -n1 | jq

