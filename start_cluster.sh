#!/bin/bash
set -xe

eval $1 -c $2 start $3 
nohup eval $1 -c $2 runplugin taigaTunnel $3
echo "Cluster is up now"
