#!/bin/bash
set -e

eval $1 -c $2 start $3 
eval $1 -c $2 runplugin taigaTunnel $3
echo "Cluster is up now"
