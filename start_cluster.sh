#!/bin/bash
set -xe

eval $1 -c $2 start --cluster-template=$4 $3 
nohup $1 -c $2 runplugin taigaTunnel $3 > /tmp/taigaTunnel 2>&1
sleep 5
cat /tmp/taigaTunnel
echo "Cluster is up now"
