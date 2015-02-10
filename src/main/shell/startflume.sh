#!/bin/bash


export FLUME_HOME=/home/sponge/app/apache-flume
agent_name=a1


$FLUME_HOME/bin/flume-ng agent -n $agent_name -c conf -f ../conf/flume.conf
