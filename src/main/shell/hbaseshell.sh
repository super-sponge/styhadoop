#!/bin/bash

export HBASE_SHELL_OPTS="-verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDateStamps  -XX:+PrintGCDetails -Xloggc:$HBASE_HOME/logs/gc-hbase.log" ./bin/hbase shell

echo "describe 'test'" | ./hbase shell -n > /dev/null 2>&1
status=$?
echo "The status was " $status
if ($status == 0); then
    echo "The command succeeded"
else
    echo "The command may have failed."
fi
return $status

## Checking for success or failure in scripts. you should check the result,not only the return value.

## Execut script
./hbase shell ../scripts/sample_commands.txt
## hbase shell debug mode
## ./bin/hbase shell -d