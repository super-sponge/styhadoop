#!/bin/bash
HADOOP_HOME=/home/sponge/app/hadoop-2.6.0
HIVE_HOME=/home/sponge/app/apache-hive-0.14.0-bin
CLASSPATH=$CLASSPATH:
for i in $HADOOP_HOME/share/hadoop/common/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done
for i in $HIVE_HOME/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

java -cp  $CLASSPATH:/home/sponge/IdeaProjects/styhadoop/target/styhadoop-1.0-SNAPSHOT.jar com.sponge.srd.hive.HiveServer2Client
