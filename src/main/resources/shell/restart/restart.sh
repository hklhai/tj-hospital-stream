#!/bin/sh
export FLUME_HOME=/root/app/apache-flume
export MAVEN_HOME=/root/app/maven-3.6.3
export SCALA_HOME=/root/app/scala-2.11.12
export JAVA_HOME=/root/app/jdk1.8.0_231
export KAFKA_HOME=/root/app/kafka_2.10-0.10.2.1
export FLINK_HOME=/root/app/flink-1.8.0
export REDIS_HOME=/root/app/redis-5.0.7
export ZK_HOME=/root/app/zookeeper-3.4.12
export HADOOP_HOME=/root/app/hadoop-2.6.5
export HBASE_HOME=/root/app/hbase-2.2.3
export HBASE_MANAGES_ZK=false


export PATH=$JAVA_HOME/bin:$PATH:$SCALA_HOME/bin:$MAVEN_HOME/bin:$FLUME_HOME/bin:$KAFKA_HOME/bin:$FLINK_HOME/bin:$REDIS_HOME/bin:$ZK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar



ps -ef | grep flink | grep -v grep | awk '{print "kill -9 "$2}'|sh

ps -ef | grep  org.apache.flink.runtime.taskexecutor.TaskManagerRunner | grep -v grep | awk '{print "kill -9 "$2}'|sh


/root/app/flink-1.8.0/bin/start-cluster.sh 


nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessTask /root/TJJar/base/tj-hospital.jar > /root/TJJar/processTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessErrorTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessErrorTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcMediumVoltageTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcMediumVoltageTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYxTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYxTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcTransformerTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcTransformerTask.log 2>&1 &


nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcLowPressureTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcLowPressureTask.log 2>&1 &


sleep 3

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.LowPressureAcbTask /root/TJJar/tj-hospital.jar > /root/TJJar/LowPressureAcbTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.LowPressureAtsTask /root/TJJar/tj-hospital.jar > /root/TJJar/LowPressureAtsTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.LowPressureCapacitorTask /root/TJJar/tj-hospital.jar > /root/TJJar/LowPressureCapacitorTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.LowPressureDrawerTask /root/TJJar/tj-hospital.jar > /root/TJJar/LowPressureDrawerTask.log 2>&1 &


