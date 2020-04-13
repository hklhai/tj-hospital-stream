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

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyYear.log 2>&1 &
