# TJ Hospital
 
[![Build Status](https://travis-ci.org/hklhai/tj-hospital-stream.svg?branch=master)](https://travis-ci.org/hklhai/tj-hospital-stream)
 
---
> HK  
> hkhai@outlook.com

 
组件 | 机器 | 位置
---|---|---
flume | ecs-455a | /root/app/apache-flume
kafka | ecs-455a | /root/app/kafka_2.10-0.10.2.1
flink | ecs-455a | /root/app/flink

### zookeeper启动
cd /root/app/zookeeper-3.4.12/bin
./zkServer.sh start
 


### flume启动

cd /root/app/apache-flume/bin
nohup ./flume-ng agent --conf-file ../conf/http-kafka-conf.properties --name a1 -Dflume.root.logger=INFO,console 2>&1 &

 
 
## kafka
### 启动kafka
/root/app/kafka_2.10-0.10.2.1/bin/kafka-server-start.sh -daemon /root/app/kafka_2.10-0.10.2.1/config/server.properties



###  新建topic
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic mediumvoltage --replication-factor 1 --partitions 1 --create
    
### 删除topic
kafka-topics.sh --zookeeper localhost:2181 --delete --topic medium_voltage
    
###  产生
kafka-console-producer.sh --broker-list localhost:9092 --topic mediumvoltage

### 消费 deprecated
kafka-console-consumer.sh --zookeeper localhost:2181 --topic mediumvoltage --from-beginning
  
### 消费
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --from-beginning
 


## kafka
### 查看消费进度
kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --topic hk2  --zookeeper 127.0.0.1:2181 --group hk2

### 计算消息的消息堆积情况
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group hk2

kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connection-topic

kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group attr-topic