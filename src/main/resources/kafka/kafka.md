###  新建topic

dev
``` 
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic mediumvoltage --replication-factor 1 --partitions 1 --create
    
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yxtest --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yctest --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic asset2 --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic err1 --replication-factor 1 --partitions 1 --create

```

    
producrtion
``` 
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic all --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yx --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yc --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic err --replication-factor 1 --partitions 1 --create

```



    
### 删除topic
kafka-topics.sh --zookeeper localhost:2181 --delete --topic mediumvoltage
    
###  产生
kafka-console-producer.sh --broker-list localhost:9092 --topic mediumvoltage

### 消费 deprecated
kafka-console-consumer.sh --zookeeper localhost:2181 --topic mediumvoltage --from-beginning
  
### 消费
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage 

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic yxtest 

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic yctest 

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --partition 0 --offset 25320

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --partition  0 --offset 620019

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic err1 --from-beginning
 
 


## kafka
### 查看消费进度
kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --topic mediumvoltage  --zookeeper 127.0.0.1:2181  

### 计算消息的消息堆积情况
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group asset2

kafka-console-consumer.sh --bootstrap-server localhost:9092 --describe --group yxtest

kafka-console-consumer.sh --bootstrap-server localhost:9092 --describe --group  yctest 

### 查询
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --partition 0 --offset 60
 




```

org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6UpsertTableSinkFactory
org.apache.flink.table.sources.CsvBatchTableSourceFactory
org.apache.flink.streaming.connectors.kafka.Kafka010TableSourceSinkFactory
org.apache.flink.streaming.connectors.kafka.Kafka09TableSourceSinkFactory

```