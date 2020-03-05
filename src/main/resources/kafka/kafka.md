
###  新建topic

dev
``` 
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic mediumvoltage --replication-factor 1 --partitions 1 --create
    
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yxtest --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yctest --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic hk3 --replication-factor 1 --partitions 1 --create

// canal
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic hk5 --replication-factor 1 --partitions 1 --create
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic hk6 --replication-factor 1 --partitions 1 --create
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic hk7 --replication-factor 1 --partitions 1 --create


```

    
producrtion
``` 
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic all --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yx --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic yc --replication-factor 1 --partitions 1 --create
```

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic purchasePathAnalysisInPut --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic purchasePathAnalysisConf --replication-factor 1 --partitions 1 --create

kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic purchasePathAnalysisOutPut --replication-factor 1 --partitions 1 --create

    
### 删除topic
kafka-topics.sh --zookeeper localhost:2181 --delete --topic mediumvoltage
    
###  产生
kafka-console-producer.sh --broker-list localhost:9092 --topic mediumvoltage

kafka-console-producer.sh --broker-list localhost:9092 --topic hk3



### 消费 deprecated
kafka-console-consumer.sh --zookeeper localhost:2181 --topic mediumvoltage --from-beginning
  
### 消费
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic yxtest --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic yctest --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --partition 0 --offset 400


kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic purchasePathAnalysisInPut --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic purchasePathAnalysisConf --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic purchasePathAnalysisOutPut --from-beginning



kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hk5 --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hk6 --from-beginning

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hk7 --from-beginning


## kafka
### 查看消费进度
kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --topic mediumvoltage  --zookeeper 127.0.0.1:2181 --group mediumvoltage

### 计算消息的消息堆积情况
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group mediumvoltage
 
### 查询
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mediumvoltage --partition 0 --offset 60
 

### 查看topic列表
kafka-topics.sh --zookeeper 127.0.0.1:2181 --list


