# TJ Hospital
 
[![Build Status](https://travis-ci.org/hklhai/tj-hospital-stream.svg?branch=master)](https://travis-ci.org/hklhai/tj-hospital-stream)
 
---

![image](https://github.com/hklhai/tj-hospital-stream/blob/master/screenshot/ios.gif)
---
 


> HK  
> hkhai@outlook.com

---
D:\HXQH\CompanyProjectGitLab\Tianjin-Hospital\stream-project\src\main\resources\db2\backup
 
---
组件 | 机器 | 位置
---|---|---
flume | ecs-455a | /root/app/apache-flume
kafka | ecs-455a | /root/app/kafka_2.10-0.10.2.1
flink | ecs-455a | /root/app/flink
zk    | ecs-455a | /root/app/zookeeper-3.4.12
hbase | ecs-455a | /root/app/hbase-2.0.5
hadoop| ecs-455a | /root/app/hadoop-2.6.5



### zookeeper启动
```
cd /root/app/zookeeper-3.4.12/bin
./zkServer.sh start
 ```


### flume启动
```
cd /root/app/apache-flume/bin
nohup ./flume-ng agent --conf-file ../conf/http-kafka-conf.properties --name a1 -Dflume.root.logger=INFO,console 2>&1 &
```
 
 
## kafka
### 启动kafka
```
/root/app/kafka_2.10-0.10.2.1/bin/kafka-server-start.sh -daemon /root/app/kafka_2.10-0.10.2.1/config/server.properties
```


## Flink
```
/root/app/flink-1.8.0/bin/start-cluster.sh 
```



## ELK
需切换至hadoop用户
### ElasticSearch
```
cd /home/hadoop/app/elasticsearch-6.1.2/bin
./elasticsearch -d
```


### Kibana
``` 
cd /home/hadoop/app/kibana-6.1.2-linux-x86_64
nohup ./kibana > ki.log  2>&1 &
```

