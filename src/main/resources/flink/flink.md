## 启动Flink作业

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessTask /root/TJJar/base/tj-hospital.jar > /root/TJJar/processTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessErrorTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessErrorTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcMediumVoltageTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcMediumVoltageTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYxTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYxTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcTransformerTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcTransformerTask.log 2>&1 &





## 批量停止Flink作业
ps -ef | grep flink | grep -v grep | awk '{print "kill -9 "$2}'|sh



## Flink
```
/root/app/flink-1.8.0/bin/start-cluster.sh 
```
