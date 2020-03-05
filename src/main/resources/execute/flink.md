## 启动Flink作业
cd /root/app/flink-1.8.0/bin

nohup ./flink run -c com.hxqh.task.JoinTask /root/TJJar/tj-hospital.jar > joinTask.log 2>&1 &


nohup ./flink run -c com.hxqh.task.ProcessTask /root/TJJar/tj-hospital.jar > alldata.log 2>&1 &

nohup ./flink run -c com.hxqh.task.ProcessYcAtsTask /root/TJJar/tj-hospital.jar > yc_ast.log 2>&1 &

nohup ./flink run -c com.hxqh.task.ProcessYxAtsTask /root/TJJar/tj-hospital.jar > yx_ast.log 2>&1 &



## Hbase 
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.WriteHbaseDemo /root/src/tj-hospital-stream/target/tj-hospital.jar > WriteHbaseDemo.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.ReadHbaseRawDemo /root/src/tj-hospital-stream/target/tj-hospital.jar > ReadHbaseRawDemo.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.ReadHbaseCustomDemo /root/src/tj-hospital-stream/target/tj-hospital.jar > ReadHbaseCustomDemo.log 2>&1 &
