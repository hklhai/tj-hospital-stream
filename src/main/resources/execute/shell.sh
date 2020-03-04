nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.WriteHbaseDemo /root/src/tj-hospital-stream/target/tj-hospital.jar > WriteHbaseDemo.log 2>&1 &


nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.ReadHbaseRawDemo /root/src/tj-hospital-stream/target/tj-hospital.jar > ReadHbaseRawDemo.log 2>&1 &


nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.ReadHbaseCustomDemo /root/src/tj-hospital-stream/target/tj-hospital.jar > ReadHbaseCustomDemo.log 2>&1 &
