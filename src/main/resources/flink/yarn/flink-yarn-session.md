`
/root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessTask -j /root/TJJar/base/tj-hospital.jar -a '' -p 2 -yid application_15597 -nm ProcessTask -d


/root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessTask \
-j /root/TJJar/base/tj-hospital.jar -a '--input-topic", "mediumvoltage", "--bootstrap.servers", "tj-hospital.com:9092","--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "asset2", "--output-topic-yx", "yxtest", "--output-topic-yc", "yctest", "--asset-topic", "asset2"' \
-p 2 -yid application_15597 -nm ProcessTask -d

`