package com.hxqh.stream.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import static com.hxqh.constant.Constant.NUM;

/**
 *
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hk1 --from-beginning
 *
 * Created by Ocean lin on 2020/2/11.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class CustomKafkaProducer {

    public static void main(String[] args) {

        args = new String[]{"--output-topic", "hk1", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "hk",
                "--windows.size", "5", "--windows.slide", "1"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < NUM) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("tj-hospital.com", 9999, "\n");


        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"), new SimpleStringSchema(), parameterTool.getProperties());
        dataStream.addSink(myProducer);

        try {
            env.execute("CustomKafkaProducer");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
