package com.hxqh.stream.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import static com.hxqh.constant.Constant.NUM;

/**
 * kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic hk1 --replication-factor 1 --partitions 1 --create
 * kafka-console-producer.sh --broker-list localhost:9092 --topic hk1
 * <p>
 * <p>
 * Created by Ocean lin on 2020/2/11.
 *
 * @author Ocean lin
 */
public class CustomKafkaConsumer {

    public static void main(String[] args) {

        args = new String[]{"--input-topic", "hk1", "--bootstrap.servers", "tj-hospital.com:9092",
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-topic"), new SimpleStringSchema(), parameterTool.getProperties());

        DataStream<String> input = env.addSource(flinkKafkaConsumer);
        input.print();

        try {
            env.execute("CustomKafkaConsumer");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
