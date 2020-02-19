package com.hxqh.task;

import com.hxqh.sink.Db2YcAtsSink;
import com.hxqh.transfer.ProcessWaterEmitter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import static com.hxqh.constant.Constant.NUM;

/**
 * Created by Ocean lin on 2020/2/18.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ProcessYcTask {

    public static void main(String[] args) {


        args = new String[]{"--input-topic", "yctest", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "yctest"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < NUM) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        // create a checkpoint every 5 seconds 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(parameterTool);
        // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-topic"), new SimpleStringSchema(), parameterTool.getProperties());

        FlinkKafkaConsumerBase kafkaConsumerBase = flinkKafkaConsumer.assignTimestampsAndWatermarks(new ProcessWaterEmitter());
        DataStream<String> input = env.addSource(kafkaConsumerBase);
        input.addSink(new Db2YcAtsSink()).name("YC-DB2-Sink");

        // todo ES Sink
        //input.addSink(new ElasticSearchYcSink()).name("ElasticSearchYcSink");

        try {
            env.execute("ProcessYcTask");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
