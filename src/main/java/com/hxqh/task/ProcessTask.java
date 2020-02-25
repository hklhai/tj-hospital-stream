package com.hxqh.task;


import com.alibaba.fastjson.JSON;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.transfer.ProcessWaterEmitter;
import com.hxqh.utils.JsonUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.concurrent.TimeUnit;

import static com.hxqh.constant.Constant.*;


/**
 * Created by Ocean lin on 2020/2/14.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ProcessTask {

    public static void main(String[] args) throws Exception {
        args = new String[]{"--input-topic", "mediumvoltage", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "mediumvoltage",
                "--output-topic-yx", "yxtest", "--output-topic-yc", "yctest"};

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

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));

        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-topic"), new SimpleStringSchema(), parameterTool.getProperties());

        FlinkKafkaConsumerBase kafkaConsumerBase = flinkKafkaConsumer.assignTimestampsAndWatermarks(new ProcessWaterEmitter());
        DataStream<String> input = env.addSource(kafkaConsumerBase);

        DataStream<String> allData = input.filter(s -> {
            if (s != null && !"".equals(s) && JsonUtils.isjson(s)) {
                return true;
            }
            return false;
        });

        DataStream<String> yx = allData.filter(s -> {
            IEDEntity iedEntity = JSON.parseObject(s, IEDEntity.class);
            return iedEntity.getCKType().equals(YX) ? true : false;
        });


        FlinkKafkaProducer010<String> yxProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-yx"), new SimpleStringSchema(), parameterTool.getProperties());
        yx.addSink(yxProducer).name("YX-SINK");

        DataStream<String> yc = allData.filter(s -> {
            IEDEntity iedEntity = JSON.parseObject(s, IEDEntity.class);
            return iedEntity.getCKType().equals(YC) ? true : false;
        });

        FlinkKafkaProducer010<String> ycProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-yc"), new SimpleStringSchema(), parameterTool.getProperties());
        yc.addSink(ycProducer).name("YC-SINK");

        try {
            env.execute("ProcessTask");
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }


}
