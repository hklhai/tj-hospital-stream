package com.hxqh.task;


import com.alibaba.fastjson.JSON;
import com.hxqh.domain.AssetType;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.function.ConnectedBroadcastProcessFunction;
import com.hxqh.function.TjBroadcastProcessFunction;
import com.hxqh.schema.AssetTypeDeserializationSchema;
import com.hxqh.transfer.ProcessWaterEmitter;
import com.hxqh.utils.JsonUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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

    public static final MapStateDescriptor<String, AssetType> configStateDescriptor = new MapStateDescriptor<String, AssetType>(
            "configBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<AssetType>() {
            }));


    public static void main(String[] args) throws Exception {
        args = new String[]{"--input-topic", "mediumvoltage", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "asset2",
                "--output-topic-yx", "yxtest", "--output-topic-yc", "yctest", "--asset-topic", "asset2"};

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
        KeyedStream<String, String> input = env.addSource(kafkaConsumerBase).keyBy((s) -> {
            return s;
        });


        /**
         * 读取Kafka 配置流信息
         */
        final FlinkKafkaConsumer010<AssetType> kafkaConfigEventSource = new FlinkKafkaConsumer010<AssetType>(
                parameterTool.getRequired("asset-topic"),
                new AssetTypeDeserializationSchema(),
                parameterTool.getProperties());
        final BroadcastStream<AssetType> configBroadcastStream = env.addSource(kafkaConfigEventSource)
                .broadcast(configStateDescriptor);


        DataStream<String> process = input.connect(configBroadcastStream).process(new TjBroadcastProcessFunction());
        DataStream<String> allData = process.filter(s -> {
            if (s != null && !"".equals(s) && JsonUtils.isjson(s)) {
                return true;
            }
            return false;
        });

        DataStream<String> yx = allData.filter(s -> {
            IEDEntity iedEntity = JSON.parseObject(s, IEDEntity.class);
            return iedEntity.getCKType().equals(YX) ? true : false;
        }).name("YX-Filter");


        FlinkKafkaProducer010<String> yxProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-yx"), new SimpleStringSchema(), parameterTool.getProperties());
        yx.addSink(yxProducer).name("YX-SINK");

        DataStream<String> yc = allData.filter(s -> {
            IEDEntity iedEntity = JSON.parseObject(s, IEDEntity.class);
            return iedEntity.getCKType().equals(YC) ? true : false;
        }).name("YC-Filter");

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
