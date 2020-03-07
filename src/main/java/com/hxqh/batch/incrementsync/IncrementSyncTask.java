package com.hxqh.batch.incrementsync;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.hxqh.batch.flow.FlowSoure;
import com.hxqh.batch.function.ProcessFunction;
import com.hxqh.batch.schema.FlatMessageSchema;
import com.hxqh.batch.sink.HbaseSyncSink;
import com.hxqh.domain.Flow;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import static com.hxqh.constant.Constant.NUM;

/**
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */
public class IncrementSyncTask {


    public static final MapStateDescriptor<String, Flow> flowBroadcastState = new MapStateDescriptor<>(
            "flowBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Flow>() {
            }));


    public static void main(String[] args) throws Exception {
        args = new String[]{"--input-topic", "hk5", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "asset2",
                "--key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "--value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "--auto.offset.reset", "latest",
                "--flink.partition-discovery.interval-millis", "3000"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < NUM) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer010<FlatMessage> consumer = new FlinkKafkaConsumer010<>(parameterTool.getRequired("input-topic"),
                new FlatMessageSchema(), parameterTool.getProperties());

        DataStream<FlatMessage> message = env.addSource(consumer);
        KeyedStream<FlatMessage, String> keyedMessage = message.keyBy(new KeySelector<FlatMessage, String>() {
            @Override
            public String getKey(FlatMessage value) throws Exception {
                return value.getDatabase() + "|" + value.getTable();
            }
        });

        BroadcastStream<Flow> broadcast = env.addSource(new FlowSoure()).broadcast(flowBroadcastState);


        DataStream<Tuple2<FlatMessage, Flow>> connected = keyedMessage.connect(broadcast).process(new ProcessFunction()).setParallelism(1);
        connected.addSink(new HbaseSyncSink());

        env.execute("IncrementSyncTask");

    }
}
