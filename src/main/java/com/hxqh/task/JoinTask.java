package com.hxqh.task;


import com.hxqh.domain.UserEvent;
import com.hxqh.schema.UserEventDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/2/24.
 *
 * @author Ocean lin
 */
public class JoinTask {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 检查输入参数
        ParameterTool params = parameterCheck(args);

        // 设置EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //启动checkp
        env.enableCheckpointing(600000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //语义保证
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        //checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(10000L);
        //启动外部持久化检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * stateBackend
         */
        //env.setStateBackend(new FsStateBackend("hdfs://mycluster/flink-checkpoints/purchase-behavior"));

        // restart 策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS)));

        /**
         * Kafka consumer
         */
        Properties consumerProps = new Properties();
        consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID));

        /**
         * 读取kafka事件流
         */
        final FlinkKafkaConsumer010<UserEvent> kafkaUserEventSource = new FlinkKafkaConsumer010<UserEvent>(params.get(INPUT_EVENT_TOPIC), new UserEventDeserializationSchema(), consumerProps);

        KeyedStream<UserEvent, String> customerUserEventStream = env.addSource(kafkaUserEventSource)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(24)))
                .keyBy((userEvent) -> {
                    String userId = userEvent.getUserId();
                    return userId;
                });
        customerUserEventStream.print();


    }


    /**
     * 参数校验
     */
    public static ParameterTool parameterCheck(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        if (!params.has(BOOTSTRAP_SERVERS)) {
            System.err.println("----------------parameter[bootstrap.servers] is required-------------------------");
            System.exit(-1);
        }

        if (!params.has(GROUP_ID)) {
            System.err.println("----------------parameter[group.id] is required-------------------------");
            System.exit(-1);
        }

        if (!params.has(INPUT_EVENT_TOPIC)) {
            System.err.println("----------------parameter[input-event-topic] is required-------------------------");
            System.exit(-1);
        }

        if (!params.has(INPUT_CONFIG_TOPIC)) {
            System.err.println("----------------parameter[input-config-topic] is required-------------------------");
            System.exit(-1);
        }

        if (!params.has(OUTPUT_TOPIC)) {
            System.err.println("----------------parameter[output-topic] is required-------------------------");
            System.exit(-1);
        }

        return params;
    }


    /**
     * 自定义watermark
     */
    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {
        public CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserEvent element) {
            return element.getEventTime();
        }
    }


}
