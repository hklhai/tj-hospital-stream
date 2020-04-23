package com.hxqh.task;

import com.hxqh.domain.Yx;
import com.hxqh.schema.YxRowSchema;
import com.hxqh.task.alarm.acb.AcbFirstAlarm;
import com.hxqh.task.alarm.acb.AcbSecondAlarm;
import com.hxqh.task.alarm.acb.AcbThirdAlarm;
import com.hxqh.transfer.LowPressureAcbWaterEmitter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static com.hxqh.constant.Constant.NUM_4;

/**
 * 低压馈线柜、进线柜、母联柜遥测实时报警
 * <p>
 * Created by Ocean lin on 2020/4/20.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class LowPressureAcbTask {

    public static void main(String[] args) {
        args = new String[]{"--input-topic", "lowacb", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "lowacb", "--output-topic", "mediumvoltage"};


        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < NUM_4) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(parameterTool);
        // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));


        tableEnvironment.connect(new Kafka()
                .version("0.10")
                .topic(parameterTool.getRequired("input-topic"))
                .startFromLatest()
                .property("connector.type", "kafka")
                .property("group.id", parameterTool.getRequired("group.id"))
                .property("bootstrap.servers", parameterTool.getRequired("bootstrap.servers")))
                .withFormat(
                        // 指定字段缺失是否允许失败
                        new Json().failOnMissingField(false)
                                .deriveSchema()
                ).withSchema(
                new Schema()
                        .field("IEDName", Types.STRING())
                        .field("CKType", Types.STRING())
                        .field("colTime", Types.STRING())
                        .field("assetYpe", Types.STRING())
                        .field("location", Types.STRING())
                        .field("parent", Types.STRING())
                        .field("productModel", Types.STRING())
                        .field("productModelB", Types.STRING())
                        .field("productModelC", Types.STRING())

                        .field("ActiveElectricDegree", Types.DOUBLE())
                        .field("ContactWear", Types.DOUBLE())
                        .field("OperationNumber", Types.INT())
                        .field("PhaseL1CurrentPercent", Types.DOUBLE())
                        .field("PhaseL1L2Voltage", Types.DOUBLE())
                        .field("PhaseL2CurrentPercent", Types.DOUBLE())
                        .field("PhaseL2L3Voltage", Types.DOUBLE())
                        .field("PhaseL3CurrentPercent", Types.DOUBLE())
                        .field("PhaseL3L1Voltage", Types.DOUBLE())
                        .field("PowerFactor", Types.DOUBLE())
                        .field("ReactiveElectricDegree", Types.DOUBLE())

        ).inAppendMode().registerTableSource("acb");

        Table table = tableEnvironment.sqlQuery("select * from acb");
        DataStream<Row> data = tableEnvironment.toAppendStream(table, Row.class);
        data.assignTimestampsAndWatermarks(new LowPressureAcbWaterEmitter());

        DataStream<Yx> firstAlarm = data.flatMap(new AcbFirstAlarm());
        DataStream<Yx> secondAlarm = data.flatMap(new AcbSecondAlarm());
        DataStream<Yx> thridAlarm = data.flatMap(new AcbThirdAlarm());

        DataStream<Yx> allAlarm = firstAlarm.union(secondAlarm).union(thridAlarm);

        FlinkKafkaProducer010<Yx> yxProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"), new YxRowSchema(), parameterTool.getProperties());
        allAlarm.addSink(yxProducer);

        try {
            env.execute("LowPressureAcbTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
