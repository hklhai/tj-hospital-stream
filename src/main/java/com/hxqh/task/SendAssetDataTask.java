package com.hxqh.task;

import com.hxqh.schema.CustomRowSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.types.Row;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/2/26.
 *
 * @author Ocean lin
 */
public class SendAssetDataTask {

    public static void main(String[] args) {

        args = new String[]{"--output-topic", "asset2", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "asset2",
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

        JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(DRIVER_NAME)
                .setDBUrl(DB_URL)
                .setUsername(USERNAME)
                .setPassword(PASSWORD)
                .setQuery("select  ASSETNUM, ASSETYPE, PRODUCTMODEL from ASSET ")
                .setRowTypeInfo(new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(String.class)))
                .finish();
        DataStreamSource<Row> input = env.createInput(inputFormat);

        FlinkKafkaProducer010<Row> myProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"), new CustomRowSchema(), parameterTool.getProperties());
        input.addSink(myProducer);

        try {
            env.execute("SendAssetDataTask");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
