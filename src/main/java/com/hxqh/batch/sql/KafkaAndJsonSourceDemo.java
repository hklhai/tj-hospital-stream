package com.hxqh.batch.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * {"userId":10001,"day":"2017-03-02","begintime":1488326400000,"endtime":1488327000000,"data":[{"package":"com.browser","activetime":120000}]}
 * <p>
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
public class KafkaAndJsonSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.connect(new Kafka()
                .version("0.10")
                .topic("hk3")
                .startFromLatest()
                .property("group.id", "group1")
                .property("bootstrap.servers", "tj-hospital.com:9092"))
                .withFormat(
                        // 指定字段缺失是否允许失败
                        new Json().failOnMissingField(false)
                                .deriveSchema()
                ).withSchema(
                new Schema()
                        .field("userId", Types.LONG())
                        .field("day", Types.STRING())
                        .field("begintime", Types.LONG())
                        .field("endtime", Types.LONG())
                        .field("data", ObjectArrayTypeInfo.getInfoFor(
                                Row[].class,
                                Types.ROW(new String[]{"package", "activetime"},
                                        new TypeInformation[]{Types.STRING(), Types.LONG()})))

        ).inAppendMode().registerTableSource("userlog");

        Table table = tableEnvironment.sqlQuery("select userId from userlog");
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnvironment.toRetractStream(table, Row.class);
        rowDataStream.print();
        env.execute("KafkaAndJsonSourceDemo");
    }
}
