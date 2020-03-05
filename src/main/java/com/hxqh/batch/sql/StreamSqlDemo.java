package com.hxqh.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
public class StreamSqlDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStream<String> ds1 = env.readTextFile("D:\\data\\user.txt");
        DataStream<Tuple2<String, String>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });

        // 转换Table
        Table table = tableEnvironment.fromDataStream(ds2,"uid,name");

        // 注册一个表
        tableEnvironment.registerTable("user1", table);
        Table table2 = tableEnvironment.sqlQuery("select * from user1").select("name");

        // 将表转成DataStream
        DataStream<String> nameStream = tableEnvironment.toAppendStream(table2, String.class);

        nameStream.print();
        env.execute("StreamSqlDemo");
    }
}
