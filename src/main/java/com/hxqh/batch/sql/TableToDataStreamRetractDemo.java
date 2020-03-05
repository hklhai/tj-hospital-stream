package com.hxqh.batch.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.ES_HOST;

/**
 *
 * nc -lk 9999
 *
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class TableToDataStreamRetractDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream(ES_HOST, 9999, "\n");

        DataStream<Tuple2<String, Integer>> ds = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (int i = 0; i < s.length; i++) {
                    out.collect(new Tuple2<>(s[i], 1));
                }
            }
        });

        // DataStream转表并注册
        Table table = tEnv.fromDataStream(ds, "word,frequency");
        tEnv.registerTable("wc", table);

        // 分组统计 Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.
        Table groupTable = tEnv.sqlQuery("select word,count(frequency) as cn from wc group by word");
        // 表转DataStream
        DataStream<Tuple2<Boolean, Row>> dataStream = tEnv.toRetractStream(groupTable, Row.class);

        // 表转DataStream
        dataStream.print();

        env.execute("TableToDataStreamRetractDemo");
    }

}
