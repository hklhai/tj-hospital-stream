package com.hxqh.stream;

import com.hxqh.batch.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/1/4.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class WordCountStream {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream;
        if (parameter.has("input")) {
            stream = env.readTextFile(parameter.get("input"));
        } else {
            stream = env.fromElements(WordCountData.WORDS);
        }
        DataStream<Tuple2<String, Integer>> flatMap = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.toLowerCase().split(" ");
                for (String str : s1) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        });

        DataStream<Tuple2<String, Integer>> sum = flatMap.keyBy(0).sum(1);

        if (parameter.has("output")) {
            sum.writeAsCsv(parameter.get("output"));
        } else {
            sum.print();
        }
        env.execute(" Word Count Stream");

    }
}
