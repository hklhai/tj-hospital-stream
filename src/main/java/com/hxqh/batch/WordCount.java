package com.hxqh.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 执行
 * <p>
 * flink run -c com.hxqh.batch.WordCount tj-hospital.jar
 * flink run -c com.hxqh.batch.WordCount tj-hospital.jar --input /root/app/execute-jar/in.txt --output /root/app/execute-jar/out.txt
 * <p>
 * Created by Ocean lin on 2020/1/4.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class WordCount {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet;
        if (parameter.has("input")) {
            dataSet = env.readTextFile(parameter.get("input"));
        } else {
            dataSet = WordCountData.getDefaultTextLineDataSet(env);
        }

//        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] s1 = s.toLowerCase().split(" ");
//                for (String str : s1) {
//                    collector.collect(new Tuple2<>(str, 1));
//                }
//            }
//        });

        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = dataSet.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] s1 = s.toLowerCase().split(" ");
                    for (String str : s1) {
                        collector.collect(new Tuple2<>(str, 1));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING,Types.INT));

        DataSet<Tuple2<String, Integer>> sum = flatMap.groupBy(0).sum(1);

        if (parameter.has("output")) {
            sum.writeAsCsv(parameter.get("output"), "\n", " ");
            env.execute(" Word Count!");
        } else {
            sum.print();
        }

    }
}
