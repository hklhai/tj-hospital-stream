package com.hxqh.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 *
 * arg --output d:\out
 *
 * Created by Ocean lin on 2020/1/12.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class WordCountCounter {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet;
        if (parameter.has("input")) {
            dataSet = env.readTextFile(parameter.get("input"));
        } else {
            dataSet = WordCountData.getDefaultTextLineDataSet(env);
        }


        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = dataSet.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            // 1.创建累加器对象
            private IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2.注册累加器对象
                getRuntimeContext().addAccumulator("counter", this.counter);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 3.累加器统计数据行数
                this.counter.add(1);
                String[] s1 = s.toLowerCase().split(" ");
                for (String str : s1) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        });

        DataSet<Tuple2<String, Integer>> sum = flatMap.groupBy(0).sum(1);

        if (parameter.has("output")) {
            sum.writeAsCsv(parameter.get("output"), "\n", " ");
            JobExecutionResult jobExecutionResult = env.execute("WordCountCounter");
            int counter = jobExecutionResult.getAccumulatorResult("counter");
            System.out.println("counter=" + counter);
        } else {
            sum.print();
        }

    }
}
