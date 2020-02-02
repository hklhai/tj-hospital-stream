package com.hxqh.stream.window;

import com.hxqh.mockdata.MockData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Ocean lin on 2020/2/2.
 *
 * @author Ocean lin
 */
public class AggregateFunctionOnWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(MockData.data);
        DataStream<Double> aggregate = input.keyBy(0).countWindow(3).aggregate(new AggregateFunction<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>, Double>() {
            /**
             * 保存sum和count
             * @return
             */
            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return new Tuple2<>(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(Tuple3<String, String, Integer> t1, Tuple2<Integer, Integer> acc) {
                return new Tuple2<>(acc.f0 + t1.f2, acc.f1 + 1);
            }

            @Override
            public Double getResult(Tuple2<Integer, Integer> acc) {
                return acc.f0 * 1.0 / acc.f1;
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc1, Tuple2<Integer, Integer> acc2) {
                return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
            }
        });
        aggregate.print();
        env.execute("AggregateFunctionOnWindow");

    }
}
