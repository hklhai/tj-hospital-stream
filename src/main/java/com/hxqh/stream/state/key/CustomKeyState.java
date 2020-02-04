package com.hxqh.stream.state.key;

import com.hxqh.mockdata.MockData;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/2/4.
 *
 * @author Ocean lin
 */
public class CustomKeyState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Long>> streamSource = env.fromElements(MockData.eventData);

        streamSource.keyBy(0).flatMap(new CountWithKeyState()).setParallelism(10).print();
        env.execute("CustomKeyState");
    }

    private static class CountWithKeyState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long, Long>> sum;


        @Override
        public void flatMap(Tuple2<Long, Long> val, Collector<Tuple2<Long, Long>> collector) throws Exception {
            Tuple2<Long, Long> currentSum = sum.value();
            if (null == currentSum) {
                currentSum = Tuple2.of(0L, 0L);
            }

            currentSum.f0 += 1;
            currentSum.f1 += val.f1;
            sum.update(currentSum);

            if (currentSum.f0 >= 3) {
                collector.collect(Tuple2.of(val.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            /**
             * 注意这里仅仅用了状态，但是没有利用状态来容错
             */
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>("avgState", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }));

            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
