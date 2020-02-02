package com.hxqh.stream.window;

import com.hxqh.mockdata.MockData;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/2/2.
 *
 * @author Ocean lin
 */
public class ProcessWindowFunctionOnWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(MockData.data);
        DataStream<Double> process = input.keyBy(0).countWindow(3).process(new ProcessWindowFunction<Tuple3<String, String, Integer>, Double, Tuple, GlobalWindow>() {
            /**
             * 所有数据到达后开始计算
             * @param tuple
             * @param context
             * @param iterable
             * @param collector
             * @throws Exception
             */
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Integer>> iterable, Collector<Double> collector) throws Exception {
                Integer sum = 0;
                Integer count = 0;
                for (Tuple3<String, String, Integer> tuple3 : iterable) {
                    sum += tuple3.f2;
                    count++;
                }
                collector.collect(sum * 1.0 / count);
            }
        });
        process.print();
        env.execute("ProcessWindowFunctionOnWindow");
    }
}
