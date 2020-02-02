package com.hxqh.stream.window;

import com.hxqh.mockdata.MockData;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Ocean lin on 2020/2/2.
 *
 * @author Ocean lin
 */
public class ReduceFunctionOnWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(MockData.data);
        DataStream<Tuple3<String, String, Integer>> reduce = input.keyBy(0).countWindow(3).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> v1, Tuple3<String, String, Integer> v2) throws Exception {
                return new Tuple3<>(v1.f0, v1.f1, v1.f2 + v2.f2);
            }
        });

        reduce.print();
        env.execute("ReduceFunctionOnWindow");
    }


}
