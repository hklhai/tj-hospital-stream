package com.hxqh.stream;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Ocean lin on 2020/1/19.
 *
 * @author Ocean lin
 */
public class CustomDataSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStreamSource = env.addSource(new CustomSourceFunction());
        SingleOutputStreamOperator<Long> sum = dataStreamSource.timeWindowAll(Time.seconds(5)).sum(0);

        sum.print().setParallelism(1);
        env.execute("CustomDataSource");
    }



    public static class CustomSourceFunction implements ParallelSourceFunction<Long> {

        private Long num = 0L;

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (true) {
                sourceContext.collect(num);
                num++;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
