package com.hxqh.batch.faulttolerance;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Created by Ocean lin on 2020/3/3.
 *
 * @author Ocean lin
 */
public class FailureRateDemo {

    public static void main(String[] args) throws Exception {
        //获取一个运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
         * 一小时最多失败2次，间隔时间10s
         */
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                2,
                Time.of(1, TimeUnit.HOURS),
                Time.of(10, TimeUnit.SECONDS)
        ));

        //读取数据
        DataSet<String> data = env.fromElements("1", "2", "", "4", "5");

        data.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        }).print();
    }

}
