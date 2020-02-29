package com.hxqh.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
public class DistinctDemo {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Long, String, Integer>> elements = env.fromElements(Tuple3.of(1L, "zhangsan", 28),
                Tuple3.of(3L, "lisi", 34),
                Tuple3.of(3L, "wangwu", 23),
                Tuple3.of(3L, "zhaoliu", 34),
                Tuple3.of(3L, "lili", 25));

        elements.distinct(0).print();
    }
}
