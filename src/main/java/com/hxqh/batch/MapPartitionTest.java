package com.hxqh.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
public class MapPartitionTest {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //产生数据
        DataSet<Long> ds = env.generateSequence(1, 20);
        MapPartitionOperator<Long, Long> partition = ds.mapPartition(new MapPartitionFunction<Long, Long>() {
            @Override
            public void mapPartition(Iterable<Long> iterable, Collector<Long> collector) throws Exception {
                Long count = 0L;
                for (Long aLong : iterable) {
                    count++;
                }
                collector.collect(count);
            }
        });

        partition.print();

    }


}
