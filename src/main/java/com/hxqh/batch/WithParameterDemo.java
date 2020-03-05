package com.hxqh.batch;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;


/**
 * Created by Ocean lin on 2020/3/3.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class WithParameterDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5);

        Configuration conf = new Configuration();
        conf.setInteger("limit", 3);

        DataSet<Integer> filter = data.filter(new RichFilterFunction<Integer>() {
            private int limit;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                limit = parameters.getInteger("limit", 0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value > limit;
            }
        });

        filter.print();
    }


}
