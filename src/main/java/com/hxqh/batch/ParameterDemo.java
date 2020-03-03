package com.hxqh.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


/**
 * Created by Ocean lin on 2020/3/3.
 *
 * @author Ocean lin
 */
public class ParameterDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSet<Integer> data = env.fromElements(1, 2, 3, 4, 5);

        /**
         * 构造方法传递参数
         */
        DataSet<Integer> filter = data.filter(new MyFilter(3));
        filter.print();

    }

    private static class MyFilter implements FilterFunction<Integer> {
        private int limit = 3;

        public MyFilter(int limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Integer value) throws Exception {
            return value > limit;
        }
    }
}
