package com.hxqh.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Created by Ocean lin on 2020/3/1.
 *
 * @author Ocean lin
 */
public class BroadcastDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //准备需要广播的数据
        ArrayList<Tuple2<String, String>> broadCastData = new ArrayList<>();
        broadCastData.add(new Tuple2<>("101", "jack"));
        broadCastData.add(new Tuple2<>("102", "tom"));
        broadCastData.add(new Tuple2<>("103", "john"));
    }
}
