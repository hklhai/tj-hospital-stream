package com.hxqh.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
public class UnionDemo {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(101,"hk"));
        list1.add(new Tuple2<>(102,"hk1"));
        list1.add(new Tuple2<>(103,"hk2"));

        ArrayList<Tuple2<Integer,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(101,"hk3"));
        list2.add(new Tuple2<>(102,"hk4"));
        list2.add(new Tuple2<>(103,"hk5"));

        DataSet<Tuple2<Integer, String>> ds1 = env.fromCollection(list1);
        DataSet<Tuple2<Integer, String>> ds2 = env.fromCollection(list2);
        DataSet<Tuple2<Integer, String>> union = ds1.union(ds2);
        union.print();

    }

}
