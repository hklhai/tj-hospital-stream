package com.hxqh.batch;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;


/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
public class DefaultJoinDemo {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1, "lily"));
        list1.add(new Tuple2<>(2, "lucy"));
        list1.add(new Tuple2<>(3, "tom"));
        list1.add(new Tuple2<>(4, "jack"));

        ArrayList<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1, "beijing"));
        list2.add(new Tuple2<>(2, "shanghai"));
        list2.add(new Tuple2<>(3, "guangzhou"));

        DataSet<Tuple2<Integer, String>> ds1 = env.fromCollection(list1);
        DataSet<Tuple2<Integer, String>> ds2 = env.fromCollection(list2);

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> joinedData = ds1.join(ds2).where(0).equalTo(0);
        joinedData.print();
    }

}
