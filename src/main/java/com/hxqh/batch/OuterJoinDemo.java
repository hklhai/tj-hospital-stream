package com.hxqh.batch;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Created by Ocean lin on 2020/3/1.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class OuterJoinDemo {


    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1, "lily"));
        list1.add(new Tuple2<>(2, "lucy"));
        list1.add(new Tuple2<>(4, "jack"));

        ArrayList<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1, "beijing"));
        list2.add(new Tuple2<>(2, "shanghai"));
        list2.add(new Tuple2<>(3, "guangzhou"));

        DataSet<Tuple2<Integer, String>> ds1 = env.fromCollection(list1);
        DataSet<Tuple2<Integer, String>> ds2 = env.fromCollection(list2);

        ds1.leftOuterJoin(ds2).where(0).equalTo(0).with(
                new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (null == second) {
                            return new Tuple3<>(first.f0, first.f1, "null");
                        } else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();


        ds1.rightOuterJoin(ds2).where(0).equalTo(0).with(
                new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (null == first) {
                            return new Tuple3<>(second.f0, "null", second.f1);
                        } else {
                            return new Tuple3<>(second.f0, first.f1, second.f1);
                        }
                    }
                }).print();


        ds1.fullOuterJoin(ds2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (null == first) {
                            return new Tuple3<>(second.f0, "null", second.f1);
                        } else if (null == second) {
                            return new Tuple3<>(first.f0, first.f1, "null");
                        } else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                }).print();
    }
}
