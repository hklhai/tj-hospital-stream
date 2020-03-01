package com.hxqh.batch;

import com.hxqh.domain.UserInfo;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Created by Ocean lin on 2020/3/1.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class JoinWithJoinFunctionDemo {


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
        DataSet<UserInfo> res = ds1.join(ds2).where(0).equalTo(0).with(new UserFunction());
        res.print();
    }


    private static class UserFunction implements JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, UserInfo> {
        @Override
        public UserInfo join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
            return UserInfo.of(first.f0, first.f1, second.f1);
        }
    }
}
