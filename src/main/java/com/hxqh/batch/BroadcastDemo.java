package com.hxqh.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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

        //读取数据源
        DataSet<Tuple2<String, String>> tuple2broadCastData = env.fromCollection(broadCastData);

        //数据集转换为map类型
        DataSet<HashMap<String, String>> toBroadCast = tuple2broadCastData.map(new MapFunction<Tuple2<String, String>, HashMap<String, String>>() {
            @Override
            public HashMap<String, String> map(Tuple2<String, String> value) throws Exception {
                HashMap<String, String> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });

        //准备处理数据
        ArrayList<Tuple2<String, Integer>> operatorData = new ArrayList<>();
        operatorData.add(new Tuple2<>("101", 2000000));
        operatorData.add(new Tuple2<>("102", 190000));
        operatorData.add(new Tuple2<>("103", 1000000));

        //读取处理数据
        DataSet<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(operatorData);

        DataSet<String> result = tuple2DataSource.map(new RichMapFunction<Tuple2<String, Integer>, String>() {

            List<Map<String, String>> broadCastMap = new ArrayList<>();

            Map<String, String> allMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastName");
                for (Map map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(Tuple2<String, Integer> tuple2) throws Exception {
                String name = allMap.get(tuple2.f0);
                return name + "," + tuple2.f1;
            }
        }).withBroadcastSet(toBroadCast, "broadCastName");
        result.print();
    }
}
