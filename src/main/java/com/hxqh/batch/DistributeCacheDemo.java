package com.hxqh.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by Ocean lin on 2020/3/2.
 *
 * @author Ocean lin
 */
public class DistributeCacheDemo {


    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 注册一个本地可执行文件，用户基本数据
        env.registerCachedFile("file:///D:\\data\\user.txt", "localFile", true);

        // 准备用户游戏充值数据
        ArrayList<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("101", 2000000));
        data.add(new Tuple2<>("102", 190000));
        data.add(new Tuple2<>("103", 1090000));

        // 读取数据源
        DataSet<Tuple2<String, Integer>> tuple2DataSource = env.fromCollection(data);


        DataSet<String> res = tuple2DataSource.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            Map<String, String> allMap = new HashMap<>();


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File localFile = getRuntimeContext().getDistributedCache().getFile("localFile");
                List<String> lines = FileUtils.readLines(localFile);

                for (String line : lines) {
                    String[] split = line.split(",");
                    allMap.put(split[0], split[1]);
                }
            }

            @Override
            public String map(Tuple2<String, Integer> tuple2) throws Exception {
                String name = allMap.get(tuple2.f0);
                return name + "," + tuple2.f1;
            }
        });

        res.print();
    }

}
