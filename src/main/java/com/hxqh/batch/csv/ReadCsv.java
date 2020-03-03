package com.hxqh.batch.csv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

/**
 * Created by Ocean lin on 2020/2/26.
 *
 * @author Ocean lin
 */
public class ReadCsv {

    public static void main(String[] args) {
        //获取一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取输入数据
        DataSet<Tuple3<Integer, Integer, String>> csvDS = env.readCsvFile("D:\\data\\user.csv")
                .includeFields("11100")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .ignoreComments("##")
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(Integer.class, Integer.class, String.class);

        DataSet<Map<String, Object>> map = csvDS.map(new MapFunction<Tuple3<Integer, Integer, String>, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Tuple3<Integer, Integer, String> integerIntegerStringTuple3) throws Exception {
                return null;
            }
        });

        try {
            csvDS.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
