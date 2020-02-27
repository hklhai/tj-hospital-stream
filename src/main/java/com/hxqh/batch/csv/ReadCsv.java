package com.hxqh.batch.csv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

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

        try {
            csvDS.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
