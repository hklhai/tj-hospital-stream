package com.hxqh.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class BatchTableDemo {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<String> ds1 = env.readTextFile("D:\\data\\user.txt");
        DataSet<Tuple2<String, String>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });
        Table table1 = tEnv.fromDataSet(ds2, "uid,uname");

        Table table2 = table1.select("uname");

        DataSet<String> dataSet = tEnv.toDataSet(table2, String.class);

        dataSet.print();


    }
}
