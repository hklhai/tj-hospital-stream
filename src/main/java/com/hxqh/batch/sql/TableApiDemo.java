package com.hxqh.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 订单编号，用户ID，订单状态，订单金额，到付\在线，支付方式
 * 1,101,0,234,1,2
 * <p>
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class TableApiDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<String> ds1 = env.readTextFile("D:\\data\\order.txt");

        DataSet<Tuple6<String, String, Integer, Double, Integer, Integer>> ds = ds1.map(new MapFunction<String, Tuple6<String, String, Integer, Double, Integer, Integer>>() {
            @Override
            public Tuple6<String, String, Integer, Double, Integer, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple6(split[0], split[1], Integer.parseInt(split[2]), Double.parseDouble(split[3]),
                        Integer.parseInt(split[4]), Integer.parseInt(split[5]));
            }
        });

        tEnv.registerDataSet("order", ds, "orderid,userid,orderstatus,price,paytype,payfrom");
        Table order = tEnv.scan("order");

        Table result = order.filter("orderstatus==1")
                .groupBy("userid")
                .select("userid,price.sum as income");

        DataSet<Row> rowDataSet = tEnv.toDataSet(result, Row.class);

        rowDataSet.print();

    }
}
