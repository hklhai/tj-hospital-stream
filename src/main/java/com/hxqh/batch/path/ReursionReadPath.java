package com.hxqh.batch.path;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
public class ReursionReadPath {

    public static void main(String[] args) {
        //获取一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //新建一个配置
        Configuration conf = new Configuration();

        //设置递归参数
        conf.setBoolean("recursive.file.enumeration", true);

        DataSet<String> ds = env.readTextFile("file:///D:\\data")
                .withParameters(conf);

        try {
            ds.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
