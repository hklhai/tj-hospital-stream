package com.hxqh.batch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import static com.hxqh.constant.Constant.ES_HOST;

/**
 * Created by Ocean lin on 2020/3/4.
 *
 * @author Ocean lin
 */
public class WriteHbaseDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple4<String, String, Integer, String>> user = env.fromElements(Tuple4.of("100", "hk", 30, "BJ"),
                Tuple4.of("101", "hk1", 30, "SH"),
                Tuple4.of("102", "小花", 30, "SX"),
                Tuple4.of("103", "小明", 30, "HB")
        );

        DataSet<Tuple2<Text, Mutation>> res = user.map(new RichMapFunction<Tuple4<String, String, Integer, String>, Tuple2<Text, Mutation>>() {

            private transient Tuple2<Text, Mutation> resultTp;

            private byte[] columnFamily = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                resultTp = new Tuple2<>();
            }

            @Override
            public Tuple2<Text, Mutation> map(Tuple4<String, String, Integer, String> user) throws Exception {
                resultTp.f0 = new Text(user.f0);
                Put put = new Put(user.f0.getBytes(ConfigConstants.DEFAULT_CHARSET));
                if (null != user.f1) {
                    put.addColumn(columnFamily, Bytes.toBytes("name"), Bytes.toBytes(user.f1));
                }
                if (null != user.f2) {
                    put.addColumn(columnFamily, Bytes.toBytes("age"), Bytes.toBytes(user.f2.toString()));
                }
                if (null != user.f3) {
                    put.addColumn(columnFamily, Bytes.toBytes("city"), Bytes.toBytes(user.f3));
                }
                resultTp.f1 = put;
                return resultTp;
            }
        });
        res.print();

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", ES_HOST);
        configuration.set(TableOutputFormat.OUTPUT_TABLE, "hk_flink:users");
        configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
        Job job = Job.getInstance(configuration);

        res.output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(), job));

        env.execute("WriteHbaseDemo");
    }

}
