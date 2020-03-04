package com.hxqh.batch;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static com.hxqh.constant.Constant.ES_HOST;

/**
 * Created by Ocean lin on 2020/3/4.
 *
 * @author Ocean lin
 */
public class ReadHbaseDemo {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<Tuple2<String, String>> input = env.createInput(new TableInputFormat<Tuple2<String, String>>() {
            private byte[] columnFamily = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);

            @Override
            public void configure(Configuration parameters) {
                HTable table = createtable();
                if (null != table) {
                    scan = getScanner();
                }
            }

            private HTable createtable() {
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                configuration.set("hbase.zookeeper.quorum", ES_HOST);
                try {
                    return new HTable(configuration, "hk_flink:users");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }


            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addFamily(columnFamily);
                return scan;
            }

            @Override
            protected String getTableName() {
                return "hk_flink:users";
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                return Tuple2.of(Bytes.toString(result.getRow()),
                        Bytes.toString(result.getValue(columnFamily, "value".getBytes(ConfigConstants.DEFAULT_CHARSET)))
                );
            }
        });

        input.print();
    }
}
