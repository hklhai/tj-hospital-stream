package com.hxqh.batch;

import com.hxqh.custom.CustomTableInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
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
@SuppressWarnings("Duplicates")
public class ReadHbaseCustomDemo {


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<Tuple4<String, String, Integer, String>> input = env.createInput(new CustomTableInputFormat<Tuple4<String, String, Integer, String>>() {
            private byte[] columnFamily = "F".getBytes(ConfigConstants.DEFAULT_CHARSET);

            @Override
            public void configure(Configuration parameters) {
                table = createtable();
                if (null != table) {
                    scan = getScanner();
                }
            }

            private HTable createtable() {
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                configuration.set("hbase.zookeeper.quorum", ES_HOST);
                try {
                    return new HTable(configuration, getTableName());
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
            protected Tuple4<String, String, Integer, String> mapResultToTuple(Result result) {
                String t1 = Bytes.toString(result.getRow());
                String t2 = Bytes.toString(result.getValue(columnFamily, "name".getBytes(ConfigConstants.DEFAULT_CHARSET)));
                Integer t3 = Integer.parseInt(Bytes.toString(result.getValue(columnFamily, "age".getBytes(ConfigConstants.DEFAULT_CHARSET))));
                String t4 = Bytes.toString(result.getValue(columnFamily, "city".getBytes(ConfigConstants.DEFAULT_CHARSET)));
                return Tuple4.of(t1, t2, t3, t4);
            }
        });

        input.print();
    }
}
