package com.hxqh.batch.loadfactor;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

import static com.hxqh.constant.Constant.*;
import static com.hxqh.constant.Constant.DB2_PASSWORD;

/**
 * 单台中压开关柜负荷率-季度
 *
 * Created by Ocean lin on 2020/3/18.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MediumVoltageLoadFactorQuarter {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        java.util.List<Tuple2<String, Double>> list = new ArrayList<>();
        Properties properties = new Properties();
        properties.put("url", JDBC_ES_URL);
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        dds.setInitialSize(1);
        Connection connection = dds.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastQuarterStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
        String sql = "select IEDName,productModelB,avg(APhaseCurrent) as avgA,avg(BPhaseCurrent) as avgB,avg(CPhaseCurrent) as avgC from yc_mediumvoltage_1" +
                " where CreateTime>'" + start + "' and CreateTime<'" + end + "' group by IEDName,productModelB";
        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple2<String, Double> tuple2 = new Tuple2<>();
            tuple2.f0 = resultSet.getString("IEDName") + "|" + resultSet.getString("productModelB");
            tuple2.f1 = (resultSet.getDouble("avgA") + resultSet.getDouble("avgB") + resultSet.getDouble("avgC")) / 3;
            list.add(tuple2);
        }
        ps.close();
        connection.close();
        dds.close();
        DataSource<Tuple2<String, Double>> source = env.fromCollection(list);

        DataSet<Row> sink = source.map(new org.apache.flink.api.common.functions.MapFunction<Tuple2<String, Double>, Row>() {
            @Override
            public Row map(Tuple2<String, Double> value) throws Exception {
                Row row = new Row(3);
                String[] split = value.f0.split("\\|");
                row.setField(0, split[0]);
                if (split[1].endsWith("A")) {
                    String currentRated = split[1].replaceAll("A", "");
                    row.setField(1, value.f1 / Double.parseDouble(currentRated));
                }
                row.setField(2, RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_LOAD_QUARTER (IEDNAME,LOADFACTOR,CREATETIME) VALUES(?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(new int[]{Types.VARCHAR, Types.DOUBLE, Types.VARCHAR})
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        sink.output(outputBuilder.finish());

        env.execute("MediumVoltageLoadFactorQuarter");

    }
}
