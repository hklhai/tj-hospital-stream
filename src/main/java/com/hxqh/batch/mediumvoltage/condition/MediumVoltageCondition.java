package com.hxqh.batch.mediumvoltage.condition;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static com.hxqh.constant.Constant.*;

/**
 * 中压设备-电压状况 每8h取一个点
 * <p>
 * Created by Ocean lin on 2020/3/23.
 *
 * @author Ocean lin
 */
public class MediumVoltageCondition {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Row> list = new ArrayList<>();
        Properties properties = new Properties();
        properties.put("url", JDBC_ES_URL);
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        dds.setInitialSize(1);
        Connection connection = dds.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLast8HoursStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
        // String sql = "select assetYpe,productModel,location,productModelC,avg(ABLineVoltage) as avgA,avg(BCLineVoltage) as avgB,avg(CALineVoltage) as avgC from yc_mediumvoltage3 where CreateTime>'2020-03-11 08:00:00' and CreateTime<'2020-03-11 15:59:59' group by assetYpe,productModel,location,productModelC";
        String sql = "select assetYpe,productModel,location,productModelC,avg(ABLineVoltage) as avgA,avg(BCLineVoltage) as avgB,avg(CALineVoltage) as avgC from yc_mediumvoltage3" +
                " where CreateTime>'" + start + "' and CreateTime<'" + end + "' group by assetYpe,productModel,location,productModelC";
        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Row row = new Row(7);
            row.setField(0, resultSet.getString("assetYpe"));
            row.setField(1, resultSet.getString("productModel"));
            row.setField(2, resultSet.getString("location"));
            row.setField(3, resultSet.getString("productModelC"));
            row.setField(4, (resultSet.getDouble("avgA") + resultSet.getDouble("avgB") + resultSet.getDouble("avgC")) / 3);
            row.setField(5, end);
            row.setField(6, DateUtils.formatDate(new Date()));
            list.add(row);
        }
        ps.close();
        connection.close();
        dds.close();
        DataSource<Row> source = env.fromCollection(list);
        // source.print();

        String insertQuery = "INSERT INTO RE_VOLTAGE_CONDITION(ASSETYPE,PRODUCTMODEL,LOCATION,productModelC,VOLTAGECONDITION,TIMEPOINT,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        source.output(outputBuilder.finish());

        env.execute("MediumVoltageCondition");
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR};
    }

}
