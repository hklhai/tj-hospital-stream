package com.hxqh.batch.mediumvoltage.temperature;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/4/2.
 *
 * @author Ocean lin
 */
public class MediumVoltageTemperature {

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
//        String sql = "select IEDName,avg(AUpperArmTemperature) as AUpperArmTemperatureAvg,avg(BUpperArmTemperature) as BUpperArmTemperatureAvg,avg(CUpperArmTemperature) as CUpperArmTemperatureAvg," +
//                 "max(AUpperArmTemperature) as AUpperArmTemperatureMax,max(BUpperArmTemperature) as BUpperArmTemperatureMax,max(CUpperArmTemperature) as CUpperArmTemperatureMax," +
//                 "avg(ALowerArmTemperature) as ALowerArmTemperatureAvg,avg(BLowerArmTemperature) as BLowerArmTemperatureAvg,avg(CLowerArmTemperature) as CLowerArmTemperatureAvg," +
//                 "max(ALowerArmTemperature) as ALowerArmTemperatureMax,max(BLowerArmTemperature) as BLowerArmTemperatureMax,max(CLowerArmTemperature) as CLowerArmTemperatureMax," +
//                 "avg(ACableTemperature) as ACableTemperatureAvg,avg(BCableTemperature) as BCableTemperatureAvg,avg(CCableTemperature) as CCableTemperatureAvg," +
//                 "max(ACableTemperature) as ACableTemperatureMax,max(BCableTemperature) as BCableTemperatureMax,max(CCableTemperature) as CCableTemperatureMax " +
//                 "from yc_mediumvoltage3 where ColTime>'2020-03-20 08:00:00' and ColTime<'2020-03-20 23:59:59' group by IEDName";

        String sql = "select IEDName,avg(AUpperArmTemperature) as AUpperArmTemperatureAvg,avg(BUpperArmTemperature) as BUpperArmTemperatureAvg,avg(CUpperArmTemperature) as CUpperArmTemperatureAvg," +
                "max(AUpperArmTemperature) as AUpperArmTemperatureMax,max(BUpperArmTemperature) as BUpperArmTemperatureMax,max(CUpperArmTemperature) as CUpperArmTemperatureMax," +
                "avg(ALowerArmTemperature) as ALowerArmTemperatureAvg,avg(BLowerArmTemperature) as BLowerArmTemperatureAvg,avg(CLowerArmTemperature) as CLowerArmTemperatureAvg," +
                "max(ALowerArmTemperature) as ALowerArmTemperatureMax,max(BLowerArmTemperature) as BLowerArmTemperatureMax,max(CLowerArmTemperature) as CLowerArmTemperatureMax," +
                "avg(ACableTemperature) as ACableTemperatureAvg,avg(BCableTemperature) as BCableTemperatureAvg,avg(CCableTemperature) as CCableTemperatureAvg," +
                "max(ACableTemperature) as ACableTemperatureMax,max(BCableTemperature) as BCableTemperatureMax,max(CCableTemperature) as CCableTemperatureMax " +
                "from yc_mediumvoltage3 where ColTime>'" + start + "' and ColTime<'" + end + "' group by IEDName";

        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Row row = new Row(21);
            row.setField(0, resultSet.getString("IEDName"));
            row.setField(1, resultSet.getDouble("AUpperArmTemperatureAvg"));
            row.setField(2, resultSet.getDouble("BUpperArmTemperatureAvg"));
            row.setField(3, resultSet.getDouble("CUpperArmTemperatureAvg"));
            row.setField(4, resultSet.getDouble("AUpperArmTemperatureMax"));
            row.setField(5, resultSet.getDouble("BUpperArmTemperatureMax"));
            row.setField(6, resultSet.getDouble("CUpperArmTemperatureMax"));
            row.setField(7, resultSet.getDouble("ALowerArmTemperatureAvg"));
            row.setField(8, resultSet.getDouble("BLowerArmTemperatureAvg"));
            row.setField(9, resultSet.getDouble("CLowerArmTemperatureAvg"));
            row.setField(10, resultSet.getDouble("ALowerArmTemperatureMax"));
            row.setField(11, resultSet.getDouble("BLowerArmTemperatureMax"));
            row.setField(12, resultSet.getDouble("CLowerArmTemperatureMax"));
            row.setField(13, resultSet.getDouble("ACableTemperatureAvg"));
            row.setField(14, resultSet.getDouble("BCableTemperatureAvg"));
            row.setField(15, resultSet.getDouble("CCableTemperatureAvg"));
            row.setField(16, resultSet.getDouble("ACableTemperatureMax"));
            row.setField(17, resultSet.getDouble("BCableTemperatureMax"));
            row.setField(18, resultSet.getDouble("CCableTemperatureMax"));

            row.setField(19, end);
            row.setField(20, new Timestamp((new Date()).getTime()));
            list.add(row);
        }
        ps.close();
        connection.close();
        dds.close();
        DataSource<Row> source = env.fromCollection(list);

        String insertQuery = "INSERT INTO RE_VOLTAGE_TEMPERATURE (IEDName,AUpperArmTemperatureAvg,BUpperArmTemperatureAvg,CUpperArmTemperatureAvg,AUpperArmTemperatureMax,BUpperArmTemperatureMax,CUpperArmTemperatureMax," +
                "ALowerArmTemperatureAvg,BLowerArmTemperatureAvg,CLowerArmTemperatureAvg,ALowerArmTemperatureMax,BLowerArmTemperatureMax,CLowerArmTemperatureMax," +
                "ACableTemperatureAvg,BCableTemperatureAvg,CCableTemperatureAvg,ACableTemperatureMax,BCableTemperatureMax,CCableTemperatureMax,TIMEPOINT,CREATETIME" +
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        source.output(outputBuilder.finish());

        env.execute("MediumVoltageTemperature");
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
                Types.VARCHAR, Types.TIMESTAMP};
    }
}
