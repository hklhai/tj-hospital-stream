package com.hxqh.batch.mediumvoltage.temperature;

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
 * Created by Ocean lin on 2020/3/24.
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
//                 "from yc_mediumvoltage3 where CreateTime>'2020-03-11 08:00:00' and CreateTime<'2020-03-11 15:59:59' group by IEDName";

        String sql = "select IEDName,avg(AUpperArmTemperature) as AUpperArmTemperatureAvg,avg(BUpperArmTemperature) as BUpperArmTemperatureAvg,avg(CUpperArmTemperature) as CUpperArmTemperatureAvg," +
                "max(AUpperArmTemperature) as AUpperArmTemperatureMax,max(BUpperArmTemperature) as BUpperArmTemperatureMax,max(CUpperArmTemperature) as CUpperArmTemperatureMax," +
                "avg(ALowerArmTemperature) as ALowerArmTemperatureAvg,avg(BLowerArmTemperature) as BLowerArmTemperatureAvg,avg(CLowerArmTemperature) as CLowerArmTemperatureAvg," +
                "max(ALowerArmTemperature) as ALowerArmTemperatureMax,max(BLowerArmTemperature) as BLowerArmTemperatureMax,max(CLowerArmTemperature) as CLowerArmTemperatureMax," +
                "avg(ACableTemperature) as ACableTemperatureAvg,avg(BCableTemperature) as BCableTemperatureAvg,avg(CCableTemperature) as CCableTemperatureAvg," +
                "max(ACableTemperature) as ACableTemperatureMax,max(BCableTemperature) as BCableTemperatureMax,max(CCableTemperature) as CCableTemperatureMax " +
                "from yc_mediumvoltage3 where CreateTime>'" + start + "' and CreateTime<'" + end + "' group by IEDName";

        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Row row = new Row(21);
            row.setField(0, resultSet.getString("IEDName"));
            row.setField(1, Double.parseDouble(resultSet.getString("AUpperArmTemperatureAvg")));
            row.setField(2, Double.parseDouble(resultSet.getString("BUpperArmTemperatureAvg")));
            row.setField(3, Double.parseDouble(resultSet.getString("CUpperArmTemperatureAvg")));
            row.setField(4, Double.parseDouble(resultSet.getString("AUpperArmTemperatureMax")));
            row.setField(5, Double.parseDouble(resultSet.getString("BUpperArmTemperatureMax")));
            row.setField(6, Double.parseDouble(resultSet.getString("CUpperArmTemperatureMax")));

            row.setField(7, Double.parseDouble(resultSet.getString("ALowerArmTemperatureAvg")));
            row.setField(8, Double.parseDouble(resultSet.getString("BLowerArmTemperatureAvg")));
            row.setField(9, Double.parseDouble(resultSet.getString("CLowerArmTemperatureAvg")));
            row.setField(10, Double.parseDouble(resultSet.getString("ALowerArmTemperatureMax")));
            row.setField(11, Double.parseDouble(resultSet.getString("BLowerArmTemperatureMax")));
            row.setField(12, Double.parseDouble(resultSet.getString("CLowerArmTemperatureMax")));

            row.setField(13, Double.parseDouble(resultSet.getString("ACableTemperatureAvg")));
            row.setField(14, Double.parseDouble(resultSet.getString("BCableTemperatureAvg")));
            row.setField(15, Double.parseDouble(resultSet.getString("CCableTemperatureAvg")));
            row.setField(16, Double.parseDouble(resultSet.getString("ACableTemperatureMax")));
            row.setField(17, Double.parseDouble(resultSet.getString("BCableTemperatureMax")));
            row.setField(18, Double.parseDouble(resultSet.getString("CCableTemperatureMax")));

            row.setField(19, end);
            row.setField(20, DateUtils.formatDate(new Date()));
            list.add(row);
        }
        ps.close();
        connection.close();
        dds.close();
        DataSource<Row> source = env.fromCollection(list);

        String insertQuery = "INSERT INTO  (IEDName,AUpperArmTemperatureAvg,BUpperArmTemperatureAvg,CUpperArmTemperatureAvg,AUpperArmTemperatureMax,BUpperArmTemperatureMax,CUpperArmTemperatureMax," +
                "ALowerArmTemperatureAvg,BLowerArmTemperatureAvg,CLowerArmTemperatureAvg,ALowerArmTemperatureMax,BLowerArmTemperatureMax,CLowerArmTemperatureMax" +
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
                Types.VARCHAR, Types.VARCHAR};
    }
}
