package com.hxqh.batch.mediumvoltage.useefficiency;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 单台中压设备-季度统计信息
 * <p>
 * Created by Ocean lin on 2020/4/9.
 *
 * @author Ocean lin
 */
@SuppressWarnings("DuplicatedCode")
public class MediumVoltageUseEfficiencyStatisticsQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String selectMaximumQuery = "select u.* from (SELECT IEDNAME,PARTICULARTIME,RUNNINGTIME,row_number() over(PARTITION BY IEDNAME ORDER BY RUNNINGTIME desc) AS rn FROM yc_medium_voltage_run_month where PARTICULARTIME in " + RemindDateUtils.getLastQuarterString() + " ) u where u.rn = 1";
        System.out.println(selectMaximumQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(MYSQL_DRIVER_NAME).setDBUrl(MYSQL_DB_URL)
                        .setQuery(selectMaximumQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO}))
                        .setUsername(MYSQL_USERNAME).setPassword(MYSQL_PASSWORD);
        DataSource<Row> source = env.createInput(inputBuilder.finish());
        DataSet<Tuple3<String, String, Double>> maximumDs = source.map(new MapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Row row) throws Exception {
                return Tuple3.of(row.getField(0).toString(), row.getField(1).toString(), Double.parseDouble(row.getField(2).toString()));
            }
        });

        String selectMinimumQuery = "select u.* from (SELECT IEDNAME,PARTICULARTIME,RUNNINGTIME,row_number() over(PARTITION BY IEDNAME ORDER BY RUNNINGTIME asc) AS rn FROM yc_medium_voltage_run_month where PARTICULARTIME in " + RemindDateUtils.getLastQuarterString() + " ) u where u.rn = 1";
        System.out.println(selectMaximumQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputMinBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(MYSQL_DRIVER_NAME).setDBUrl(MYSQL_DB_URL)
                        .setQuery(selectMinimumQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO}))
                        .setUsername(MYSQL_USERNAME).setPassword(MYSQL_PASSWORD);
        DataSource<Row> minSource = env.createInput(inputMinBuilder.finish());
        DataSet<Tuple3<String, String, Double>> minimumDs = minSource.map(new MapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Row row) throws Exception {
                return Tuple3.of(row.getField(0).toString(), row.getField(1).toString(), Double.parseDouble(row.getField(2).toString()));
            }
        });
        DataSet<Tuple3<String, String, String>> join = maximumDs.join(minimumDs).where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> join(Tuple3<String, String, Double> first, Tuple3<String, String, Double> second) throws Exception {
                        if (first.f2 > second.f2) {
                            return Tuple3.of(first.f0, second.f1, first.f1);
                        } else {
                            return Tuple3.of(first.f0, first.f1, second.f1);
                        }
                    }
                });

        // 获取平均运行电流__A，最大__A，最小__A
        List<Tuple4<String, Double, Double, Double>> list = new ArrayList<>();
        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastQuarterStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();

//        String sql = "select IEDName,max(APhaseCurrent) as APhaseCurrentMax,max(BPhaseCurrent) as BPhaseCurrentMax,max(CPhaseCurrent) as CPhaseCurrentMax,min(APhaseCurrent) as APhaseCurrentMin,min(BPhaseCurrent) as BPhaseCurrentMin,min(CPhaseCurrent) as CPhaseCurrentMin,avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg from yc_mediumvoltage3 where ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59' group by IEDName";
        String sql = "select IEDName,max(APhaseCurrent) as APhaseCurrentMax,max(BPhaseCurrent) as BPhaseCurrentMax,max(CPhaseCurrent) as CPhaseCurrentMax,min(APhaseCurrent) as APhaseCurrentMin,min(BPhaseCurrent) as BPhaseCurrentMin,min(CPhaseCurrent) as CPhaseCurrentMin,avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg from yc_mediumvoltage3 where ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String iedName = resultSet.getString("IEDName");
            Double phaseCurrentMax = Math.max(Math.max(resultSet.getDouble("APhaseCurrentMax"), resultSet.getDouble("BPhaseCurrentMax")), resultSet.getDouble("CPhaseCurrentMax"));
            Double phaseCurrentMin = Math.max(Math.max(resultSet.getDouble("APhaseCurrentMin"), resultSet.getDouble("BPhaseCurrentMin")), resultSet.getDouble("CPhaseCurrentMin"));
            Double phaseCurrentAvg = (resultSet.getDouble("APhaseCurrentAvg") + resultSet.getDouble("BPhaseCurrentAvg") + resultSet.getDouble("CPhaseCurrentAvg")) / 3;
            list.add(Tuple4.of(iedName, phaseCurrentMax, phaseCurrentMin, phaseCurrentAvg));
        }
        DataSource<Tuple4<String, Double, Double, Double>> dataSet = env.fromCollection(list);
        ElasticSearchUtils.close(connection, ps, resultSet);

        DataSet<Row> result = join.join(dataSet).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, String, String>, Tuple4<String, Double, Double, Double>, Row>() {
            @Override
            public Row join(Tuple3<String, String, String> first, Tuple4<String, Double, Double, Double> second) throws Exception {
                Row row = new Row(7);
                row.setField(0, first.f0);
                row.setField(1, first.f1);
                row.setField(2, first.f2);
                row.setField(3, second.f1);
                row.setField(4, second.f2);
                row.setField(5, second.f3);
                row.setField(6, RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        String insertQuery = "INSERT INTO  RE_VOLTAGE_UE_INFO_QUARTER(IEDNAME,MINMONTH,MAXMONTH,PHASECURRENTMAX,PHASECURRENTMIN,PHASECURRENTAVG,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("MediumVoltageUseEfficiencyStatisticsQuarter");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
