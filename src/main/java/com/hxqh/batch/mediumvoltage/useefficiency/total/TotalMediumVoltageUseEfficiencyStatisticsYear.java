package com.hxqh.batch.mediumvoltage.useefficiency.total;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * 整体中压设备-年度统计信息
 * <p>
 */
@SuppressWarnings("DuplicatedCode")
public class TotalMediumVoltageUseEfficiencyStatisticsYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        //放置es数据的List
        List<Tuple4<String, Double, Double, Double>> list = new ArrayList<>();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从db2查询asset信息，类型，型号，位置等
        String assetQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,productModelC from ASSET where ASSETYPE='中压开关设备'";
        JDBCInputFormat.JDBCInputFormatBuilder assetInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(assetQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO})).
                        setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        DataSource<Row> assetRow = env.createInput(assetInputBuilder.finish());
        DataSet<Tuple3<String, String, String>> assetDs = assetRow.map(new MapFunction<Row, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|" + row.getField(2).toString() + "|" + row.getField(3).toString() + "|" + row.getField(4).toString();
                return Tuple3.of(row.getField(0).toString(), key, "");
            }
        });


        //从mysql中取得设备的最大运行时间及对应月份，like年度
        String selectMaximumQuery = "select u.* from (SELECT IEDNAME,PARTICULARTIME,RUNNINGTIME,row_number() over(PARTITION BY IEDNAME ORDER BY RUNNINGTIME desc) AS rn FROM yc_medium_voltage_run_month where PARTICULARTIME like '" + RemindDateUtils.getLastYear() + "%' ) u where u.rn = 1";
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


        //从mysql中取得设备的最小运行时间及对应月份，like年度
        String selectMinimumQuery = "select u.* from (SELECT IEDNAME,PARTICULARTIME,RUNNINGTIME,row_number() over(PARTITION BY IEDNAME ORDER BY RUNNINGTIME asc) AS rn FROM yc_medium_voltage_run_month where PARTICULARTIME like '" + RemindDateUtils.getLastYear() + "%' ) u where u.rn = 1";
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


        DataSet<Tuple3<String, String, String>> mysqljoin = maximumDs.join(minimumDs).where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> join(Tuple3<String, String, Double> first, Tuple3<String, String, Double> second) throws Exception {
                        if (first.f2 > second.f2) {
                            return Tuple3.of(first.f0, second.f1 + "|" + second.f2, first.f1 + "|" + first.f2);
                        } else {
                            return Tuple3.of(first.f0, first.f1 + "|" + first.f2, second.f1 + "|" + second.f2);
                        }
                    }
                });


        //将从mysql中取得的月份负荷数据和从db2取得的设备表数据做链接join
        DataSet<Tuple3<String, String, String>> assetjoin = mysqljoin.join(assetDs).where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> join(Tuple3<String, String, String> first, Tuple3<String, String, String> second) throws Exception {
                        return Tuple3.of(second.f1, first.f1, first.f2);
                    }
                });

        DataSet<Tuple3<String, String, String>> join = assetjoin.groupBy(0).reduce(new ReduceFunction<Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> reduce(Tuple3<String, String, String> v1, Tuple3<String, String, String> v2) throws Exception {

                //最小和最小比
                if (Double.parseDouble(v1.f1.split("\\|")[1].toString()) > Double.parseDouble(v2.f1.split("\\|")[1].toString())) {
                    v1.f1 = v2.f1;
                }
                //最大和最大比
                if (Double.parseDouble(v2.f2.split("\\|")[1].toString()) > Double.parseDouble(v1.f2.split("\\|")[1].toString())) {
                    v1.f2 = v2.f2;
                }
                return v1;


            }
        });


        //es取数据开始
        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();


        //测试语句
        // String sqlFirst = "select assetYpe,productModel,location,productModelC,max(APhaseCurrent) as APhaseCurrentMax,max(BPhaseCurrent) as BPhaseCurrentMax,max(CPhaseCurrent) as CPhaseCurrentMax,min(APhaseCurrent) as APhaseCurrentMin,min(BPhaseCurrent) as BPhaseCurrentMin,min(CPhaseCurrent) as CPhaseCurrentMin,avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg from yc_mediumvoltage3 where ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59' group by assetYpe,productModel,location,productModelC";
        String sqlFirst = "select assetYpe,productModel,location,productModelC," +
                "max(APhaseCurrent) as APhaseCurrentMax,max(BPhaseCurrent) as BPhaseCurrentMax,max(CPhaseCurrent) as CPhaseCurrentMax," +
                "min(APhaseCurrent) as APhaseCurrentMin,min(BPhaseCurrent) as BPhaseCurrentMin,min(CPhaseCurrent) as CPhaseCurrentMin," +
                "avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg " +
                "from yc_mediumvoltage3 where ColTime>='" + start + "' and ColTime<='" + end + "' group by assetYpe,productModel,location,productModelC";

        PreparedStatement ps = connection.prepareStatement(sqlFirst);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String assetType = resultSet.getString("assetYpe");
            String productModel = resultSet.getString("location");
            String location = resultSet.getString("productModel");
            String productModelc = resultSet.getString("productModelC");
            String key = assetType + "|" + location + "|" + productModel + "|" + productModelc;
            //求得最大电流，最小电流和平均电流
            Double phaseCurrentMax = Math.max(Math.max(resultSet.getDouble("APhaseCurrentMax"), resultSet.getDouble("BPhaseCurrentMax")), resultSet.getDouble("CPhaseCurrentMax"));
            Double phaseCurrentMin = Math.min(Math.min(resultSet.getDouble("APhaseCurrentMin"), resultSet.getDouble("BPhaseCurrentMin")), resultSet.getDouble("CPhaseCurrentMin"));
            Double phaseCurrentAvg = (resultSet.getDouble("APhaseCurrentAvg") + resultSet.getDouble("BPhaseCurrentAvg") + resultSet.getDouble("CPhaseCurrentAvg")) / 3;

            list.add(Tuple4.of(key, phaseCurrentMax, phaseCurrentMin, phaseCurrentAvg));
        }
        DataSource<Tuple4<String, Double, Double, Double>> dataSet = env.fromCollection(list);

        ElasticSearchUtils.close(connection, ps, resultSet);
        //es取数据结束

        DataSet<Row> result = join.join(dataSet).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, String, String>, Tuple4<String, Double, Double, Double>, Row>() {
            @Override
            public Row join(Tuple3<String, String, String> first, Tuple4<String, Double, Double, Double> second) throws Exception {
                Row row = new Row(9);
                String[] split = first.f0.split("\\|");
                row.setField(0, split[0]);
                row.setField(1, split[1]);
                row.setField(2, split[2]);

                String[] split1 = first.f1.split("\\|");
                row.setField(3, split1[0]);
                String[] split2 = first.f2.split("\\|");
                row.setField(4, split2[0]);

                row.setField(5, second.f1);
                row.setField(6, second.f2);
                row.setField(7, second.f3);
                row.setField(8, RemindDateUtils.getLastYear());
                return row;
            }
        });


        String insertQuery = "INSERT INTO  RE_ALL_VOLTAGE_UE_INFO_YEAR(ASSETYPE,PRODUCTMODEL,LOCATION,MINMONTH,MAXMONTH,PHASECURRENTMAX,PHASECURRENTMIN,PHASECURRENTAVG,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalMediumVoltageUseEfficiencyStatisticsYear");

    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
