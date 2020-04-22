package com.hxqh.batch.lowpressure.powersupply;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.enums.PercentageRreactive;
import com.hxqh.task.sink.MySQLYxFanSink;
import com.hxqh.utils.DateUtils;
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
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import scala.tools.cmd.gen.AnyVals;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 每月第2天生成下月电流数据
 *
 * Created by Ocean on 2020/4/21.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class LowPressureCurrentMonth {

    public static void main(String[] args) throws Exception {
        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // asset
        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        String lowQuery = "select ASSETNUM,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB from ASSET where ASSETYPE like '低压开关设备%'";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> assetRow = env.createInput(inputBuilder.finish());

        DataSet<Tuple3<String, String,String>> assetMap = assetRow.map(new MapFunction<Row, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|" +row.getField(2).toString()+"|"+row.getField(3).toString();
                return Tuple3.of(row.getField(0).toString(), key,row.getField(4).toString());
            }
        });
       // assetMap.print();

        //从es中取得低压设备的电流和电压
        //放置es数据的List
        List<Tuple2<String, Double>> lowList = new ArrayList<>();

        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastMonthStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
        //测试数据
//        String start = "2020-04-01 00:00:00";
//        String end = "2020-05-31 23:59:59";

        //注意这里es中是区分大小写的
        String sqlEs = "select IEDName," +
                "avg(PhaseL1CurrentPercent) as PhaseL1CurrentPercentAvg,avg(PhaseL2CurrentPercent) as PhaseL2CurrentPercentAvg,avg(PhaseL3CurrentPercent) as PhaseL3CurrentPercentAvg " +
                "from yc_lowpressure where ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";

        System.out.println(sqlEs);
        PreparedStatement ps = connection.prepareStatement(sqlEs);

        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            String iedName = resultSet.getString("IEDName");
            //求得平均电流百分比
            Double phaseCurrentPercentAvg = (resultSet.getDouble("PhaseL1CurrentPercentAvg") + resultSet.getDouble("PhaseL2CurrentPercentAvg") + resultSet.getDouble("PhaseL3CurrentPercentAvg")) / 3;

            //测试数据
            //            String iedName = "2AA8-7";
            //            Double phaseCurrentPercentAvg = 0.78;
            lowList.add(Tuple2.of(iedName, phaseCurrentPercentAvg/100));
        }

        DataSource<Tuple2<String, Double>> dataSet = env.fromCollection(lowList);


        ElasticSearchUtils.close(connection, ps, resultSet);
        //es取数据结束

        DataSet<Tuple4<String, String, String,Double>> join = assetMap.join(dataSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, String>, Tuple2<String, Double>,Tuple4<String, String, String,Double>>() {
                    @Override
                    public Tuple4<String, String, String,Double> join(Tuple3<String, String, String> first, Tuple2<String, Double> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1,first.f2, second.f1);
                    }
                });

        DataSet<Row> result = join.map(new MapFunction<Tuple4<String, String, String,Double>, Row>() {
            @Override
            public Row map(Tuple4<String, String, String,Double> value) throws Exception {
                Row row = new Row(8);
                row.setField(0, value.f0);
                String[] spilt = value.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);

                row.setField(4, value.f2);
                row.setField(5, value.f3);
                Double currenAvg = value.f3;
                if(value.f2.contains("A")){
                    currenAvg = Double.parseDouble(value.f2.substring(0,value.f2.lastIndexOf("A")))*currenAvg;
                }
                row.setField(6, currenAvg);
                row.setField(7,RemindDateUtils.getLastMonth());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_LP_CURRENT_MONTH (IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB,PHASECURRENTPERCENTAVG,PHASECURRENTAVG,CREATETIME)" +
                " VALUES(?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowPressureCurrentMonth");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO};
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.DOUBLE,Types.DOUBLE, Types.VARCHAR};
    }

}

