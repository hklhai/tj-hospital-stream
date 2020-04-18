package com.hxqh.batch.transformer.useefficiency;

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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
 */
@SuppressWarnings("DuplicatedCode")
public class TransformerUseEfficiencyStatisticsYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // 变压器获取上一季度中负荷最高和最低月份
        final TypeInformation<?>[] fieldTypes = getFieldTypes();

        RowTypeInfo transTypeInfo = new RowTypeInfo(fieldTypes);

        String transMaximumQuery = "select u.* from (SELECT IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT,LOADVALUE,CREATETIME,row_number() over(PARTITION BY IEDNAME ORDER BY LOADVALUE desc) AS rn FROM RE_TRANS_UE_MONTH where ASSETYPE='变压器' and CREATETIME  like '" + RemindDateUtils.getLastYear() + "%' ) u where u.rn = 1";
        System.out.println(transMaximumQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transMaximumQuery).setRowTypeInfo(transTypeInfo)
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        DataSource<Row> maxSource = env.createInput(inputBuilder.finish());
        DataSet<Tuple5<String, String,String, Double,String>> transMaxDs = maxSource.map(new MapFunction<Row, Tuple5<String, String,String, Double,String>>() {
            @Override
            public Tuple5<String, String,String, Double,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString();
                return Tuple5.of(row.getField(0).toString(), key , row.getField(6).toString(),Double.parseDouble(row.getField(7).toString()),row.getField(8).toString());
            }
        });

        String transMinimumQuery = "select u.* from (SELECT IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT,LOADVALUE,CREATETIME,row_number() over(PARTITION BY IEDNAME ORDER BY LOADVALUE asc) AS rn FROM RE_TRANS_UE_MONTH where ASSETYPE='变压器' and CREATETIME  like '" + RemindDateUtils.getLastYear() + "%' ) u where u.rn = 1";
        System.out.println(transMinimumQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputMinBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transMinimumQuery).setRowTypeInfo(transTypeInfo)
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        DataSource<Row> minSource = env.createInput(inputMinBuilder.finish());

        DataSet<Tuple5<String, String,String, Double,String>> transMinDs = minSource.map(new MapFunction<Row, Tuple5<String, String,String, Double,String>>() {
            @Override
            public Tuple5<String, String,String, Double,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString();
                return Tuple5.of(row.getField(0).toString(), key , row.getField(6).toString(),Double.parseDouble(row.getField(7).toString()),row.getField(8).toString());
            }
        });

        DataSet<Tuple5<String, String,String,String,String>> transjoin = transMaxDs.join(transMinDs).where(0).equalTo(0)
                .with(new JoinFunction<Tuple5<String, String,String, Double,String>, Tuple5<String, String,String, Double,String>, Tuple5<String, String,String,String,String>>() {
                    @Override
                    public Tuple5<String, String,String,String,String> join(Tuple5<String, String,String, Double,String> first, Tuple5<String, String,String, Double,String> second) throws Exception {
                        if (first.f3 > second.f3) {
                            return Tuple5.of(first.f0, first.f1,first.f2, second.f4, first.f4);
                        } else {
                            return Tuple5.of(first.f0, first.f1,first.f2, first.f4, second.f4);
                        }
                    }
                });
        transjoin.print();

        // 获取中压平均运行电流__A，最大__A，最小__A
        List<Tuple4<String, Double, Double, Double>> list = new ArrayList<>();
        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();

        String sql = "select IEDName,max(APhaseCurrent) as APhaseCurrentMax,max(BPhaseCurrent) as BPhaseCurrentMax,max(CPhaseCurrent) as CPhaseCurrentMax," +
                "min(APhaseCurrent) as APhaseCurrentMin,min(BPhaseCurrent) as BPhaseCurrentMin,min(CPhaseCurrent) as CPhaseCurrentMin," +
                "avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg " +
                "from yc_mediumvoltage3 where ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String iedName = resultSet.getString("IEDName");
            Double phaseCurrentMax = Math.max(Math.max(resultSet.getDouble("APhaseCurrentMax"), resultSet.getDouble("BPhaseCurrentMax")), resultSet.getDouble("CPhaseCurrentMax"));
            Double phaseCurrentMin = Math.min(Math.min(resultSet.getDouble("APhaseCurrentMin"), resultSet.getDouble("BPhaseCurrentMin")), resultSet.getDouble("CPhaseCurrentMin"));
            Double phaseCurrentAvg = (resultSet.getDouble("APhaseCurrentAvg") + resultSet.getDouble("BPhaseCurrentAvg") + resultSet.getDouble("CPhaseCurrentAvg")) / 3;
            list.add(Tuple4.of(iedName, phaseCurrentMax, phaseCurrentMin, phaseCurrentAvg));
        }
        DataSource<Tuple4<String, Double, Double, Double>> dataSet = env.fromCollection(list);
        ElasticSearchUtils.close(connection, ps, resultSet);

        DataSet<Row> result = transjoin.join(dataSet).where(2).equalTo(0).with(new JoinFunction<Tuple5<String, String,String,String,String>, Tuple4<String, Double, Double, Double>, Row>() {
            @Override
            public Row join(Tuple5<String, String,String,String,String> first, Tuple4<String, Double, Double, Double> second) throws Exception {
                Row row = new Row(13);
                row.setField(0, first.f0);
                String[] spilt = first.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);
                row.setField(4, spilt[3]);
                row.setField(5, spilt[4]);

                row.setField(6, first.f2);//所属父级
                row.setField(7, first.f3);//最小月份
                row.setField(8, first.f4);//最大月份

                row.setField(9, second.f1);
                row.setField(10, second.f2);
                row.setField(11, second.f3);
                row.setField(12, RemindDateUtils.getLastYear());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_TRANS_UE_INFO_YEAR(IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT," +
                "MINMONTH,MAXMONTH,PHASECURRENTMAX,PHASECURRENTMIN,PHASECURRENTAVG,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransformerUseEfficiencyStatisticsYear");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.VARCHAR};
    }


}
