package com.hxqh.batch.transformer.useefficiency;

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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 每月第2天生成上月电流，电压，负荷等数据
 * <p>
 */
@SuppressWarnings("Duplicates")
public class TransformerUseEfficiencyMonth {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        // 变压器
        RowTypeInfo transTypeInfo = new RowTypeInfo(fieldTypes);
        String transQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT from ASSET where ASSETYPE='变压器'";
        System.out.println(transQuery);
        JDBCInputFormat.JDBCInputFormatBuilder transInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transQuery).setRowTypeInfo(transTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> transRow = env.createInput(transInputBuilder.finish());

        DataSet<Tuple3<String, String,String>> transData = transRow.map(new MapFunction<Row, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString();
                return Tuple3.of(row.getField(0).toString(), key, row.getField(6).toString());
            }
        });



        // 获取中压平均电流和电压，求得负荷率
        List<Tuple3<String, String, Double>> list = new ArrayList<>();
        Connection connection = ElasticSearchUtils.getConnection();
        //获取上个月的第一天和最后一天
        DataStartEnd startEnd = RemindDateUtils.getLastMonthStartEndTime();
//        String start = startEnd.getStart();
//        String end = startEnd.getEnd();
        String start = "2020-03-01 00:00:00";
        String end = "2020-03-31 23:59:59";


        String sql = "select IEDName,avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg, " +
                "assetYpe,productModel,avg(ABLineVoltage) as ABLineVoltageAvg,avg(BCLineVoltage) as BCLineVoltageAvg,avg(CALineVoltage) as CALineVoltageAvg "+
                "from yc_mediumvoltage3 where assetYpe='中压开关设备' and ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String iedName = resultSet.getString("IEDName");
           // String assetYpe = resultSet.getString("assetYpe");

            //上游侧父资产的平均电流值
            Double phaseCurrentAvg = (resultSet.getDouble("APhaseCurrentAvg") + resultSet.getDouble("BPhaseCurrentAvg") + resultSet.getDouble("CPhaseCurrentAvg")) / 3;

            //上游侧父资产的平均电压值
            Double lineVoltageAvg = (resultSet.getDouble("ABLineVoltageAvg") + resultSet.getDouble("BCLineVoltageAvg") + resultSet.getDouble("CALineVoltageAvg")) / 3;
            //负荷
            Double loadValue = phaseCurrentAvg * lineVoltageAvg;

            list.add(Tuple3.of(iedName,"中压开关设备", loadValue));
        }
        DataSource<Tuple3<String, String, Double>> dataSet = env.fromCollection(list);
        ElasticSearchUtils.close(connection, ps, resultSet);


        DataSet<Tuple3<String, String, Double>> reduce = dataSet.groupBy(0).reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> reduce(Tuple3<String, String, Double> v1, Tuple3<String, String, Double> v2) throws Exception {
                if(v1.f2>v2.f2){
                    return v1;
                }else{
                    return v2;
                }
            }
        });


        DataSet<Tuple4<String, String, String,Double>> join = transData.join(reduce).where(2).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, Double>,Tuple4<String, String, String,Double>>() {
                    @Override
                    public Tuple4<String, String, String,Double> join(Tuple3<String, String, String> first, Tuple3<String, String, Double> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1,first.f2, second.f2);
                    }
                });


        DataSet<Row> result = join.map(new MapFunction<Tuple4<String, String,String, Double>, Row>() {
            @Override
            public Row map(Tuple4<String, String,String, Double> value) throws Exception {
                Row row = new Row(9);
                row.setField(0, value.f0);
                String[] spilt = value.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);
                row.setField(4, spilt[3]);
                row.setField(5, spilt[4]);

                row.setField(6, value.f2);
                row.setField(7, value.f3);
                row.setField(8, RemindDateUtils.getLastMonth());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_TRANS_UE_MONTH (IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT,LOADVALUE,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransformerUseEfficiencyMonth");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,Types.DOUBLE, Types.VARCHAR};
    }

}

