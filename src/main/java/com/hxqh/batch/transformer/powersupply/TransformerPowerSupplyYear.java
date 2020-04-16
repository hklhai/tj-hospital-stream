package com.hxqh.batch.transformer.powersupply;

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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 单台变压器供用电状况--季度
 * <p>
 * Created by Ocean on 2020/4/13.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class TransformerPowerSupplyYear {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // asset
        RowTypeInfo assetTypeInfo = new RowTypeInfo(fieldTypes);
        String selectQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT from ASSET where ASSETYPE='变压器'";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(assetTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> assetRow = env.createInput(inputBuilder.finish());

        DataSet<Tuple3<String, String,String>> asset = assetRow.map(new MapFunction<Row, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString();
                return Tuple3.of(row.getField(0).toString(), key, row.getField(6).toString());
            }
        });



        //低压开关设备

        String lowerQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT " +
                "from ASSET where ASSETYPE like '低压开关设备%'";
        JDBCInputFormat.JDBCInputFormatBuilder lowerinputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowerQuery).setRowTypeInfo(assetTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> lowerassetRow = env.createInput(lowerinputBuilder.finish());

        DataSet<Tuple3<String, String,String>> lowerasset = lowerassetRow.map(new MapFunction<Row, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString();
                return Tuple3.of(row.getField(0).toString(), key, row.getField(6).toString());
            }
        });

        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();


        //上游数据
        List<Tuple3<String,Double,Double>> upperList = new ArrayList<>();
        Connection upperConnection = ElasticSearchUtils.getConnection();



        String upperSql = "select IEDName,max(PositiveActive) as PositiveActiveMax,min(PositiveActive) as PositiveActiveMin," +
                "avg(APhaseCurrent) as APhaseCurrentAvg,avg(BPhaseCurrent) as BPhaseCurrentAvg,avg(CPhaseCurrent) as CPhaseCurrentAvg," +
                "avg(ABLineVoltage) as ABLineVoltageAvg,avg(BCLineVoltage) as BCLineVoltageAvg,avg(CALineVoltage) as CALineVoltageAvg from yc_mediumvoltage3 " +
                "where assetYpe= '中压开关设备' and ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";
        PreparedStatement upperPs = upperConnection.prepareStatement(upperSql);
        ResultSet upperResultSet = upperPs.executeQuery();
        while (upperResultSet.next()) {
            String iedName = upperResultSet.getString("IEDName");
            //上游侧父资产的平均电流值
            Double phaseCurrentAvg = (upperResultSet.getDouble("APhaseCurrentAvg") + upperResultSet.getDouble("BPhaseCurrentAvg") + upperResultSet.getDouble("CPhaseCurrentAvg")) / 3;
            //上游侧父资产的平均电压值
            Double lineVoltageAvg = (upperResultSet.getDouble("ABLineVoltageAvg") + upperResultSet.getDouble("BCLineVoltageAvg") + upperResultSet.getDouble("CALineVoltageAvg")) / 3;
            //负荷
            Double loadValue = phaseCurrentAvg * lineVoltageAvg;
            //上游有功电度量最大,最小
            Double positiveActiveMax = upperResultSet.getDouble("PositiveActiveMax");

            Double positiveActiveMin = upperResultSet.getDouble("PositiveActiveMin");

            Double activeElectricDegree = positiveActiveMax-positiveActiveMin;

            upperList.add(Tuple3.of(iedName, loadValue,activeElectricDegree));
        }

        DataSource<Tuple3<String, Double,Double>> upperDataSet = env.fromCollection(upperList);


        //下游数据：低压  assetYpe= '中压开关设备' and
        List<Tuple2<String,Double>> lowerList = new ArrayList<>();
        Connection lowerConnection = ElasticSearchUtils.getConnection();
        String lowerSql = "select IEDName,max(PositiveActive) as PositiveActiveMax," +
                "min(PositiveActive) as PositiveActiveMin from yc_lowpressure " +
                "where assetYpe like '低压开关设备%' and ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";
        PreparedStatement lowerPs = lowerConnection.prepareStatement(lowerSql);
        ResultSet lowerResultSet = lowerPs.executeQuery();
        while (lowerResultSet.next()) {
            String iedName = lowerResultSet.getString("IEDName");

            //下游有功电度量最大,最小
            Double positiveActiveMax = lowerResultSet.getDouble("PositiveActiveMax");

            Double positiveActiveMin = lowerResultSet.getDouble("PositiveActiveMin");

            Double activeElectricDegree = positiveActiveMax-positiveActiveMin;

            lowerList.add(Tuple2.of(iedName, activeElectricDegree));
        }

        DataSource<Tuple2<String, Double>> lowerDataSet = env.fromCollection(lowerList);


        ElasticSearchUtils.close(upperConnection, upperPs, upperResultSet);
        ElasticSearchUtils.close(lowerConnection, lowerPs, lowerResultSet);


        DataSet<Tuple5<String, String, String,Double,Double>> upperjoin = asset.join(upperDataSet).where(2).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, Double, Double>,Tuple5<String, String, String,Double,Double>>() {
                    @Override
                    public Tuple5<String, String, String,Double,Double> join(Tuple3<String, String, String> first, Tuple3<String, Double, Double> second) throws Exception {
                        return Tuple5.of(first.f0, first.f1,first.f2, second.f1,second.f2);
                    }
                });


        DataSet<Tuple4<String, String, String,Double>> lowerjoin = lowerasset.join(lowerDataSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, String>,Tuple2<String, Double>,Tuple4<String, String, String,Double>>() {
                    @Override
                    public Tuple4<String, String, String,Double> join(Tuple3<String, String, String> first, Tuple2<String, Double> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1,first.f2, second.f1);
                    }
                });


        DataSet<Tuple5<String, String, String,Double,Double>> join = upperjoin.join(lowerjoin).where(0).equalTo(2)
                .with(new JoinFunction<Tuple5<String, String, String,Double,Double>, Tuple4<String, String, String,Double>,Tuple5<String, String, String,Double,Double>>() {
                    @Override
                    public Tuple5<String, String, String,Double,Double> join(Tuple5<String, String, String,Double,Double> first, Tuple4<String, String, String,Double> second) throws Exception {
                        return Tuple5.of(first.f0, first.f1,first.f2, first.f3,first.f4-second.f3);
                    }
                });


        DataSet<Row> result = join.map(new MapFunction<Tuple5<String, String, String,Double,Double>, Row>() {
            @Override
            public Row map(Tuple5<String, String, String,Double,Double> value) throws Exception {
                Row row = new Row(13);
                row.setField(0, value.f0);//设备编码
                String[] spilt = value.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);
                row.setField(4, spilt[3]);
                row.setField(5, spilt[4]);

                Double totalCount = Double.parseDouble(spilt[3].substring(0,spilt[3].lastIndexOf("kVA")));

                row.setField(6, value.f2);//parent
                row.setField(7, value.f3);//负荷
                Double loadRatio = value.f3/totalCount;
                row.setField(8, loadRatio);//负荷率
                row.setField(9, RemindDateUtils.getLastYear());//时间-季度
                row.setField(10, value.f4);//消耗
                Double losRatio = value.f4/totalCount;
                row.setField(11, losRatio);//消耗率

                BigDecimal data1 = new BigDecimal(losRatio);
                BigDecimal data2 = new BigDecimal(0.1);
                String losDesc = "";
                //总损耗占比10%以下为合理
                if(data1.compareTo(data2)>0){
                    //证明>0.1
                    losDesc = "关注";
                }else{
                    losDesc = "合理";
                }
                row.setField(12, losDesc);//消耗描述
                return row;
            }
        });


        result.print();


        String insertQuery = "INSERT INTO RE_TRANS_PS_YEAR (IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT,LOADVALUE,LOADRATIO,CREATETIME,LOSSVALUE,LOSSRATIO,LOSSDESC) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransformerPowerSupplyYear");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,
                Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE,Types.VARCHAR
        };
    }

}

