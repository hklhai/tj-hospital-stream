package com.hxqh.batch.lowpressure.powersupply;

import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 单台低压电流负荷最高最低月份--季度
 *
 * Created by Ocean on 2020/4/20.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class LowPressureCurrentLoadQuarter {

    public static void main(String[] args) throws Exception {
        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);

        String maxQuery = "select u.* from (SELECT IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB,CREATETIME,PHASECURRENTAVG,row_number() over(PARTITION BY IEDNAME ORDER BY PHASECURRENTAVG desc) AS rn FROM RE_LP_CURRENT_MONTH where CREATETIME in " + RemindDateUtils.getLastQuarterString() + " ) u where u.rn = 1";
        JDBCInputFormat.JDBCInputFormatBuilder maxInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(maxQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        System.out.println(maxQuery);

        DataSet<Row> maxRow = env.createInput(maxInputBuilder.finish());
        maxRow.print();

        DataSet<Tuple4<String, String,String,Double>> maxDes = maxRow.map(new MapFunction<Row, Tuple4<String, String,String,Double>>() {
            @Override
            public Tuple4<String, String,String,Double> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|" +row.getField(2).toString()
                        +"|"+row.getField(3).toString()+ "|" +row.getField(4).toString();
                return Tuple4.of(row.getField(0).toString(), key,row.getField(5).toString(),Double.parseDouble(row.getField(6).toString()));
            }
        });

        String minQuery = "select u.* from (SELECT IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB,CREATETIME,PHASECURRENTAVG,row_number() over(PARTITION BY IEDNAME ORDER BY PHASECURRENTAVG asc) AS rn FROM RE_LP_CURRENT_MONTH where CREATETIME in " + RemindDateUtils.getLastQuarterString() + " ) u where u.rn = 1";
        JDBCInputFormat.JDBCInputFormatBuilder minInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(minQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> minRow = env.createInput(minInputBuilder.finish());
        minRow.print();

        DataSet<Tuple4<String, String,String,Double>> minDes = minRow.map(new MapFunction<Row, Tuple4<String, String,String,Double>>() {
            @Override
            public Tuple4<String, String,String,Double> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|" +row.getField(2).toString()
                        +"|"+row.getField(3).toString()+ "|" +row.getField(4).toString();
                return Tuple4.of(row.getField(0).toString(), key,row.getField(5).toString(),Double.parseDouble(row.getField(6).toString()));
            }
        });

        DataSet<Tuple4<String, String, String,String>> join = maxDes.join(minDes).where(0).equalTo(0)
                .with(new JoinFunction< Tuple4<String, String,String,Double>,Tuple4<String, String,String,Double>,Tuple4<String, String, String,String>>() {
                    @Override
                    public Tuple4<String, String, String,String> join(Tuple4<String, String,String,Double> first, Tuple4<String, String,String,Double> second) throws Exception {
                        if (first.f3 > second.f3) {
                            return Tuple4.of(first.f0, first.f1, second.f2, first.f2);
                        } else {
                            return Tuple4.of(first.f0, first.f1, first.f2, second.f2);
                        }
                    }
                });

        DataSet<Row> result = join.map(new MapFunction<Tuple4<String, String, String,String>, Row>() {
            @Override
            public Row map(Tuple4<String, String, String,String> value) throws Exception {
                Row row = new Row(8);
                row.setField(0, value.f0);
                String[] spilt = value.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);
                row.setField(4, spilt[3]);

                row.setField(5, value.f2);
                row.setField(6, value.f3);
                row.setField(7,RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_LP_CURRENT_LOAD_Q (IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB,MINMONTH,MAXMONTH,CREATETIME)" +
                " VALUES(?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowPressureCurrentLoadQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.BIG_DEC_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR,Types.VARCHAR,Types.VARCHAR, Types.VARCHAR};
    }

}

