package com.hxqh.batch.lowpressure.powersupply.total;

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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体低压电流负荷最高最低月份--季度
 *
 * Created by Ocean on 2020/4/21.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class TotalLowPressureCurrentLoadQuarter {

    public static void main(String[] args) throws Exception {
        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);

        String maxQuery = "select u.* from (SELECT IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,CREATETIME,PHASECURRENTAVG,row_number() over(PARTITION BY IEDNAME ORDER BY PHASECURRENTAVG desc) AS rn FROM RE_LP_CURRENT_MONTH where CREATETIME in " + RemindDateUtils.getLastQuarterString() + " ) u where u.rn = 1";
        JDBCInputFormat.JDBCInputFormatBuilder maxInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(maxQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        System.out.println(maxQuery);

        DataSet<Row> maxRow = env.createInput(maxInputBuilder.finish());


        DataSet<Tuple4<String, String,String,Double>> maxDes = maxRow.map(new MapFunction<Row, Tuple4<String, String,String,Double>>() {
            @Override
            public Tuple4<String, String,String,Double> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|" +row.getField(2).toString()
                        +"|"+row.getField(3).toString();
                return Tuple4.of(row.getField(0).toString(), key,row.getField(4).toString(),Double.parseDouble(row.getField(5).toString()));
            }
        });
        maxDes.print();

        String minQuery = "select u.* from (SELECT IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,CREATETIME,PHASECURRENTAVG,row_number() over(PARTITION BY IEDNAME ORDER BY PHASECURRENTAVG asc) AS rn FROM RE_LP_CURRENT_MONTH where CREATETIME in " + RemindDateUtils.getLastQuarterString() + " ) u where u.rn = 1";
        System.out.println(minQuery);
        JDBCInputFormat.JDBCInputFormatBuilder minInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(minQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> minRow = env.createInput(minInputBuilder.finish());


        DataSet<Tuple4<String, String,String,Double>> minDes = minRow.map(new MapFunction<Row, Tuple4<String, String,String,Double>>() {
            @Override
            public Tuple4<String, String,String,Double> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|" +row.getField(2).toString()
                        +"|"+row.getField(3).toString();
                return Tuple4.of(row.getField(0).toString(), key,row.getField(4).toString(),Double.parseDouble(row.getField(5).toString()));
            }
        });
        minDes.print();

        DataSet<Tuple3<String, String, String>> join = maxDes.join(minDes).where(0).equalTo(0)
                .with(new JoinFunction< Tuple4<String, String,String,Double>,Tuple4<String, String,String,Double>,Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> join(Tuple4<String, String,String,Double> first, Tuple4<String, String,String,Double> second) throws Exception {
                        if (first.f3 > second.f3) {
                            return Tuple3.of(first.f1, second.f2+"|"+second.f3, first.f2+"|"+first.f3);
                        } else {
                            return Tuple3.of(first.f1, first.f2+"|"+first.f3, second.f2+"|"+second.f3);
                        }
                    }
                });
        join.print();

        DataSet<Tuple3<String, String, String>> totalJoin = join.groupBy(0).reduce(new ReduceFunction<Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> reduce(Tuple3<String, String, String> v1, Tuple3<String, String, String> v2) throws Exception {
                String[] spitvmin1 =  v1.f1.split("\\|");
                String[] spitvmax1 =  v1.f2.split("\\|");
                String[] spitvmin2 =  v2.f1.split("\\|");
                String[] spitvmax2 =  v2.f2.split("\\|");

                Double currentmin1 = Double.parseDouble(spitvmin1[1].toString());
                Double currentmin2 = Double.parseDouble(spitvmin2[1].toString());
                Double currentmax1 = Double.parseDouble(spitvmax1[1].toString());
                Double currentmax2 = Double.parseDouble(spitvmax2[1].toString());

                if(currentmin1>currentmin2){
                    v1.f1 = v2.f1;
                }
                if(currentmax2>currentmax1){
                    v1.f2 = v2.f2;
                }
                return v1;


            }
        });

        totalJoin.print();

        DataSet<Row> result = totalJoin.map(new MapFunction<Tuple3<String, String, String>, Row>() {
            @Override
            public Row map(Tuple3<String, String, String> value) throws Exception {
                Row row = new Row(6);
                String[] spilt = value.f0.split("\\|");
                row.setField(0, spilt[0]);
                row.setField(1, spilt[1]);
                row.setField(2, spilt[2]);

                String[] split1 = value.f1.split("\\|");
                row.setField(3, split1[0]);
                String[] split2 = value.f2.split("\\|");
                row.setField(4, split2[0]);

                row.setField(5,RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_ALL_LP_CURRENT_LOAD_Q (ASSETYPE,LOCATION,PRODUCTMODELC,MINMONTH,MAXMONTH,CREATETIME)" +
                " VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalLowPressureCurrentLoadQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.BIG_DEC_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.VARCHAR};
    }

}

