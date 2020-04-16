package com.hxqh.batch.transformer.health;

import com.hxqh.enums.ChangeEnum;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 单台开关柜使用效率运行时长对比-年度
 *
 * Created by Ocean lin on 2020/4/14.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class TransFormerHealthCompareYear {


    public static void main(String[] args) throws Exception {

        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String quarterQuery = "select IEDNAME,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELC,SCORE,CREATETIME from RE_TRANS_HEALTH_YEAR where ASSETYPE='变压器' and CREATETIME in " + RemindDateUtils.getLastTwoYearString();
        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());
        DataSet<Tuple9<String, String, String, String, String, Double, String, String, Double>> quarter = quarterRow.map(new MapFunction<Row, Tuple9<String, String, String, String, String, Double, String, String, Double>>() {
            @Override
            public Tuple9<String, String, String, String, String, Double, String, String, Double> map(Row row) throws Exception {
                return Tuple9.of(row.getField(0).toString(), row.getField(1).toString(), row.getField(2).toString(),
                        row.getField(3).toString(), row.getField(4).toString(), Double.parseDouble(row.getField(5).toString()),
                        row.getField(6).toString(), "", 0.0);
            }
        });

        DataSet<Tuple9<String, String, String, String, String, Double, String, String, Double>> reduce = quarter.groupBy(0).reduce(new ReduceFunction<Tuple9<String, String, String, String, String, Double, String, String, Double>>() {
            @Override
            public Tuple9<String, String, String, String, String, Double, String, String, Double> reduce(Tuple9<String, String, String, String, String, Double, String, String, Double> v1, Tuple9<String, String, String, String, String, Double, String, String, Double> v2) throws Exception {
                if (v1.f6.compareTo(v2.f6) == 1) {
                    Double div = (v1.f5 - v2.f5) / v2.f5;
                    v1.f8 = div;
                    if (div > -Proportion && div < Proportion) {
                        v1.f7 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v1.f7 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v1.f7 = ChangeEnum.Increased.getCode();
                    }
                    return v1;
                } else {
                    Double div = (v2.f5 - v1.f5) / v1.f5;
                    v2.f8 = div;
                    if (div > -Proportion && div < Proportion) {
                        v2.f7 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v2.f7 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v2.f7 = ChangeEnum.Increased.getCode();
                    }
                    return v2;
                }
            }
        });


        DataSet<Row> result = reduce.map(new MapFunction<Tuple9<String, String, String, String, String, Double, String, String, Double>, Row>() {
            @Override
            public Row map(Tuple9<String, String, String, String, String, Double, String, String, Double> value) throws Exception {
                Row row = new Row(9);
                row.setField(0, value.f0);
                row.setField(1, value.f1);
                row.setField(2, value.f2);
                row.setField(3, value.f3);
                row.setField(4, value.f4);
                row.setField(5, (int) Double.parseDouble(value.f5.toString()));
                row.setField(6, value.f6);
                row.setField(7, value.f7);
                row.setField(8, value.f8);
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_TRANS_HEA_COM_YEAR (IEDNAME,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELC,SCORE,CREATETIME,COMPARISON,RATIO) VALUES(?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransFormerHealthCompareYear");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE
        };
    }

}

