package com.hxqh.batch.mediumvoltage;

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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/3/13.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MediumVoltageCompareYear {


    public static void main(String[] args) throws Exception {

        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String quarterQuery = "select IEDNAME,ASSETYPE,SCORE,CREATETIME from RE_SCORE_YEAR where CREATETIME in " + RemindDateUtils.getLastTwoYearString();
        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());
        DataSet<Tuple6<String, String, Double, String, String, Double>> quarter = quarterRow.map(new MapFunction<Row, Tuple6<String, String, Double, String, String, Double>>() {
            @Override
            public Tuple6<String, String, Double, String, String, Double> map(Row row) throws Exception {
                return Tuple6.of(row.getField(0).toString(), row.getField(1).toString(),
                        Double.parseDouble(row.getField(2).toString()), row.getField(3).toString(), "", 0.0);
            }
        });

        DataSet<Tuple6<String, String, Double, String, String, Double>> reduce = quarter.groupBy(0).reduce(new ReduceFunction<Tuple6<String, String, Double, String, String, Double>>() {
            @Override
            public Tuple6<String, String, Double, String, String, Double> reduce(Tuple6<String, String, Double, String, String, Double> v1, Tuple6<String, String, Double, String, String, Double> v2) throws Exception {
                if (v1.f3.compareTo(v2.f3) == 1) {
                    Double div = (v1.f2 - v2.f2) / v2.f2;
                    v1.f5 = div;
                    if (div > -Proportion && div < Proportion) {
                        v1.f4 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v1.f4 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v1.f4 = ChangeEnum.Increased.getCode();
                    }
                    return v1;
                } else {
                    Double div = (v2.f2 - v1.f2) / v1.f2;
                    v2.f5 = div;
                    if (div > -Proportion && div < Proportion) {
                        v2.f4 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v2.f4 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v2.f4 = ChangeEnum.Increased.getCode();
                    }
                    return v2;
                }
            }
        });


        DataSet<Row> result = reduce.map(new MapFunction<Tuple6<String, String, Double, String, String, Double>, Row>() {
            @Override
            public Row map(Tuple6<String, String, Double, String, String, Double> value) throws Exception {
                Row row = new Row(6);
                row.setField(0, value.f0);
                row.setField(1, value.f1);
                row.setField(2, value.f2);
                row.setField(3, value.f3);
                row.setField(4, value.f4);
                row.setField(5, value.f5);
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_COMPARE_YEAR (IEDNAME,ASSETYPE,SCORE,CREATETIME,COMPARISON,RATIO) VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("MediumVoltageCompareQuarter");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE,
        };
    }

}

