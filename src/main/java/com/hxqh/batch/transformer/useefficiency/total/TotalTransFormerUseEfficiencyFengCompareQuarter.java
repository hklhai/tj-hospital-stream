package com.hxqh.batch.transformer.useefficiency.total;

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
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体风机运行时长对比-季度
 * <p>
 */
public class TotalTransFormerUseEfficiencyFengCompareQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String quarterQuery = "select ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC,RUNNINGTIME,CREATETIME from RE_ALL_TRANS_UE_FENG_Q where ASSETYPE='变压器'and CREATETIME in " + RemindDateUtils.getLastTwoQuarterString();
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
                        row.getField(3).toString(), row.getField(4).toString(),
                        Double.parseDouble(row.getField(5).toString()), row.getField(6).toString(), "", 0.0);
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


        MapOperator<Tuple9<String, String, String, String, String, Double, String, String, Double>, Row> result = reduce.map(new MapFunction<Tuple9<String, String, String, String, String, Double, String, String, Double>, Row>() {
            @Override
            public Row map(Tuple9<String, String, String, String, String, Double, String, String, Double> tuple) throws Exception {
                Row row = new Row(9);
                row.setField(0, tuple.f0);
                row.setField(1, tuple.f1);
                row.setField(2, tuple.f2);
                row.setField(3, tuple.f3);
                row.setField(4, tuple.f4);
                row.setField(5, tuple.f5);
                row.setField(6, tuple.f6);
                row.setField(7, tuple.f7);
                row.setField(8, tuple.f8);
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_ALL_TRANS_UE_FENG_COM_Q(ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC,RUNNINGTIME,CREATETIME,COMPARISON,RATIO) VALUES(?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransFormerUseEfficiencyFengCompareQuarter");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE};
    }
}
