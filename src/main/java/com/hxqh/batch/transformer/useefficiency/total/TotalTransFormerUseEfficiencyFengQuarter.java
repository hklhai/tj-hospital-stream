package com.hxqh.batch.transformer.useefficiency.total;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体风机运行时长-季度
 * <p>
 */
public class TotalTransFormerUseEfficiencyFengQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataStartEnd startEnd = RemindDateUtils.getLastQuarterStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
//        String start = "2019-03-01 00:00:00";
//        String end = "2020-05-30 23:59:59";

        String quarterQuery = "select ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC,SUM(RUNNINGTIME) from RE_TRANS_PS_RUN_MONTH where ASSETYPE='变压器' and ColTime>='" + start + "' and ColTime<='" + end + "'  group by IEDNAME,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC";
        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());
        DataSet<Tuple6<String, String, String, String, String, Double>> quarter = quarterRow.map(new MapFunction<Row, Tuple6<String, String, String, String, String, Double>>() {
            @Override
            public Tuple6<String, String, String, String, String, Double> map(Row row) throws Exception {
                return Tuple6.of(row.getField(0).toString(), row.getField(1).toString(), row.getField(2).toString(),
                        row.getField(3).toString(), row.getField(4).toString(), Double.parseDouble(row.getField(5).toString()));
            }
        });

        MapOperator<Tuple6<String, String, String, String, String, Double>, Row> result = quarter.map(new MapFunction<Tuple6<String, String, String, String, String, Double>, Row>() {
            @Override
            public Row map(Tuple6<String, String, String, String, String, Double> tuple) throws Exception {
                Row row = new Row(7);
                row.setField(0, tuple.f0);
                row.setField(1, tuple.f1);
                row.setField(2, tuple.f2);
                row.setField(3, tuple.f3);
                row.setField(4, tuple.f4);
                row.setField(5, tuple.f5);
                row.setField(6, RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_ALL_TRANS_UE_FENG_Q(ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC,RUNNINGTIME,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransFormerUseEfficiencyFengQuarter");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR};
    }
}
