package com.hxqh.batch.transformer.health.total;

import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体变压器运行健康状况得分-季度
 * <p>
 * Created by Ocean on 2020/4/14.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class TotalTransFormerHealthQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String quarterQuery = "select PRODUCTMODELC,ASSETYPE,PRODUCTMODEL,LOCATION,avg(SCORE) as SCORE from RE_TRANS_HEALTH_QUARTER " +
                "where ASSETYPE='变压器' and CREATETIME ='" + RemindDateUtils.getLastQuarter() + "' group by PRODUCTMODELC,ASSETYPE,PRODUCTMODEL,LOCATION";

        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());


        DataSet<Row> sinkRow = quarterRow.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = new Row(7);
                row.setField(0, value.getField(0).toString());
                row.setField(1, value.getField(1).toString());
                row.setField(2, value.getField(2).toString());
                row.setField(3, value.getField(3).toString());
                row.setField(4, getScorelevel(Double.parseDouble(value.getField(4).toString())));
                row.setField(5, value.getField(4));
                row.setField(6, RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        String insertQuery = "INSERT INTO  RE_ALL_TRANS_HEA_Q(PRODUCTMODELC,ASSETYPE,PRODUCTMODEL,LOCATION,SCORELEVEL,SCORE,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        sinkRow.output(outputBuilder.finish());

        env.execute("TotalTransFormerHealthQuarter");
    }


    public static String getScorelevel(double score) {
        if (score > 80) {
            return "优";
        } else if (60 < score && score <= 80) {
            return "良";
        } else {
            return "差";
        }
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR};
    }

}
