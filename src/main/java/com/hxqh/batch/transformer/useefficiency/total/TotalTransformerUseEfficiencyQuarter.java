package com.hxqh.batch.transformer.useefficiency.total;

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
 * 整体电压器设备总使用时长及停机时长--季度统计信息
 * <p>
 */
@SuppressWarnings("DuplicatedCode")
public class TotalTransformerUseEfficiencyQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // 变压器
        final TypeInformation<?>[] fieldTypes = getFieldTypes();

        RowTypeInfo transTypeInfo = new RowTypeInfo(fieldTypes);
        String transQuery = "select ASSETYPE,PRODUCTMODELC,sum(RUNNINGTIME),sum(UTILIZATIONRATIO)/count(*),sum(DOWNTIME) " +
                "from RE_TRANS_UE_QUARTER where ASSETYPE='变压器' and CREATETIME = '" + RemindDateUtils.getLastQuarter() + "' " +
                "group by ASSETYPE,PRODUCTMODELC";
        System.out.println(transQuery);
        JDBCInputFormat.JDBCInputFormatBuilder transInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transQuery).setRowTypeInfo(transTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> transRow = env.createInput(transInputBuilder.finish());


        DataSet<Row> result = transRow.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = new Row(6);
                row.setField(0, value.getField(0).toString());
                row.setField(1, value.getField(1).toString());
                row.setField(2, Double.parseDouble(value.getField(2).toString()));
                row.setField(3, Double.parseDouble(value.getField(3).toString()));
                row.setField(4, Double.parseDouble(value.getField(4).toString()));
                row.setField(5, RemindDateUtils.getLastQuarter());
                return row;
            }
        });
        result.print();

        String insertQuery = "INSERT INTO RE_ALL_TRANS_UE_QUARTER(ASSETYPE,PRODUCTMODELC,RUNNINGTIME,UTILIZATIONRATIO,DOWNTIME,CREATETIME) VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransformerUseEfficiencyQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
