package com.hxqh.batch.lowpressure.powersupply.total;

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
 * 整体低压设备供应电最大，最小，平均电压统计-季度
 * <p>
 * Created by Ocean on 2020/4/21.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class TotalLowPressureVoltageQuarter {
    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        String voltageQuery = "select ASSETYPE,LOCATION,PRODUCTMODELC,max(PHASEVOLTAGEMAX),min(PHASEVOLTAGEMIN),avg(PHASEVOLTAGEAVG) " +
                "from RE_LP_VA_Q where ASSETYPE like '低压开关设备%' and CREATETIME = '" + RemindDateUtils.getLastQuarter() + "' " +
                "group by LOCATION,PRODUCTMODELC,ASSETYPE";
        System.out.println(voltageQuery);

        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(voltageQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> voltageQueryRow = env.createInput(inputBuilder.finish());

        DataSet<Row> result = voltageQueryRow.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = new Row(7);
                row.setField(0, value.getField(0).toString());
                row.setField(1, value.getField(1).toString());
                row.setField(2, value.getField(2).toString());
                row.setField(3, Double.parseDouble(value.getField(3).toString()));
                row.setField(4, Double.parseDouble(value.getField(4).toString()));
                row.setField(5, Double.parseDouble(value.getField(5).toString()));
                row.setField(6, RemindDateUtils.getLastQuarter());
                return row;
            }
        });


        result.print();

        String insertQuery = "INSERT INTO RE_ALL_LP_VOLTAGE_Q (ASSETYPE,LOCATION,PRODUCTMODELC,PHASEVOLTAGEMAX,PHASEVOLTAGEMIN,PHASEVOLTAGEAVG,CREATETIME) " +
                "VALUES(?,?,?,?,?,?,?)";

        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                    .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalLowPressureVoltageQuarter");
}


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR
        };
    }
}
