package com.hxqh.batch.lowpressure.powersupply.total;

import com.hxqh.constant.Constant;
import com.hxqh.enums.VoltageChangeEnum;
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
 * 整体低压设备供应电最电压统计-年度比对，与额定电压400V比较
 * <p>
 * Created by Ocean on 2020/4/21.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class TotalLowPressureVoltageCompareYear {
    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        String voltageQuery = "select ASSETYPE,LOCATION,PRODUCTMODELC,PHASEVOLTAGEAVG " +
                "from RE_ALL_LP_VOLTAGE_Y where ASSETYPE like '低压开关设备%' and CREATETIME = '" + RemindDateUtils.getLastYear() + "' ";
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
                Double phaseVoltageVvg = Double.parseDouble(value.getField(3).toString());
                row.setField(3, phaseVoltageVvg);
                Double radio = (phaseVoltageVvg-400)/400;
                String comparison = "";
                if(phaseVoltageVvg> Constant.OverVoltageRatio_UP*400){
                    //超过10%
                    comparison = VoltageChangeEnum.Increased.getCode();
                } else if (phaseVoltageVvg <= Constant.OverVoltageRatio_DOWN*400) {
                    //低于85%
                    comparison = VoltageChangeEnum.Decreased.getCode();
                } else {
                    comparison = VoltageChangeEnum.Roughly_flat.getCode();
                }
                row.setField(4, comparison);
                row.setField(5, radio);
                row.setField(6, RemindDateUtils.getLastYear());
                return row;
            }
        });


        result.print();

        String insertQuery = "INSERT INTO RE_ALL_LP_VOLTAGE_COM_Y (ASSETYPE,LOCATION,PRODUCTMODELC,PHASEVOLTAGEAVG,COMPARISON,RATIO,CREATETIME) " +
                "VALUES(?,?,?,?,?,?,?)";

        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                    .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalLowPressureVoltageCompareYear");
}


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR
        };
    }
}
