package com.hxqh.batch.lowpressure.useefficiency.total;


import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import java.sql.Types;
import static com.hxqh.constant.Constant.*;

/**
 * 整体低压设备总运行时长--季度
 * <p>
 * Created by Ocean on 2020/4/22.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class TotalLowPressureUseEfficiencyQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        String quarterQuery = "select ASSETYPE,LOCATION,PRODUCTMODELC,sum(RUNNINGTIME),avg(UTILIZATIONRATIO)" +
                " from RE_LP_UE_QUARTER where ASSETYPE like '低压开关设备%' and CREATETIME = '" + RemindDateUtils.getLastQuarter() + "' group by ASSETYPE,LOCATION,PRODUCTMODELC ";
        JDBCInputFormat.JDBCInputFormatBuilder lowInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(lowInputBuilder.finish());

        DataSet<Tuple3<String, Double,Double>> quarterData = quarterRow.map(new MapFunction<Row, Tuple3<String, Double,Double>>() {
            @Override
            public Tuple3<String, Double,Double> map(Row row) throws Exception {
                String key = row.getField(0).toString() + "|"+ row.getField(1).toString()
                        +"|"+row.getField(2).toString();
                return Tuple3.of(key, Double.parseDouble(row.getField(3).toString()),Double.parseDouble(row.getField(4).toString()));
            }
        });

        DataSet<Row> result = quarterData.map(new MapFunction<Tuple3<String, Double,Double>, Row>() {
            @Override
            public Row map(Tuple3<String, Double,Double> value) throws Exception {
                Row row = new Row(6);
                String[] spilt = value.f0.split("\\|");
                row.setField(0, spilt[0]);
                row.setField(1, spilt[1]);
                row.setField(2, spilt[2]);

                row.setField(3, value.f1);
                row.setField(4, value.f2);
                row.setField(5, RemindDateUtils.getLastQuarter());

                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_ALL_LP_UE_QUARTER(ASSETYPE,LOCATION,PRODUCTMODELC,RUNNINGTIME,UTILIZATIONRATIO,CREATETIME) " +
                "VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalLowPressureUseEfficiencyQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }




    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                 Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
