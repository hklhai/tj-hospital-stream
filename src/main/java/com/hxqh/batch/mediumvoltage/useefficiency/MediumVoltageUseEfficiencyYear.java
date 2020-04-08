package com.hxqh.batch.mediumvoltage.useefficiency;

import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 单台中压设备-年度使用效率
 * <p>
 * Created by Ocean lin on 2020/4/8.
 *
 * @author Ocean lin
 */
public class MediumVoltageUseEfficiencyYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        String selectQuery = "select IEDNAME,COLTIME,PHASECURRENT,RUNNINGTIME1,DOWNTIME1,RUNNINGTIME2,DOWNTIME2,RUNNINGTIME3,DOWNTIME3,RUNNINGTIME4,DOWNTIME4,RUNSTATUS,CREATETIME,PARTICULARYEAR from yc_medium_voltage_run " +
                "where PARTICULARYEAR='" + Integer.parseInt(RemindDateUtils.getLastYear()) + "'";

        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(MYSQL_DRIVER_NAME).setDBUrl(MYSQL_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(rowTypeInfo).setUsername(MYSQL_USERNAME)
                        .setPassword(MYSQL_PASSWORD);
        DataSource<Row> source = env.createInput(inputBuilder.finish());
        DataSet<Tuple3<String, Double, Double>> sourceDs = source.map(new MapFunction<Row, Tuple3<String, Double, Double>>() {
            @Override
            public Tuple3<String, Double, Double> map(Row row) throws Exception {
                Double runningTimeTotal = Double.parseDouble(row.getField(3).toString()) + Double.parseDouble(row.getField(5).toString()) + Double.parseDouble(row.getField(7).toString()) + Double.parseDouble(row.getField(9).toString());
                Double downTimeTotal = Double.parseDouble(row.getField(4).toString()) + Double.parseDouble(row.getField(6).toString()) + Double.parseDouble(row.getField(8).toString()) + Double.parseDouble(row.getField(10).toString());
                return Tuple3.of(row.getField(0).toString(), runningTimeTotal, downTimeTotal);
            }
        });

        String assetQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION from ASSET where ASSETYPE='中压开关设备'";
        JDBCInputFormat.JDBCInputFormatBuilder assetInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(assetQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO})).
                        setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        DataSource<Row> assetRow = env.createInput(assetInputBuilder.finish());
        DataSet<Tuple4<String, String, String, String>> assetDs = assetRow.map(new MapFunction<Row, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> map(Row row) throws Exception {
                return Tuple4.of(row.getField(0).toString(), row.getField(1).toString(),
                        row.getField(2).toString(), row.getField(3).toString());
            }
        });

        DataSet<Tuple6<String, String, String, String, Double, Double>> join = sourceDs.join(assetDs).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, Double, Double>, Tuple4<String, String, String, String>, Tuple6<String, String, String, String, Double, Double>>() {
            @Override
            public Tuple6<String, String, String, String, Double, Double> join(Tuple3<String, Double, Double> first, Tuple4<String, String, String, String> second) throws Exception {
                Double utilizationRatio = first.f2 / 60 / (24 * 365);
                return Tuple6.of(second.f0, second.f1, second.f2, second.f3, first.f2 / 60, utilizationRatio);
            }
        });

        MapOperator<Tuple6<String, String, String, String, Double, Double>, Row> result = join.map(new MapFunction<Tuple6<String, String, String, String, Double, Double>, Row>() {
            @Override
            public Row map(Tuple6<String, String, String, String, Double, Double> tuple) throws Exception {
                Row row = new Row(7);
                row.setField(0, tuple.f0);
                row.setField(1, tuple.f1);
                row.setField(2, tuple.f2);
                row.setField(3, tuple.f3);
                row.setField(4, tuple.f4);
                row.setField(5, tuple.f5);
                row.setField(6, RemindDateUtils.getLastYear());
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_VOLTAGE_UE_YEAR(IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,RUNNINGTIME,UTILIZATIONRATIO,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("MediumVoltageUseEfficiencyYear");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }

    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP,
                BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP, BasicTypeInfo.INT_TYPE_INFO
        };
    }
}
