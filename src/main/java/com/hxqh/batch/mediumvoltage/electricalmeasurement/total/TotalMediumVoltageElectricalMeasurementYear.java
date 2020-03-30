package com.hxqh.batch.mediumvoltage.electricalmeasurement.total;

import com.hxqh.utils.LevelUtils;
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
 * 总体中压设备-年度有功电度量和无功电度量
 * <p>
 * Created by Ocean lin on 2020/3/30.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class TotalMediumVoltageElectricalMeasurementYear {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String quarterQuery = "select assetYpe,productModel,location,productModelC,avg(ActiveElectricDegree) as ActiveElectricDegree,avg(ReactiveElectricDegree) as ReactiveElectricDegree,avg(ELECTRICDEGREE) as ELECTRICDEGREE from RE_VOLTAGE_EM_YEAR" +
                " where CreateTime = '" + RemindDateUtils.getLastYear() + "' group by assetYpe,productModel,location,productModelC";
        System.out.println(quarterQuery);
        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());

        DataSet<Row> dataSource = quarterRow.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = new Row(10);
                Double reactivePercent = Double.parseDouble(value.getField(5).toString()) / Double.parseDouble(value.getField(6).toString());
                row.setField(0, value.getField(0).toString());
                row.setField(1, value.getField(1).toString());
                row.setField(2, value.getField(2).toString());
                row.setField(3, value.getField(3).toString());
                row.setField(4, Double.parseDouble(value.getField(4).toString()));
                row.setField(5, Double.parseDouble(value.getField(5).toString()));
                row.setField(6, Double.parseDouble(value.getField(6).toString()));
                row.setField(7, RemindDateUtils.getLastYear());
                row.setField(8, reactivePercent);
                row.setField(9, LevelUtils.computePercentageRreactive(reactivePercent));
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_ALL_VOLTAGE_EM_YEAR (assetYpe,productModel,location,productModelC,ActiveElectricDegree,ReactiveElectricDegree,ELECTRICDEGREE,CREATETIME,REACTIVEPERCENT,OPINION) VALUES(?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR})
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);

        dataSource.output(outputBuilder.finish());

        env.execute("TotalMediumVoltageElectricalMeasurementYear");

    }
}
