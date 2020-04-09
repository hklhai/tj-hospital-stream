package com.hxqh.batch.mediumvoltage.useefficiency.total;

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
 * 整体开关柜使用效率运行时长-年度
 * <p>
 * Created by Ocean lin on 2020/4/9.
 *
 * @author Ocean
 */
public class TotalMediumVoltageUseEfficiencyYear {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String quarterQuery = "select ASSETYPE,PRODUCTMODEL,LOCATION,sum(RUNNINGTIME) AS TOTALRUNNINGTIME,sum(UTILIZATIONRATIO)/count(*) AS AVGUTILIZATIONRATIO  " +
                "from RE_VOLTAGE_UE_YEAR where ASSETYPE='中压开关设备' and CREATETIME like '" + RemindDateUtils.getLastYear() + "%' group by ASSETYPE,PRODUCTMODEL,LOCATION";
        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());

        DataSet<Row> dataSource = quarterRow.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = new Row(6);
                row.setField(0, value.getField(0).toString());
                row.setField(1, value.getField(1).toString());
                row.setField(2, value.getField(2).toString());
                row.setField(3, Double.parseDouble(value.getField(3).toString()));
                row.setField(4, Double.parseDouble(value.getField(4).toString()));
                row.setField(5, RemindDateUtils.getLastYear());
                return row;
            }
        });


        String insertQuery = "INSERT INTO RE_ALL_VOLTAGE_UE_YEAR(ASSETYPE,PRODUCTMODEL,LOCATION,TOTALRUNNINGTIME,AVGUTILIZATIONRATIO,CREATETIME) VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR})
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);

        dataSource.output(outputBuilder.finish());

        env.execute("TotalMediumVoltageUseEfficiencyYear");

    }
}
