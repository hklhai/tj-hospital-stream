package com.hxqh.batch.mediumvoltage.electricalmeasurement;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
import com.hxqh.utils.LevelUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 单台中压设备-年度有功电度量和无功电度量
 * <p>
 * Created by Ocean lin on 2020/4/8.
 *
 * @author Ocean lin
 */
@SuppressWarnings("DuplicatedCode")
public class MediumVoltageElectricalMeasurementYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple5<String, Double, Double, Double, Double>> list = new ArrayList<>();

        // asset
        String assetQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,productModelC from ASSET where ASSETYPE='中压开关设备'";
        JDBCInputFormat.JDBCInputFormatBuilder assetInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(assetQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO})).
                        setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        DataSource<Row> assetRow = env.createInput(assetInputBuilder.finish());
        DataSet<Tuple5<String, String, String, String, String>> assetDs = assetRow.map(new MapFunction<Row, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(Row row) throws Exception {
                return Tuple5.of(row.getField(0).toString(), row.getField(1).toString(),
                        row.getField(2).toString(), row.getField(3).toString(), row.getField(4).toString());
            }
        });

        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();

//        String sqlFirst = "select IEDName,max(ActiveElectricDegree) as ActiveElectricDegreeMax,max(ReactiveElectricDegree) as ReactiveElectricDegreeMax,min(ActiveElectricDegree) as ActiveElectricDegreeMin,min(ReactiveElectricDegree) as ReactiveElectricDegreeMin from yc_mediumvoltage3 where ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59' group by IEDName";
        String sqlFirst = "select IEDName,max(ActiveElectricDegree) as ActiveElectricDegreeMax,max(ReactiveElectricDegree) as ReactiveElectricDegreeMax,min(ActiveElectricDegree) as ActiveElectricDegreeMin,min(ReactiveElectricDegree) as ReactiveElectricDegreeMin from yc_mediumvoltage3 where ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";

        PreparedStatement ps = connection.prepareStatement(sqlFirst);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String iedName = resultSet.getString("IEDName");
            list.add(Tuple5.of(iedName, resultSet.getDouble("ActiveElectricDegreeMax"), resultSet.getDouble("ReactiveElectricDegreeMax"),
                    resultSet.getDouble("ActiveElectricDegreeMin"), resultSet.getDouble("ReactiveElectricDegreeMin")
            ));
        }
        DataSource<Tuple5<String, Double, Double, Double, Double>> dataSet = env.fromCollection(list);
        ElasticSearchUtils.close(connection, ps, resultSet);


        DataSet<Row> join = dataSet.join(assetDs).where(0).equalTo(0).with(new JoinFunction<Tuple5<String, Double, Double, Double, Double>, Tuple5<String, String, String, String, String>, Row>() {
            @Override
            public Row join(Tuple5<String, Double, Double, Double, Double> first, Tuple5<String, String, String, String, String> second) throws Exception {
                Row row = new Row(11);
                Double activeElectricDegree = first.f1 - first.f3;
                Double reactiveElectricDegree = first.f2 - first.f4;
                Double electricDegree = activeElectricDegree + reactiveElectricDegree;
                Double reactivePercent = reactiveElectricDegree / (electricDegree + 0.01);
                row.setField(0, second.f0);
                row.setField(1, second.f1);
                row.setField(2, second.f2);
                row.setField(3, second.f3);
                row.setField(4, second.f4);
                row.setField(5, activeElectricDegree);
                row.setField(6, reactiveElectricDegree);
                row.setField(7, RemindDateUtils.getLastYear());
                row.setField(8, electricDegree);
                row.setField(9, reactivePercent);
                row.setField(10, LevelUtils.computePercentageRreactive(reactivePercent));
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_VOLTAGE_EM_YEAR(IEDName,ASSETYPE,PRODUCTMODEL,LOCATION,productModelC,ActiveElectricDegree,ReactiveElectricDegree,CREATETIME,ElectricDegree,ReactivePercent,Opinion) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
        // ElectricalMeasurement
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        join.output(outputBuilder.finish());

        env.execute("MediumVoltageElectricalMeasurementYear");

    }

    private static String buildInString(List<String> assetnumList) {
        StringBuilder stringBuilder = new StringBuilder(1024);
        for (String s : assetnumList) {
            stringBuilder.append("'").append(s).append("'").append(",");
        }
        String str = stringBuilder.toString();
        String substring = str.substring(0, str.length() - 1);
        return substring;
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE,
                Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }
}
