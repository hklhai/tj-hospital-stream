package com.hxqh.batch.mediumvoltage;

import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/3/13.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MediumVoltageScoreYear {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Quarter score
        String quarterQuery = "select IEDNAME,avg(score) as score from RE_SCORE_QUARTER " +
                "where ASSETYPE='中压开关设备' and CREATETIME like '" + RemindDateUtils.getLastYear() + "%' group by  IEDNAME";
        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO
                })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());
        DataSet<Tuple2<String, Double>> quarter = quarterRow.map(new MapFunction<Row, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Row row) throws Exception {
                return Tuple2.of(row.getField(0).toString(), Double.parseDouble(row.getField(1).toString()));
            }
        });

        // asset
        RowTypeInfo assetTypeInfo = new RowTypeInfo(fieldTypes);
        String selectQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,FRACTIONRATIO from ASSET where ASSETYPE='中压开关设备'";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(assetTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> assetRow = env.createInput(inputBuilder.finish());

        DataSet<Tuple5<String, String, String, String, Double>> asset = assetRow.map(new MapFunction<Row, Tuple5<String, String, String, String, Double>>() {
            @Override
            public Tuple5<String, String, String, String, Double> map(Row row) throws Exception {
                return Tuple5.of(row.getField(0).toString(),
                        row.getField(1).toString(),
                        row.getField(2).toString(),
                        row.getField(3).toString(),
                        Double.parseDouble(row.getField(4).toString())
                );
            }
        });

        DataSet<Row> result = quarter.join(asset).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, Double>, Tuple5<String, String, String, String, Double>, Row>() {
            @Override
            public Row join(Tuple2<String, Double> first, Tuple5<String, String, String, String, Double> second) throws Exception {
                Row row = new Row(4);
                row.setField(0, second.f0);
                row.setField(1, second.f1);
                row.setField(2, first.f1);
                row.setField(3, RemindDateUtils.getLastYear());
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_SCORE_YEAR (IEDNAME,ASSETYPE,SCORE,CREATETIME) VALUES(?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("MediumVoltageScoreQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR
        };
    }

}

