package com.hxqh.batch.mediumvoltage;

import com.hxqh.batch.mediumvoltage.input.MediumVoltageInput;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;
import static com.hxqh.constant.RowTypeConstants.MEDIUM_VOLTAGE_YX_COLUMN;
import static com.hxqh.constant.RowTypeConstants.MEDIUM_VOLTAGE_YX_TYPE;

/**
 * 中压开关柜单台运行状况及评分-月度
 *
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MediumVoltageScore {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT));


        RowTypeInfo rowTypeInfo = new RowTypeInfo(MEDIUM_VOLTAGE_YX_TYPE, MEDIUM_VOLTAGE_YX_COLUMN);

        MediumVoltageInput build = MediumVoltageInput.builder(
                httpHosts, INDEX_YX)
                .setRowTypeInfo(rowTypeInfo)
                .build();
        DataSource<Row> input = env.createInput(build);

        tEnv.registerDataSet("score", input,
                "IEDName,assetYpe,parent,location,productModel,productModelB,productModelC,fractionRatio,loadRate,alarmLevel,VariableName,Val");
        Table mediumvoltage = tEnv.sqlQuery("select IEDName,alarmLevel, count(alarmLevel) as alarmLevel from score where Val=1 group by IEDName,alarmLevel");
        build.close();

        DataSet<Row> dataSet = tEnv.toDataSet(mediumvoltage, Row.class);
        DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> ds1 = dataSet.map(new MapFunction<Row, Tuple5<String, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> map(Row value) throws Exception {
                return new Tuple5(
                        // AH01,2,1  AH01,1,1
                        value.getField(0).toString(),
                        Integer.parseInt(value.getField(1).toString()),
                        Integer.parseInt(value.getField(2).toString()),
                        ALARM_SCORE_MAP.get(Integer.parseInt(value.getField(1).toString())),
                        ALARM_SCORE_LOW_MAP.get(Integer.parseInt(value.getField(1).toString()))
                );
            }
        });
        ds1.print();

        DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> ds2 = ds1.groupBy(0).reduce(new ReduceFunction<Tuple5<String, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<String, Integer, Integer, Integer, Integer> reduce(Tuple5<String, Integer, Integer, Integer, Integer> v1, Tuple5<String, Integer, Integer, Integer, Integer> v2) throws Exception {
                // AH01,2,1,10,70  AH01,1,1,40,0
                // 存在多个报警级别以最高报警级别的最低分为准
                if (v1.f1 < v2.f1) {
                    v1.f3 = v1.f3 + v2.f3;
                    return v1;
                } else {
                    v2.f3 = v2.f3 + v2.f3;
                    return v2;
                }
            }
        });
        ds2.print();


        DataSet<Tuple2<String, Integer>> result = ds2.map(new MapFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple5<String, Integer, Integer, Integer, Integer> value) throws Exception {
                // 存在多个报警级别以最高报警级别的最低分为准
                Integer score = (100 - value.f3) > value.f4 ? (100 - value.f3) : value.f4;
                return new Tuple2<>(value.f0, score);
            }
        });

        // (AH01,50)
        result.print();

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


        DataSet<Row> sinkData = result.join(asset).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, Integer>, Tuple5<String, String, String, String, Double>, Row>() {
            @Override
            public Row join(Tuple2<String, Integer> first, Tuple5<String, String, String, String, Double> second) throws Exception {
                Row row = new Row(7);
                row.setField(0, second.f0);
                row.setField(1, second.f1);
                row.setField(2, second.f2);
                row.setField(3, second.f3);
                row.setField(4, second.f4);
                row.setField(5, first.f1);
                row.setField(6, RemindDateUtils.getLastMonth());
                return row;
            }
        });


        String insertQuery = "INSERT INTO RE_SCORE_MONTH (IEDNAME,assetYpe,productModel,location,fractionRatio,score,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        sinkData.output(outputBuilder.finish());

        env.execute("MediumVoltageScore");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.DOUBLE, Types.INTEGER, Types.VARCHAR
        };
    }

}

