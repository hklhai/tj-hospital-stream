package com.hxqh.batch.mediumvoltage.total;

import com.hxqh.enums.HealthLevel;
import com.hxqh.utils.LevelUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体中压开关健康度统计-年度
 * <p>
 * Created by Ocean lin on 2020/3/17.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class TotalMediumVoltageHealthLevelYear {


    public static void main(String[] args) throws Exception {

        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String quarterQuery = "select ASSETYPE,PRODUCTMODEL,LOCATION,SCORE from RE_SCORE_YEAR " +
                "where ASSETYPE='中压开关设备' and CREATETIME = '" + RemindDateUtils.getLastYear() + "'";

        JDBCInputFormat.JDBCInputFormatBuilder quarterBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(quarterQuery).setRowTypeInfo(new RowTypeInfo(
                        new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BIG_DEC_TYPE_INFO
                        })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> quarterRow = env.createInput(quarterBuilder.finish());

        DataSet<Tuple2<String, Integer>> levelTuple = quarterRow.map(new MapFunction<Row, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Row row) throws Exception {
                // ASSETYPE,PRODUCTMODEL,LOCATION,SCORE
                Double score = Double.parseDouble(row.getField(3).toString());
                String key = row.getField(0).toString() + "|" + row.getField(1).toString() + "|" +
                        row.getField(2).toString() + "|" + LevelUtils.computeLevel(score);
                return Tuple2.of(key, 1);
            }
        });

        ReduceOperator<Tuple2<String, Integer>> reduceDs = levelTuple.groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1 = value1.f1 + value2.f1;
                return value1;
            }
        });

        DataSet<Tuple4<String, Integer, Integer, Integer>> transferDs = reduceDs.map(new MapFunction<Tuple2<String, Integer>, Tuple4<String, Integer, Integer, Integer>>() {
            @Override
            public Tuple4<String, Integer, Integer, Integer> map(Tuple2<String, Integer> value) throws Exception {

                String[] split = value.f0.split("\\|");
                String key = split[0] + "|" + split[1] + "|" + split[2];
                Tuple4 tuple4 = new Tuple4(key, 0, 0, 0);
                if (split[3].equals(HealthLevel.excellent.getCode())) {
                    tuple4.f1 = value.f1;
                }
                if (split[3].equals(HealthLevel.good.getCode())) {
                    tuple4.f2 = value.f1;
                }
                if (split[3].equals(HealthLevel.range.getCode())) {
                    tuple4.f3 = value.f1;
                }
                return tuple4;
            }
        });

        DataSet<Tuple4<String, Integer, Integer, Integer>> reduce = transferDs.reduce(new ReduceFunction<Tuple4<String, Integer, Integer, Integer>>() {
            @Override
            public Tuple4<String, Integer, Integer, Integer> reduce(Tuple4<String, Integer, Integer, Integer> v1, Tuple4<String, Integer, Integer, Integer> v2) throws Exception {
                v1.f1 = v1.f1 + v2.f1;
                v1.f2 = v1.f2 + v2.f2;
                v1.f3 = v1.f3 + v2.f3;
                return v1;
            }
        });

        DataSet<Row> sink = reduce.map(new MapFunction<Tuple4<String, Integer, Integer, Integer>, Row>() {
            @Override
            public Row map(Tuple4<String, Integer, Integer, Integer> value) throws Exception {
                String[] split = value.f0.split("\\|");
                Row row = new Row(7);
                row.setField(0, split[0]);
                row.setField(1, split[1]);
                row.setField(2, split[2]);
                row.setField(3, value.f1);
                row.setField(4, value.f2);
                row.setField(5, value.f3);
                row.setField(6, RemindDateUtils.getLastYear());
                return row;
            }
        });
        sink.print();

        String insertQuery = "INSERT INTO RE_TOTAL_HEALTH_YEAR(ASSETYPE,PRODUCTMODEL,LOCATION,excellent,good,range,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        sink.output(outputBuilder.finish());

        env.execute("TotalMediumVoltageHealthLevelYear");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR};
    }

}

