package com.hxqh.batch.transformer.useefficiency.total;

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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体变压器使用效率电流及最大最小月份-季度统计信息
 * <p>
 */
@SuppressWarnings("DuplicatedCode")
public class TotalTransformerUseEfficiencyStatisticsQuarter {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final int[] type = getType();
        // 变压器获取上一季度中负荷最高和最低月份
        final TypeInformation<?>[] fieldTypes = getFieldTypes();

        RowTypeInfo transTypeInfo = new RowTypeInfo(fieldTypes);

        String transMonthQuery = "SELECT PRODUCTMODELC,LOADVALUE,CREATETIME FROM RE_TRANS_UE_MONTH where ASSETYPE='变压器' and CREATETIME in " + RemindDateUtils.getLastQuarterString() + " ";

        System.out.println(transMonthQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transMonthQuery).setRowTypeInfo(transTypeInfo)
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        DataSource<Row> maxSource = env.createInput(inputBuilder.finish());
        DataSet<Tuple5<String, Double, String, String, String>> transMonthDs = maxSource.map(new MapFunction<Row, Tuple5<String, Double, String, String, String>>() {
            @Override
            public Tuple5<String, Double, String, String, String> map(Row row) throws Exception {
                return Tuple5.of(row.getField(0).toString(), Double.parseDouble(row.getField(1).toString()), row.getField(2).toString(), "", "");
            }
        });
        //transMonthDs.print();

        DataSet<Tuple5<String, Double, String, String, String>> reduce = transMonthDs.groupBy(0).reduce(new ReduceFunction<Tuple5<String, Double, String, String, String>>() {
            @Override
            public Tuple5<String, Double, String, String, String> reduce(Tuple5<String, Double, String, String, String> v1, Tuple5<String, Double, String, String, String> v2) throws Exception {
                //大
                if (v1.f1 > v1.f1) {
                    v1.f4 = v1.f2;
                    v1.f3 = v2.f2;
                    return v1;
                } else {
                    //小
                    v2.f3 = v2.f2;
                    v2.f4 = v1.f2;
                    return v2;

                }
            }
        });
//reduce.print();


        String transQuery = "SELECT PRODUCTMODELC,ASSETYPE,max(PHASECURRENTMAX),min(PHASECURRENTMIN),avg(PHASECURRENTAVG) " +
                "FROM RE_TRANS_UE_INFO_QUARTER where ASSETYPE='变压器' and CREATETIME = '" + RemindDateUtils.getLastQuarter() + "' " +
                "group by PRODUCTMODELC,ASSETYPE";

        System.out.println(transQuery);
        JDBCInputFormat.JDBCInputFormatBuilder transInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
                })).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        DataSource<Row> transData = env.createInput(transInputBuilder.finish());

        DataSet<Tuple5<String, String, Double, Double, Double>> transDs = transData.map(new MapFunction<Row, Tuple5<String, String, Double, Double, Double>>() {
            @Override
            public Tuple5<String, String, Double, Double, Double> map(Row row) throws Exception {
                return Tuple5.of(row.getField(0).toString(), row.getField(1).toString(), Double.parseDouble(row.getField(2).toString()), Double.parseDouble(row.getField(3).toString()), Double.parseDouble(row.getField(4).toString()));
            }
        });

        //transDs.print();
        DataSet<Row> result = reduce.join(transDs).where(0).equalTo(0).with(new JoinFunction<Tuple5<String, Double, String, String, String>, Tuple5<String, String, Double, Double, Double>, Row>() {
            @Override
            public Row join(Tuple5<String, Double, String, String, String> first, Tuple5<String, String, Double, Double, Double> second) throws Exception {
                Row row = new Row(8);
                row.setField(0, first.f0);

                row.setField(1, second.f0);
                row.setField(2, first.f3);//最小月份
                row.setField(3, first.f4);//最大月份

                row.setField(4, second.f2);//最大电流
                row.setField(5, second.f3);//最小电流
                row.setField(6, second.f4);//平均电流

                row.setField(7, RemindDateUtils.getLastQuarter());
                return row;
            }
        });


        result.print();

        String insertQuery = "INSERT INTO RE_ALL_TRANS_UE_INFO_QUARTER(PRODUCTMODELC,ASSETYPE,MINMONTH,MAXMONTH," +
                "PHASECURRENTMAX,PHASECURRENTMIN,PHASECURRENTAVG,CREATETIME) VALUES(?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransformerUseEfficiencyStatisticsQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
