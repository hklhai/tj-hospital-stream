package com.hxqh.batch.lowpressure.useefficiency;


import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
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
 * 单台低压设备总运行时长--年度
 * <p>
 * Created by Ocean on 2020/4/22.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class LowPressureUseEfficiencyYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final TypeInformation<?>[] lowFieldTypes = getLowFieldTypes();
        RowTypeInfo lowTypeInfo = new RowTypeInfo(lowFieldTypes);
        String lowQuery = "select ASSETNUM,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC from ASSET " +
                "where ASSETYPE like '低压开关设备%' fetch first 1 rows only";
        JDBCInputFormat.JDBCInputFormatBuilder lowInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowQuery).setRowTypeInfo(lowTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> lowRow = env.createInput(lowInputBuilder.finish());

        DataSet<Tuple3<String, String,String>> lowData = lowRow.map(new MapFunction<Row, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String,String> map(Row row) throws Exception {
                String key = row.getField(2).toString() + "|"+ row.getField(3).toString()
                        +"|"+row.getField(4).toString()+"|" + row.getField(5).toString();
                return Tuple3.of(row.getField(0).toString(),  row.getField(1).toString(),key);
            }
        });

        final TypeInformation<?>[] lowMonthFieldTypes = getLowMonthFieldTypes();
        RowTypeInfo lowMonthTypeInfo = new RowTypeInfo(lowMonthFieldTypes);
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();


        String lowMonthQuery = "select IEDNAME,ASSETYPE,SUM(RUNNINGTIME) from RE_LP_UE_RUN_MONTH " +
                "where ASSETYPE like '低压开关设备%' and ColTime>='" + start + "' and ColTime<='" + end + "'  " +
                "group by IEDNAME,ASSETYPE";
        JDBCInputFormat.JDBCInputFormatBuilder lowMonthBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowMonthQuery).setRowTypeInfo(lowMonthTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> lowMonthRow = env.createInput(lowMonthBuilder.finish());
        DataSet<Tuple3<String, String, Double>> lowMonthData = lowMonthRow.map(new MapFunction<Row, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Row row) throws Exception {
                return Tuple3.of(row.getField(0).toString(), row.getField(1).toString(), Double.parseDouble(row.getField(2).toString()));
            }
        });

        DataSet<Row> result = lowData.join(lowMonthData).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, String,String>, Tuple3<String, String,Double>, Row>() {
            @Override
            public Row join(Tuple3<String, String,String> first, Tuple3<String, String,Double> second) throws Exception {
                Row row = new Row(9);

                row.setField(0, first.f0);
                row.setField(1, first.f1);
                String[] split = first.f2.split("\\|");
                row.setField(2, split[0]);
                row.setField(3, split[1]);
                row.setField(4, split[2]);
                row.setField(5, split[3]);

                row.setField(6, second.f2 / 60);//转换为小时
                row.setField(7, second.f2 / 60 / (24 * 365));//转换为比率
                row.setField(8, RemindDateUtils.getLastYear());
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_LP_UE_YEAR(IEDNAME,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC,RUNNINGTIME,UTILIZATIONRATIO,CREATETIME) " +
                "VALUES(?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowPressureUseEfficiencyYear");
    }


    private static TypeInformation<?>[] getLowFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
        };
    }

    private static TypeInformation<?>[] getLowMonthFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
