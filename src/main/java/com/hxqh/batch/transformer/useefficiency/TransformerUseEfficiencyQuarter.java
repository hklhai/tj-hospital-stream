package com.hxqh.batch.transformer.useefficiency;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
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
 * 单台中压设备-季度统计信息
 * <p>
 */
@SuppressWarnings("DuplicatedCode")
public class TransformerUseEfficiencyQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        // 变压器
        final TypeInformation<?>[] fieldTypes = getFieldTypes();

        RowTypeInfo transTypeInfo = new RowTypeInfo(fieldTypes);
        String transQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT from ASSET where ASSETYPE='变压器'";
        System.out.println(transQuery);
        JDBCInputFormat.JDBCInputFormatBuilder transInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transQuery).setRowTypeInfo(transTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> transRow = env.createInput(transInputBuilder.finish());

        DataSet<Tuple3<String, String,String>> transData = transRow.map(new MapFunction<Row, Tuple3<String, String,String>>() {
            @Override
            public Tuple3<String, String,String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString();
                return Tuple3.of(row.getField(0).toString(), key, row.getField(6).toString());
            }
        });



        //中压设备
        String mysqlQuery = "select IEDNAME,sum(RUNNINGTIME) AS TOTALRUNNINGTIME,sum(DOWNTIME) AS TOTALDOWNTIME FROM yc_medium_voltage_run_month where PARTICULARTIME in " + RemindDateUtils.getLastQuarterString() + " group by IEDNAME";
        System.out.println(mysqlQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(MYSQL_DRIVER_NAME).setDBUrl(MYSQL_DB_URL)
                        .setQuery(mysqlQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO}))
                        .setUsername(MYSQL_USERNAME).setPassword(MYSQL_PASSWORD);

        DataSource<Row> source = env.createInput(inputBuilder.finish());
        //取得变压器上游中压设备数据
        DataSet<Tuple3<String, Double, Double>> mediumData = source.map(new MapFunction<Row, Tuple3<String, Double, Double>>() {
            @Override
            public Tuple3<String, Double, Double> map(Row row) throws Exception {
                return Tuple3.of(row.getField(0).toString(), Double.parseDouble(row.getField(1).toString()), Double.parseDouble(row.getField(2).toString()));
            }
        });


        //变压器和中压结合
        DataSet<Row> result = transData.join(mediumData).where(2).equalTo(0).with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, Double, Double>, Row>() {
            @Override
            public Row join(Tuple3<String, String, String> first, Tuple3<String, Double, Double> second) throws Exception {
                Row row = new Row(11);
                row.setField(0, first.f0);//变压器设备
                String[] spilt = first.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);
                row.setField(4, spilt[3]);
                row.setField(5, spilt[4]);
                row.setField(6, first.f2);//所属父级

                row.setField(7, second.f1 / 60);//总时长
                row.setField(8, second.f1/ 60 / (24 * 90));//使用效率
                row.setField(9, second.f2 / 60);//总停机

                row.setField(10, RemindDateUtils.getLastQuarter());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_TRANS_UE_QUARTER(IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT,RUNNINGTIME,UTILIZATIONRATIO,DOWNTIME,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransformerUseEfficiencyQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
        };
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR};
    }


}
