package com.hxqh.batch.transformer.powersupply.total;

import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体变压器供用电状况--年度（按照productmodelC分组）
 * <p>
 * Created by Ocean on 2020/4/14.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class TotalTransformerPowerSupplyYear {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        RowTypeInfo psTypeInfo = new RowTypeInfo(fieldTypes);
        String selectQuery = "select ASSETYPE,PRODUCTMODELC,avg(LOADVALUE),avg(LOADRATIO),avg(LOSSVALUE),avg(LOSSRATIO) " +
                "from RE_TRANS_PS_YEAR where ASSETYPE='变压器' and CREATETIME = '" + RemindDateUtils.getLastYear() + "' group by ASSETYPE,PRODUCTMODELC";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(psTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> psRow = env.createInput(inputBuilder.finish());

        DataSet<Tuple5<String, Double,Double,Double,Double>> ps = psRow.map(new MapFunction<Row, Tuple5<String, Double,Double,Double,Double>>() {
            @Override
            public Tuple5<String, Double,Double,Double,Double> map(Row row) throws Exception {
                String key = row.getField(0).toString() + "|"+ row.getField(1).toString();
                return Tuple5.of(key, Double.parseDouble(row.getField(2).toString()),
                        Double.parseDouble(row.getField(3).toString()),Double.parseDouble(row.getField(4).toString()),
                        Double.parseDouble(row.getField(5).toString()));
            }
        });

        DataSet<Row> result = ps.map(new MapFunction<Tuple5<String, Double,Double,Double,Double>, Row>() {
            @Override
            public Row map(Tuple5<String, Double,Double,Double,Double> value) throws Exception {
                Row row = new Row(7);
                String[] spilt = value.f0.split("\\|");
                row.setField(0, spilt[0]);
                row.setField(1, spilt[1]);
                row.setField(2, value.f1);
                row.setField(3, value.f2);
                row.setField(4, value.f3);
                row.setField(5, value.f4);
                row.setField(6, RemindDateUtils.getLastYear());

                return row;
            }
        });


        result.print();


        String insertQuery = "INSERT INTO RE_ALL_TRANS_PS_YEAR (ASSETYPE,PRODUCTMODELC,LOADVALUE,LOADRATIO,LOSSVALUE,LOSSRATIO,CREATETIME) VALUES(?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransformerPowerSupplyYear");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{
                Types.VARCHAR, Types.VARCHAR,  Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR
        };
    }

}

