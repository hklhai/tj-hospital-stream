package com.hxqh.batch.lowpressure.powerfactor.total;

import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * 整体低压开关设备功率因数-年度
 * <p>
 *
 */
@SuppressWarnings("DuplicatedCode")
public class TotalLowpressurePowerfactorYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();
        //获取当前环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        String transQuery = "select ASSETYPE,LOCATION,PRODUCTMODELC,POWERFACTORAVG,CREATETIME from RE_LP_POWERFACTOR_M where ASSETYPE like '低压开关设备%' and CREATETIME like '" + RemindDateUtils.getLastYear() + "%'";
        JDBCInputFormat.JDBCInputFormatBuilder transInputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(transQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> transRow = env.createInput(transInputBuilder.finish());

        DataSet<Tuple5<String,String,Integer,String,String>> transData = transRow.map(new MapFunction<Row, Tuple5<String,String,Integer,String,String>>() {
            @Override
            public Tuple5<String,String,Integer,String,String> map(Row row) throws Exception {
                String key = row.getField(0).toString() + "|"+ row.getField(1).toString() + "|"+ row.getField(2).toString();
                Double powerfactoravg = Double.parseDouble(row.getField(3).toString());
                String time = row.getField(4).toString();
                int flag = 0;
                String min = time + ",";
                String max = "";
                if(powerfactoravg > 0.9){
                    flag = 1;
                    min = "";
                    max = time + ",";
                }
                return Tuple5.of(key,time,flag,min,max);
            }
        });

        DataSet<Tuple5<String, String,Integer, String, String>> request = transData.groupBy(0).reduce(new ReduceFunction<Tuple5<String,String,Integer,String,String>>() {
            @Override
            public Tuple5<String,String,Integer,String,String> reduce(Tuple5<String,String,Integer,String,String> v1, Tuple5<String,String,Integer,String,String> v2) throws Exception {
                if(v2.f2 == 0){
                    if(v2.f3 != ""){
                        v1.f3 += v2.f3;
                    }
                }else{
                    if(v2.f4 != ""){
                        v1.f4 += v2.f4;
                    }
                }
                return v1;
            }
        });


        DataSet<Row> result = request.map(new MapFunction<Tuple5<String, String,Integer, String, String>, Row>() {
            @Override
            public Row map(Tuple5<String, String,Integer, String, String> value) throws Exception {
                Row row = new Row(6);
                String[] splits = value.f0.split("\\|");
                row.setField(0, splits[0]);
                row.setField(1, splits[1]);
                row.setField(2, splits[2]);
                row.setField(3, value.f1);
                row.setField(4, value.f3.equals("") ? "" : value.f3.substring(0,value.f3.length()-1));
                row.setField(5, value.f4.equals("") ? "" : value.f4.substring(0,value.f4.length()-1));
                return row;
            }
        });


        String insertQuery = "INSERT INTO  RE_ALL_LP_POWERFACTOR_Y(ASSETYPE,LOCATION,PRODUCTMODELC,CREATETIME,MINMONTHSTR,MAXMONTHSTR) VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalLowpressurePowerfactorYear");
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
    }

}
