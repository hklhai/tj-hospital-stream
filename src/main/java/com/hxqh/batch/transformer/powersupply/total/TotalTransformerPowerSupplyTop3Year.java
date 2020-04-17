package com.hxqh.batch.transformer.powersupply.total;

import com.hxqh.utils.RemindDateUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;
import java.util.ArrayList;

import static com.hxqh.constant.Constant.*;

/**
 * 整体变压器供用电损耗较大的top3设备--年度
 * <p>
 */
@SuppressWarnings("Duplicates")
public class TotalTransformerPowerSupplyTop3Year {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String prouctModelcQuery = "select distinct PRODUCTMODELC,ASSETYPE from RE_TRANS_PS_YEAR " +
                "where ASSETYPE='变压器' and CREATETIME = '" + RemindDateUtils.getLastYear() + "' ";
        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(prouctModelcQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> prouctModelcDataSet = env.createInput(inputBuilder.finish());

        DataSet<Tuple2<String, String>> prouctModelcRow = prouctModelcDataSet.map(new MapFunction<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Row row) throws Exception {
                return Tuple2.of(row.getField(0).toString(),row.getField(1).toString());
            }
        });


        ArrayList prouctModelcList =  (ArrayList) prouctModelcRow.collect();
        if(CollectionUtils.isNotEmpty(prouctModelcList)){
            for(int i=0;i<prouctModelcList.size();i++){
                Tuple2<String, String> modelInfo = (Tuple2<String, String>)prouctModelcList.get(i);
                String modelType = modelInfo.f0;
                //获取前三条的数据
                String selectQuery = "select IEDNAME,PRODUCTMODELC,ASSETYPE,ROW_NUMBER() OVER() as ROW_NO " +
                        "from RE_TRANS_PS_YEAR where ASSETYPE='变压器' and PRODUCTMODELC='"+modelType+"' " +
                        "and CREATETIME = '" + RemindDateUtils.getLastYear() + "' order by LOADVALUE desc fetch first 3 rows only";
                JDBCInputFormat.JDBCInputFormatBuilder selinputBuilder =
                        JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                                .setQuery(selectQuery).setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO}))
                                .setUsername(DB2_USERNAME)
                                .setPassword(DB2_PASSWORD);

                System.out.println(selectQuery);

                DataSet<Row> dataSet = env.createInput(selinputBuilder.finish());

                DataSet<Tuple8<String, String,String, String,String,String,String,String>> dataRow = dataSet.map(new MapFunction<Row, Tuple8<String, String,String, String,String,String,String,String>>() {
                    @Override
                    public Tuple8<String, String,String, String,String,String,String,String> map(Row row) throws Exception {
                       String key = row.getField(2).toString() ;
                        return Tuple8.of(row.getField(0).toString(), row.getField(1).toString(),key,row.getField(3).toString(),"","","","");
                    }
                });


                DataSet<Tuple8<String, String,String, String,String,String,String,String>> reduce = dataRow.groupBy(1).reduce(new ReduceFunction<Tuple8<String, String,String, String,String,String,String,String>>() {
                    @Override
                    public Tuple8<String, String,String, String,String,String,String,String> reduce(Tuple8<String, String,String, String,String,String,String,String> v1, Tuple8<String, String,String, String,String,String,String,String> v2) throws Exception {

                        if(v1.f4.equals("")){
                            if(v1.f3.equals("1")){
                                v1.f5 = v1.f0;
                            }
                            if(v1.f3.equals("2")){
                                v1.f6 = v1.f0;
                            }else if(v1.f3.equals("3")){
                                v1.f7 = v1.f0;
                            }
                            v1.f4 = "exist";
                        }


                        if(v2!=null){
                            if(v2.f3.equals("1")){
                                v1.f5 = v2.f0;
                            }else if(v2.f3.equals("2")){
                                v1.f6 = v2.f0;
                            }else if(v2.f3.equals("3")){
                                v1.f7 = v2.f0;
                            }
                        }


                        return v1;
                    }
                });


                DataSet<Row> result = reduce.map(new MapFunction<Tuple8<String, String,String, String,String,String,String,String>, Row>() {
                    @Override
                    public Row map(Tuple8<String, String,String, String,String,String,String,String> value) throws Exception {
                        Row row = new Row(6);
                        row.setField(0, value.f1);//productc
                        row.setField(1, value.f2.split("\\|")[0]);//类型

                        if("".equals(value.f4)){
                            row.setField(2, value.f0);//top1设备编码
                        }else{
                            row.setField(2, value.f5);//top1设备编码
                        }

                        row.setField(3, value.f6);//top2设备编码
                        row.setField(4, value.f7);//top3设备编码

                        row.setField(5, RemindDateUtils.getLastYear());//时间-年度

                        return row;
                    }
                });


                //建表
                String insertQuery = "INSERT INTO RE_ALL_TRANS_PS_TOP3_YEAR (PRODUCTMODELC,ASSETYPE,TOPIEDNAME1,TOPIEDNAME2,TOPIEDNAME3,CREATETIME) VALUES(?,?,?,?,?,?)";
                JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                        JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                                .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
                result.output(outputBuilder.finish());

                env.execute("TotalTransformerPowerSupplyTop3Year");


            }
        }





    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO};
    }


    private static int[] getType() {
        return new int[]{
                Types.VARCHAR,Types.VARCHAR,  Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR
        };
    }

}

