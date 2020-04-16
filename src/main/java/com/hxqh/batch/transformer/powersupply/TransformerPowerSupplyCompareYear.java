package com.hxqh.batch.transformer.powersupply;

import com.hxqh.enums.ChangeEnum;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 单台变压器供用电状况--年度对比
 * <p>
 * Created by Ocean on 2020/4/14.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class TransformerPowerSupplyCompareYear {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // asset
        RowTypeInfo assetTypeInfo = new RowTypeInfo(fieldTypes);
        String selectQuery = "select IEDNAME,ASSETYPE,PRODUCTMODEL,LOCATION,PRODUCTMODELB,PRODUCTMODELC," +
                "PARENT,LOADVALUE,LOADRATIO,LOSSRATIO,LOSSVALUE,CREATETIME from " +
                "RE_TRANS_PS_YEAR where ASSETYPE='变压器' and CREATETIME in " + RemindDateUtils.getLastTwoYearString();
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(assetTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> psRow = env.createInput(inputBuilder.finish());



        DataSet<Tuple6<String, String,Double,String,String,Double>> ps = psRow.map(new MapFunction<Row, Tuple6<String, String,Double,String,String,Double>>() {
            @Override
            public Tuple6<String, String,Double,String,String,Double> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+"|" + row.getField(4).toString()
                        +"|"+row.getField(5).toString()+"|"+row.getField(6).toString()
                        +"|"+row.getField(7).toString()+"|"+row.getField(8).toString()
                        +"|"+row.getField(9).toString();
                return Tuple6.of(row.getField(0).toString(), key, Double.parseDouble(row.getField(10).toString()),row.getField(11).toString(),"",0.0);
            }
        });


        DataSet<Tuple6<String, String,Double,String,String,Double>> reduce = ps.groupBy(0).reduce(new ReduceFunction<Tuple6<String, String,Double,String,String,Double>>() {
            @Override
            public Tuple6<String, String,Double,String,String,Double> reduce(Tuple6<String, String,Double,String,String,Double> v1, Tuple6<String, String,Double,String,String,Double> v2) throws Exception {
                if (v1.f3.compareTo(v2.f3) == 1) {
                    Double div = (v1.f2 - v2.f2) / v2.f2;
                    v1.f5 = div;
                    if (div > -Proportion && div < Proportion) {
                        v1.f4 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v1.f4 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v1.f4 = ChangeEnum.Increased.getCode();
                    }
                    return v1;
                } else {
                    Double div = (v2.f2 - v1.f2) / v1.f2;
                    v2.f5 = div;
                    if (div > -Proportion && div < Proportion) {
                        v2.f4 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v2.f4 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v2.f4 = ChangeEnum.Increased.getCode();
                    }
                    return v2;
                }
            }
        });



        DataSet<Row> result = reduce.map(new MapFunction<Tuple6<String, String,Double,String,String,Double>, Row>() {
            @Override
            public Row map(Tuple6<String, String,Double,String,String,Double> value) throws Exception {
                Row row = new Row(14);
                row.setField(0, value.f0);//设备编码
                String[] spilt = value.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);
                row.setField(4, spilt[3]);
                row.setField(5, spilt[4]);
                row.setField(6, spilt[5]);

                row.setField(7, Double.parseDouble(spilt[6]));
                row.setField(8, Double.parseDouble(spilt[7]));
                row.setField(9, Double.parseDouble(spilt[8]));
                row.setField(10, value.f2);//

                row.setField(11, value.f4);//比对结果
                row.setField(12, Double.isNaN(value.f5)?0.0:value.f5);//比对率
                row.setField(13, value.f3);//时间-季度

                return row;
            }
        });


        String insertQuery = "INSERT INTO RE_TRANS_PS_COM_YEAR (IEDNAME,ASSETYPE,PRODUCTMODEL," +
                "LOCATION,PRODUCTMODELB,PRODUCTMODELC,PARENT,LOADVALUE,LOADRATIO,LOSSRATIO,LOSSVALUE," +
                "COMPARISON,RATIO,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransformerPowerSupplyCompareYear");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR,Types.DOUBLE, Types.VARCHAR
        };
    }

}

