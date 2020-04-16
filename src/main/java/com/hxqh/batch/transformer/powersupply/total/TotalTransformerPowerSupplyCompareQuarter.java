package com.hxqh.batch.transformer.powersupply.total;

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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 整体变压器供用电状况--季度对比
 * <p>
 * Created by Ocean on 2020/4/14.
 *
 * @author Ocean
 */
@SuppressWarnings("Duplicates")
public class TotalTransformerPowerSupplyCompareQuarter {


    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        RowTypeInfo psTypeInfo = new RowTypeInfo(fieldTypes);
        String selectQuery = "select ASSETYPE,PRODUCTMODELC,LOADVALUE,LOADRATIO,LOSSVALUE,LOSSRATIO,CREATETIME " +
                "from RE_ALL_TRANS_PS_QUARTER where ASSETYPE='变压器' and CREATETIME in " + RemindDateUtils.getLastTwoQuarterString() + " ";
        System.out.println(selectQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(psTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> psRow = env.createInput(inputBuilder.finish());

        DataSet<Tuple8<String, Double,Double,Double,Double,String,String,Double>> ps = psRow.map(new MapFunction<Row, Tuple8<String, Double,Double,Double,Double,String,String,Double>>() {
            @Override
            public Tuple8<String, Double,Double,Double,Double,String,String,Double> map(Row row) throws Exception {
                String key = row.getField(0).toString() + "|" + row.getField(1).toString();
                return Tuple8.of(key, Double.parseDouble(row.getField(2).toString()),
                        Double.parseDouble(row.getField(3).toString()),Double.parseDouble(row.getField(4).toString()),
                        Double.parseDouble(row.getField(5).toString()),row.getField(6).toString(),"",0.0);
            }
        });

        DataSet<Tuple8<String, Double,Double,Double,Double,String,String,Double>> reduce = ps.groupBy(0).reduce(new ReduceFunction<Tuple8<String, Double,Double,Double,Double,String,String,Double>>() {
            @Override
            public Tuple8<String, Double,Double,Double,Double,String,String,Double> reduce(Tuple8<String, Double,Double,Double,Double,String,String,Double> v1, Tuple8<String, Double,Double,Double,Double,String,String,Double> v2) throws Exception {
                if (v1.f5.compareTo(v2.f5) == 1) {
                    Double div = (v1.f3 - v2.f3) / v2.f3;
                    v1.f7 = div;
                    if (div > -Proportion && div < Proportion) {
                        v1.f6 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v1.f6 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v1.f6 = ChangeEnum.Increased.getCode();
                    }
                    return v1;
                } else {
                    Double div = (v2.f3 - v1.f3) / v1.f3;
                    v2.f7 = div;
                    if (div > -Proportion && div < Proportion) {
                        v2.f6 = ChangeEnum.Roughly_flat.getCode();
                    } else if (div <= -Proportion) {
                        v2.f6 = ChangeEnum.Decreased.getCode();
                    } else if (div >= Proportion) {
                        v2.f6 = ChangeEnum.Increased.getCode();
                    }
                    return v2;
                }
            }
        });


        DataSet<Row> result = reduce.map(new MapFunction<Tuple8<String, Double,Double,Double,Double,String,String,Double>, Row>() {
            @Override
            public Row map(Tuple8<String, Double,Double,Double,Double,String,String,Double> value) throws Exception {
                Row row = new Row(9);
                String[] spilt = value.f0.split("\\|");
                row.setField(0, spilt[0]);
                row.setField(1, spilt[1]);

                row.setField(2, value.f1);
                row.setField(3, value.f2);
                row.setField(4, value.f3);
                row.setField(5, value.f4);

                row.setField(6, value.f6);//比对结果
                row.setField(7, Double.isNaN(value.f7)?0.0:value.f7);//比对率

                row.setField(8, value.f5);//时间-季度

                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_ALL_TRANS_PS_COM_QUARTER (ASSETYPE,PRODUCTMODELC,LOADVALUE,LOADRATIO,LOSSVALUE,LOSSRATIO,COMPARISON,RATIO,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransformerPowerSupplyCompareQuarter");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.DOUBLE_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
        };
    }


    private static int[] getType() {
        return new int[]{
                Types.VARCHAR,Types.VARCHAR,  Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR,Types.DOUBLE,Types.VARCHAR
        };
    }

}

