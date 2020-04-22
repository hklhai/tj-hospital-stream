package com.hxqh.batch.lowpressure.powersupply;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.enums.ChangeEnum;
import com.hxqh.enums.CurrentChangeEnum;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.ElasticSearchUtils;
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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import scala.tools.cmd.gen.AnyVals;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 单台低压设备电流统计-季度比对
 * <p>
 * Created by Ocean on 2020/4/21.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class LowPressureCurentCompareQuarter {
    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //取得低压设备季度中的平均电流
        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        //and CREATETIME in " + RemindDateUtils.getLastTwoQuarterString()
        String lowQuery = "select IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB,PHASECURRENTAVG,CREATETIME from RE_LP_VA_Q where ASSETYPE like '低压开关设备%'  and CREATETIME = '" + RemindDateUtils.getLastQuarter() +"' ";

        System.out.println(lowQuery);
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> lowRow = env.createInput(inputBuilder.finish());
        lowRow.print();

        DataSet<Row> result = lowRow.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = new Row(9);
                row.setField(0, value.getField(0).toString());//设备编码
                row.setField(1, value.getField(1).toString());//设备类别
                row.setField(2, value.getField(2).toString());//设备位置
                row.setField(3, value.getField(3).toString());//上游变压器设备
                row.setField(4, value.getField(4).toString());//额定电流

                String productB = row.getField(4).toString();
                if(productB.contains("A")){
                    productB = productB.substring(0,productB.lastIndexOf("A"));
                }
                Double ratedCurrent = Double.parseDouble(productB);
                Double phaseCurrentAvg = Double.parseDouble(value.getField(5).toString());
                row.setField(5, phaseCurrentAvg);//平均电流

                //（平均-额定）/额定
                Double compareRadio = (phaseCurrentAvg - ratedCurrent) / ratedCurrent;//比对率
                String compareResult = "";
                if (compareRadio > -Proportion && compareRadio < Proportion) {
                    compareResult = CurrentChangeEnum.Roughly_flat.getCode();
                } else if (compareRadio <= -Proportion) {
                    compareResult = CurrentChangeEnum.Decreased.getCode();
                } else if (compareRadio >= Proportion) {
                    compareResult = CurrentChangeEnum.Increased.getCode();
                }


                row.setField(6, compareResult);//比对结果
                row.setField(7, compareRadio);//比对率

                row.setField(8, value.getField(6).toString());//时间-季度

                return row;
            }
        });


        result.print();

        String insertQuery = "INSERT INTO RE_LP_CURRENT_COM_Q (IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC," +
                "PRODUCTMODELB,PHASECURRENTAVG,COMPARISON,RATIO,CREATETIME) " +
                "VALUES(?,?,?,?,?,?,?,?,?)";

        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                    .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowPressureCurentCompareQuarter");
}


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BIG_DEC_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR
        };
    }
}
