package com.hxqh.batch.lowpressure.powersupply;

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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
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
 * 单台低压供用电电压电流情况--年度
 * <p>
 * Created by Ocean on 2020/4/21.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class LowPressureCurentAndVoltageYear {
    public static void main(String[] args) throws Exception {

        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypes);
        String lowQuery = "select ASSETNUM,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB from ASSET where ASSETYPE like '低压开关设备%'";

        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowQuery).setRowTypeInfo(typeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> lowRow = env.createInput(inputBuilder.finish());



        DataSet<Tuple2<String, String>> lowData = lowRow.map(new MapFunction<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString()+ "|"+ row.getField(4).toString();
                return Tuple2.of(row.getField(0).toString(), key);
            }
        });


        //从es中取得低压设备的电流和电压
        List<Tuple7<String, Double, Double, Double, Double, Double, Double>> lowList = new ArrayList<>();

        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
        //测试数据
//        String start = "2020-04-01 00:00:00";
//        String end = "2020-05-31 23:59:59";

        //注意这里es中是区分大小写的
        String sqlEs = "select IEDName," +
                "max(PhaseL1CurrentPercent) as PhaseL1CurrentPercentMax,max(PhaseL2CurrentPercent) as PhaseL2CurrentPercentMax,max(PhaseL3CurrentPercent) as PhaseL3CurrentPercentMax," +
                "min(PhaseL1CurrentPercent) as PhaseL1CurrentPercentMin,min(PhaseL2CurrentPercent) as PhaseL2CurrentPercentMin,min(PhaseL3CurrentPercent) as PhaseL3CurrentPercentMin," +
                "avg(PhaseL1CurrentPercent) as PhaseL1CurrentPercentAvg,avg(PhaseL2CurrentPercent) as PhaseL2CurrentPercentAvg,avg(PhaseL3CurrentPercent) as PhaseL3CurrentPercentAvg," +
                "max(PhaseL1L2Voltage) as PhaseL1L2VoltageMax,max(PhaseL2L3Voltage) as PhaseL2L3VoltageMax,max(PhaseL3L1Voltage) as PhaseL3L1VoltageMax," +
                "min(PhaseL1L2Voltage) as PhaseL1L2VoltageMin,min(PhaseL2L3Voltage) as PhaseL2L3VoltageMin,min(PhaseL3L1Voltage) as PhaseL3L1VoltageMin," +
                "avg(PhaseL1L2Voltage) as PhaseL1L2VoltageAvg,avg(PhaseL2L3Voltage) as PhaseL2L3VoltageAvg,avg(PhaseL3L1Voltage) as PhaseL3L1VoltageAvg " +
                "from yc_lowpressure where ColTime>='" + start + "' and ColTime<='" + end + "' group by IEDName";

        System.out.println(sqlEs);
        PreparedStatement ps = connection.prepareStatement(sqlEs);

        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            String iedName = resultSet.getString("IEDName");

            //求得最大电流，最小电流和平均电流
            Double phaseCurrentPercentMax = Math.max(Math.max(resultSet.getDouble("PhaseL1CurrentPercentMax"), resultSet.getDouble("PhaseL2CurrentPercentMax")), resultSet.getDouble("PhaseL3CurrentPercentMax"));
            Double phaseCurrentPercentMin = Math.min(Math.min(resultSet.getDouble("PhaseL1CurrentPercentMin"), resultSet.getDouble("PhaseL2CurrentPercentMin")), resultSet.getDouble("PhaseL3CurrentPercentMin"));
            Double phaseCurrentPercentAvg = (resultSet.getDouble("PhaseL1CurrentPercentAvg") + resultSet.getDouble("PhaseL2CurrentPercentAvg") + resultSet.getDouble("PhaseL3CurrentPercentAvg")) / 3;

            //求得最大电压，最小电压和平均电压
            Double phaseVoltageMax = Math.max(Math.max(resultSet.getDouble("PhaseL1L2VoltageMax"), resultSet.getDouble("PhaseL2L3VoltageMax")), resultSet.getDouble("PhaseL3L1VoltageMax"));
            Double phaseVoltageMin = Math.min(Math.min(resultSet.getDouble("PhaseL1L2VoltageMin"), resultSet.getDouble("PhaseL2L3VoltageMin")), resultSet.getDouble("PhaseL3L1VoltageMin"));
            Double phaseVoltageAvg = (resultSet.getDouble("PhaseL1L2VoltageAvg") + resultSet.getDouble("PhaseL2L3VoltageAvg") + resultSet.getDouble("PhaseL3L1VoltageAvg")) / 3;

            lowList.add(Tuple7.of(iedName, phaseCurrentPercentMax/100, phaseCurrentPercentMin/100, phaseCurrentPercentAvg/100,phaseVoltageMax,phaseVoltageMin,phaseVoltageAvg));
        }

        DataSource<Tuple7<String, Double, Double, Double, Double, Double, Double>> dataSet = env.fromCollection(lowList);


        ElasticSearchUtils.close(connection, ps, resultSet);
        //es取数据结束


        DataSet<Row> result = lowData.join(dataSet).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, String>, Tuple7<String, Double, Double, Double, Double, Double, Double>, Row>() {
            @Override
            public Row join(Tuple2<String, String> first, Tuple7<String, Double, Double, Double, Double, Double, Double> second) throws Exception {
                Row row = new Row(15);
                row.setField(0, first.f0);
                String[] split = first.f1.split("\\|");
                row.setField(1, split[0]);
                row.setField(2, split[1]);
                row.setField(3, split[2]);
                row.setField(4, split[3]);
                //电流百分比
                row.setField(5, second.f1);
                row.setField(6, second.f2);
                row.setField(7, second.f3);

                //电流，需要*一个额定电流：productModelB
                String productB = split[3];
                if(productB.contains("A")){
                    productB = productB.substring(0,productB.lastIndexOf("A"));
                }
                Double current = Double.parseDouble(productB);
                row.setField(8, second.f1*current);
                row.setField(9, second.f2*current);
                row.setField(10, second.f3*current);

                //电压
                row.setField(11, second.f4);
                row.setField(12, second.f5);
                row.setField(13, second.f6);

                row.setField(14, RemindDateUtils.getLastYear());
                return row;
            }
        });

        result.print();

        String insertQuery = "INSERT INTO RE_LP_VA_Y (IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC,PRODUCTMODELB," +
                "PHASECURRENTPERCENTMAX,PHASECURRENTPERCENTMIN,PHASECURRENTPERCENTAVG," +
                "PHASECURRENTMAX,PHASECURRENTMIN,PHASECURRENTAVG," +
                "PHASEVOLTAGEMAX,PHASEVOLTAGEMIN,PHASEVOLTAGEAVG,CREATETIME) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowPressureCurentAndVoltageYear");
    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,
                Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR
        };
    }
}
