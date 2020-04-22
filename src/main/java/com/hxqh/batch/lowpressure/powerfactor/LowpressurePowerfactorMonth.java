package com.hxqh.batch.lowpressure.powerfactor;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 单台低压开关设备功率因数-月度
 * <p>
 */
@SuppressWarnings("DuplicatedCode")
public class LowpressurePowerfactorMonth {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        //放置es数据的List
        List<Tuple2<String, Double>> levelList = new ArrayList<>();

        //获取当前环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //es取数据开始
        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastMonthStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
//        String start = "2020-04-01 00:00:00";
//        String end = "2020-04-30 23:59:59";

        //测试语句，注意这里es中是区分大小写的
        //Val='1' and and ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59'
//        String sqlFirst = "select assetYpe,productModel,location,alarmLevel,count(1) as levelCount from yx1 where assetYpe = '中压开关设备' and alarmLevel is not null group by assetYpe,productModel,location,alarmLevel";

        //注意，这里加入了alarmLevel is not null，需要做过滤，不然会出现空指针
//        String sqlFirst = "select IEDName,assetYpe,location,productModel,productModelC,alarmLevel,count(1) as levelCount from yx1 where Val='1' and assetYpe = '变压器' and ColTime>='2020-01-01 00:00:00' and ColTime<='2020-04-30 23:59:59'  group by IEDName,alarmLevel,assetYpe,location,productModel,productModelC";
        String sqlFirst = "select IEDName,assetYpe,location,productModel,productModelB,productModelC,avg(PowerFactor) as PowerFactor from yc_lowpressure where  ColTime>='" + start + "' and ColTime<='" + end + "'  group by IEDName,assetYpe,location,productModel,productModelB,productModelC";

        PreparedStatement ps1 = connection.prepareStatement(sqlFirst);
        ResultSet resultSet1 = ps1.executeQuery();
        while (resultSet1.next()) {
            String IEDName = resultSet1.getString("IEDName") + "|" +
                    resultSet1.getString("assetYpe") + "|" +
                    resultSet1.getString("location") + "|" +
                    resultSet1.getString("productModel") + "|" +
                    resultSet1.getString("productModelB") + "|" +
                    resultSet1.getString("productModelC");
            double count = resultSet1.getDouble("PowerFactor");
            levelList.add(Tuple2.of(IEDName, count));
        }

        DataSource<Tuple2<String, Double>> LevelDataSet = env.fromCollection(levelList);

        ElasticSearchUtils.close(connection, ps1, resultSet1);
        //es取数据结束


        DataSet<Row> result = LevelDataSet.map(new MapFunction<Tuple2<String, Double>, Row>() {
            @Override
            public Row map(Tuple2<String, Double> value) throws Exception {
                Row row = new Row(8);
                String[] splits = value.f0.split("\\|");
                row.setField(0, splits[0]);
                row.setField(1, splits[1]);
                row.setField(2, splits[2]);
                row.setField(3, splits[3]);
                row.setField(4, splits[4]);
                row.setField(5, splits[5]);
                row.setField(6, RemindDateUtils.getLastMonth());
                row.setField(7, value.f1);
                return row;
            }
        });


        String insertQuery = "INSERT INTO  RE_LP_POWERFACTOR_M(IEDNAME,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELB,PRODUCTMODELC,CREATETIME,POWERFACTORAVG) VALUES(?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowpressurePowerfactorMonth");
    }


    public static String getScorelevel(int score){
        if(score > 80){
            return "优";
        }else if(60 < score && score <= 80){
            return "良";
        }else{
            return "差";
        }
    }

    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.DOUBLE};
    }

}
