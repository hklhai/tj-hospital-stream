package com.hxqh.batch.transformer.health;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 单台变压器运行健康状况得分-季度
 * <p>
 * Created by Ocean on 2020/4/13.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class TransFormerHealthQuarter {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        //放置es数据的List
        List<Tuple5<String, Integer,Integer,Integer,Integer>> levelList = new ArrayList<>();

        //获取当前环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //es取数据开始
        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastQuarterStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();

        //测试语句，注意这里es中是区分大小写的
        //Val='1' and and ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59'
//        String sqlFirst = "select assetYpe,productModel,location,alarmLevel,count(1) as levelCount from yx1 where assetYpe = '中压开关设备' and alarmLevel is not null group by assetYpe,productModel,location,alarmLevel";

        //注意，这里加入了alarmLevel is not null，需要做过滤，不然会出现空指针
//        String sqlFirst = "select IEDName,assetYpe,location,productModel,productModelC,alarmLevel,count(1) as levelCount from yx1 where Val='1' and assetYpe = '变压器' and ColTime>='2020-01-01 00:00:00' and ColTime<='2020-04-30 23:59:59'  group by IEDName,alarmLevel,assetYpe,location,productModel,productModelC";
        String sqlFirst = "select IEDName,assetYpe,location,productModel,productModelC,alarmLevel,count(1) as levelCount from yx1 where Val='1' and assetYpe = '变压器' and ColTime>='" + start + "' and ColTime<='" + end + "'  group by IEDName,alarmLevel,assetYpe,location,productModel,productModelC";

        PreparedStatement ps1 = connection.prepareStatement(sqlFirst);
        ResultSet resultSet1 = ps1.executeQuery();
        while (resultSet1.next()) {
            String IEDName = resultSet1.getString("IEDName") + "|" +
                    resultSet1.getString("assetYpe") + "|" +
                    resultSet1.getString("location") + "|" +
                    resultSet1.getString("productModel") + "|" +
                    resultSet1.getString("productModelC");
            String level = resultSet1.getString("alarmLevel");
            double count = resultSet1.getDouble("levelCount");
            int level1 =Integer.parseInt(level);
            levelList.add(Tuple5.of(IEDName, Integer.parseInt(level), (int) count,ALARM_SCORE_MAP.get(level1),ALARM_SCORE_LOW_MAP.get(level1)));
        }

        DataSource<Tuple5<String, Integer,Integer,Integer,Integer>> LevelDataSet = env.fromCollection(levelList);

        ElasticSearchUtils.close(connection, ps1, resultSet1);
        //es取数据结束

        DataSet<Tuple5<String, Integer,Integer,Integer,Integer>> reduceVariable = LevelDataSet.groupBy(0).reduce(new ReduceFunction<Tuple5<String, Integer,Integer,Integer,Integer>>() {
            @Override
            public Tuple5<String, Integer,Integer,Integer,Integer> reduce(Tuple5<String, Integer,Integer,Integer,Integer> v1, Tuple5<String, Integer,Integer,Integer,Integer> v2) throws Exception {
                if (v1.f1 < v2.f1) {
                    v1.f3 = v1.f3 + v2.f3;
                    return v1;
                } else {
                    v2.f3 = v1.f3 + v2.f3;
                    return v2;
                }
            }
        });

        DataSet<Tuple2<String, Integer>> results = reduceVariable.map(new MapFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple5<String, Integer, Integer, Integer, Integer> value) throws Exception {
                Integer score = (100 - value.f3) > value.f4 ? (100 - value.f3) : value.f4;
                return new Tuple2<>(value.f0, score);
            }
        });

        DataSet<Row> result = results.map(new MapFunction<Tuple2<String, Integer>, Row>() {
            @Override
            public Row map(Tuple2<String, Integer> value) throws Exception {
                Row row = new Row(8);
                String[] splits = value.f0.split("\\|");
                row.setField(0, splits[0]);
                row.setField(1, splits[1]);
                row.setField(2, splits[2]);
                row.setField(3, splits[3]);
                row.setField(4, splits[4]);
                row.setField(5, getScorelevel(value.f1));
                row.setField(6, value.f1);
                row.setField(7, RemindDateUtils.getLastQuarter());
                return row;
            }
        });


        String insertQuery = "INSERT INTO  RE_TRANS_HEALTH_QUARTER(IEDNAME,ASSETYPE,LOCATION,PRODUCTMODEL,PRODUCTMODELC,SCORELEVEL,SCORE,CREATETIME) VALUES(?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TransFormerHealthQuarter");
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
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.INTEGER, Types.VARCHAR};
    }

}
