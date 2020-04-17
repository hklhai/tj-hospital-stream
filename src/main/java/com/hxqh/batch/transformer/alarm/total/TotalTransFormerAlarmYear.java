package com.hxqh.batch.transformer.alarm.total;

import com.hxqh.domain.info.DataStartEnd;
import com.hxqh.utils.ElasticSearchUtils;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;

/**
 * 整体变压器报警分析-年度
 * <p>
 *
 */
@SuppressWarnings("DuplicatedCode")
public class TotalTransFormerAlarmYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        //放置es数据的List
        List<Tuple6<String, String,Integer,Integer,Integer,String>> levelList = new ArrayList<>();

        List<Tuple6<String, String,Integer,Integer,Integer,String>> VariableList = new ArrayList<>();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //es取数据开始
        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
//        String start = "2019-03-01 00:00:00";
//        String end = "2020-05-30 23:59:59";



        //测试语句，注意这里es中是区分大小写的
        //Val='1' and and ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59'
        //String sqlFirst = "select assetYpe,productModel,location,alarmLevel,count(1) as levelCount from yx1 where assetYpe = '中压开关设备' and alarmLevel is not null group by assetYpe,productModel,location,alarmLevel";

        String sqlFirst = "select assetYpe,productModel,location,alarmLevel,count(*) as levelCount from yx1 " +
                "where assetYpe = '变压器' and Val='1' and alarmLevel is not null " +
                "and ColTime>='" + start + "' and ColTime<='" + end + "' " +
                "group by assetYpe,productModel,location,alarmLevel";

        PreparedStatement ps1 = connection.prepareStatement(sqlFirst);
        ResultSet resultSet1 = ps1.executeQuery();
        while (resultSet1.next()) {
            String assetType = resultSet1.getString("assetYpe");
            String productModel = resultSet1.getString("location");
            String location = resultSet1.getString("productModel");
           // String productModelc = resultSet1.getString("productModelC");
            String key = assetType +  "|" + location + "|" + productModel ;


            String level = resultSet1.getString("alarmLevel")==null?"":resultSet1.getString("alarmLevel");
            double count = resultSet1.getDouble("levelCount");
            Integer levelCount = (int)count ;

            levelList.add(Tuple6.of(key, level, levelCount,0,0,""));
        }

        DataSource<Tuple6<String, String,Integer,Integer,Integer,String>> LevelDataSet = env.fromCollection(levelList);


        //String sqlSecond = "select assetYpe,productModel,location,VariableName,count(1) as VariableNameMax from yx1 where Val='1' and assetYpe = '中压开关设备' and ColTime>='2020-03-01 00:00:00' and ColTime<='2020-03-31 23:59:59' group by assetYpe,productModel,location,VariableName order by VariableNameMax desc";

        String sqlSecond = "select assetYpe,productModel,location,VariableName,count(*) as VariableNameMax from yx1 " +
                "where Val='1' and assetYpe = '变压器' and VariableName is not null " +
                "and ColTime>='" + start + "' and ColTime<='" + end + "' " +
                "group by assetYpe,productModel,location,VariableName order by VariableNameMax desc";


        PreparedStatement ps2 = connection.prepareStatement(sqlSecond);
        ResultSet resultSet2 = ps2.executeQuery();
        while (resultSet2.next()) {
            String assetType = resultSet2.getString("assetYpe");
            String productModel = resultSet2.getString("location");
            String location = resultSet2.getString("productModel");
            //String productModelc = resultSet2.getString("productModelC");
            String key = assetType +  "|" + location + "|" + productModel ;


            String VariableName = resultSet2.getString("VariableName")==null?"":resultSet2.getString("VariableName");
            double count = resultSet2.getDouble("VariableNameMax");
            Integer VariableNameCount = (int) count;

            VariableList.add(Tuple6.of(key, VariableName, VariableNameCount,0,0,""));
        }
        DataSource<Tuple6<String, String,Integer,Integer,Integer,String>> VariableDataSet = env.fromCollection(VariableList);


        ElasticSearchUtils.close(connection, ps1, resultSet1);
        ElasticSearchUtils.close(connection, ps2, resultSet2);
        //es取数据结束


        DataSet<Tuple6<String, String,Integer,Integer,Integer,String>> reduceVariable = VariableDataSet.groupBy(0).reduce(new ReduceFunction<Tuple6<String, String,Integer,Integer,Integer,String>>() {
            @Override
            public Tuple6<String, String,Integer,Integer,Integer,String> reduce(Tuple6<String, String,Integer,Integer,Integer,String> v1, Tuple6<String, String,Integer,Integer,Integer,String> v2) throws Exception {
               if(v2.f2>v1.f2){
                   v1.f1 = v2.f1;
                   v1.f2 = v2.f2;
                   return v1;
               }else{
                   v2.f1 = v1.f1;
                   v2.f2 = v1.f2;
                   return v2;
               }
            }
        });

        DataSet<Tuple6<String, String,Integer,Integer,Integer,String>> reduceLevel = LevelDataSet.groupBy(0).reduce(new ReduceFunction<Tuple6<String, String,Integer,Integer,Integer,String>>() {
            @Override
            public Tuple6<String, String,Integer,Integer,Integer,String> reduce(Tuple6<String, String,Integer,Integer,Integer,String> v1, Tuple6<String, String,Integer,Integer,Integer,String> v2) throws Exception {
                if(!v1.f1.equals("")){
                    if(v1.f1.equals("2")){
                        v1.f3 = v1.f2;
                    }else if(v1.f1.equals("3")){
                        v1.f4 = v1.f2;
                    }
                    v1.f1="";
                }

                if(v2.f1.equals("1")){
                    v1.f2 = v2.f2;
                }else if(v2.f1.equals("2")){
                    v1.f3 = v2.f2;
                }else if(v2.f1.equals("3")){
                    v1.f4 = v2.f2;
                }

                return v1;
            }
        });

        DataSet<Tuple6<String, String,Integer,Integer,Integer,String>> join = reduceLevel.join(reduceVariable).where(0).equalTo(0)
        .with(new JoinFunction<Tuple6<String, String,Integer,Integer,Integer,String>, Tuple6<String, String,Integer,Integer,Integer,String>, Tuple6<String, String,Integer,Integer,Integer,String>>() {
            @Override
            public Tuple6<String, String,Integer,Integer,Integer,String> join(Tuple6<String, String,Integer,Integer,Integer,String> first, Tuple6<String, String,Integer,Integer,Integer,String> second) throws Exception {
                return  Tuple6.of(first.f0,first.f1,first.f2,first.f3,first.f4,second.f1);
            }
        });



        DataSet<Row> result = join.map(new MapFunction<Tuple6<String, String,Integer,Integer,Integer,String>, Row>() {
            @Override
            public Row map(Tuple6<String, String,Integer,Integer,Integer,String> value) throws Exception {
                Row row = new Row(11);
                String[] spilt = value.f0.split("\\|");
                int totalCount = value.f2 + value.f3 + value.f4;
                row.setField(0, spilt[0]);
                row.setField(1, spilt[1]);
                row.setField(2, spilt[2]);
                row.setField(3, value.f2);
                row.setField(4, totalCount==0?0.0:((double)value.f2) / totalCount);
                row.setField(5, value.f3);
                row.setField(6, totalCount==0?0.0:((double)value.f3) / totalCount);
                row.setField(7, value.f4);
                row.setField(8, totalCount==0?0.0:((double)value.f4) / totalCount);
                row.setField(9, value.f5);
                row.setField(10, RemindDateUtils.getLastYear());

                return row;
            }
        });


        String insertQuery = "INSERT INTO  RE_ALL_TRANS_ALARM_Y(ASSETYPE,PRODUCTMODEL,LOCATION,ALARMLEVEL1COUNT,ALARMLEVEL1RATIO,ALARMLEVEL2COUNT,ALARMLEVEL2RATIO,ALARMLEVEL3COUNT,ALARMLEVEL3RATIO,VARIABLENAMEMAX,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("TotalTransFormerAlarmYear");

    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DOUBLE,Types.INTEGER, Types.DOUBLE,Types.INTEGER, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR,};
    }


}
