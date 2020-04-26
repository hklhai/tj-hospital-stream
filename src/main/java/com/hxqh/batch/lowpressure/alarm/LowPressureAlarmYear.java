package com.hxqh.batch.lowpressure.alarm;

import com.hxqh.domain.info.DataStartEnd;
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
import org.apache.flink.api.java.tuple.Tuple6;
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
 * 单台低压设备报警分析-年度
 * <p>
 * Created by Ocean on 2020/4/26.
 *
 * @author Ocean
 */
@SuppressWarnings("DuplicatedCode")
public class LowPressureAlarmYear {

    public static void main(String[] args) throws Exception {
        final int[] type = getType();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String lowQuery = "select ASSETNUM,ASSETYPE,LOCATION,PRODUCTMODELC from ASSET where ASSETYPE like '低压开关设备%'";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(lowQuery).setRowTypeInfo(new RowTypeInfo( new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
                })).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        DataSet<Row> lowRow = env.createInput(inputBuilder.finish());
        DataSet<Tuple2<String, String>> lowData = lowRow.map(new MapFunction<Row, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Row row) throws Exception {
                String key = row.getField(1).toString() + "|"+ row.getField(2).toString()
                        +"|"+row.getField(3).toString();
                return Tuple2.of(row.getField(0).toString(), key);
            }
        });

        //放置es数据的List
        List<Tuple6<String, String,Integer,Integer,Integer,String>> levelList = new ArrayList<>();

        List<Tuple6<String, String,Integer,Integer,Integer,String>> VariableList = new ArrayList<>();

        Connection connection = ElasticSearchUtils.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();

        String sqlFirst = "select IEDName,alarmLevel,count(*) as levelCount from yx where Val='1' and assetYpe like '低压开关设备%' " +
                "and ColTime>='" + start + "' and ColTime<='" + end + "'  group by IEDName,alarmLevel";
        PreparedStatement ps1 = connection.prepareStatement(sqlFirst);
        ResultSet resultSet1 = ps1.executeQuery();
        while (resultSet1.next()) {
            String IEDName = resultSet1.getString("IEDName");
            String level = resultSet1.getString("alarmLevel")==null?"":resultSet1.getString("alarmLevel");
            double count = resultSet1.getDouble("levelCount");
            Integer levelCount = (int)count ;
            levelList.add(Tuple6.of(IEDName, level, levelCount,0,0,""));
        }

        DataSource<Tuple6<String, String,Integer,Integer,Integer,String>> LevelDataSet = env.fromCollection(levelList);
        String sqlSecond = "select IEDName,VariableName,count(*) as VariableNameCount from yx " +
                "where Val='1' and assetYpe like '低压开关设备%' and VariableName is not null " +
                "and ColTime>='" + start + "' and ColTime<='" + end + "' " +
                "group by IEDName,VariableName order by VariableNameCount desc";
        PreparedStatement ps2 = connection.prepareStatement(sqlSecond);
        ResultSet resultSet2 = ps2.executeQuery();
        while (resultSet2.next()) {
            String IEDName = resultSet2.getString("IEDName");
            String VariableName = resultSet2.getString("VariableName")==null?"":resultSet2.getString("VariableName");
            double count = resultSet2.getDouble("VariableNameCount");
            Integer VariableNameCount = (int) count;
            VariableList.add(Tuple6.of(IEDName, VariableName, VariableNameCount,0,0,""));
        }
        DataSource<Tuple6<String, String,Integer,Integer,Integer,String>> VariableDataSet = env.fromCollection(VariableList);

        ElasticSearchUtils.close(connection, ps1, resultSet1);
        ElasticSearchUtils.close(connection, ps2, resultSet2);
        //es取数据结束

        //先将三条转为有1，2，3级别报警的单条数据
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

        //整合做join的将最多报警的名称整合进去
        DataSet<Tuple6<String, String,Integer,Integer,Integer,String>> join = reduceLevel.join(reduceVariable).where(0).equalTo(0)
                .with(new JoinFunction<Tuple6<String, String,Integer,Integer,Integer,String>, Tuple6<String, String,Integer,Integer,Integer,String>, Tuple6<String, String,Integer,Integer,Integer,String>>() {
                    @Override
                    public Tuple6<String, String,Integer,Integer,Integer,String> join(Tuple6<String, String,Integer,Integer,Integer,String> first, Tuple6<String, String,Integer,Integer,Integer,String> second) throws Exception {
                        String key = second.f1 + "|" + second.f2;
                        return  Tuple6.of(first.f0,first.f1,first.f2,first.f3,first.f4,key);
                    }
                });

        DataSet<Row> result = lowData.join(join).where(0).equalTo(0).with(new JoinFunction<Tuple2<String, String>, Tuple6<String, String,Integer,Integer,Integer,String>, Row>() {
            @Override
            public Row join(Tuple2<String, String> first, Tuple6<String, String,Integer,Integer,Integer,String> second) throws Exception {
                Row row = new Row(13);
                int totalCount = second.f2 + second.f3 + second.f4;
                row.setField(0, second.f0);
                String[] spilt = first.f1.split("\\|");
                row.setField(1, spilt[0]);
                row.setField(2, spilt[1]);
                row.setField(3, spilt[2]);

                row.setField(4, second.f2);
                row.setField(5, totalCount==0?0.0:((double)second.f2) / totalCount);
                row.setField(6, second.f3);
                row.setField(7, totalCount==0?0.0:((double)second.f3) / totalCount);
                row.setField(8, second.f4);
                row.setField(9, totalCount==0?0.0:((double)second.f4) / totalCount);

                String[] variable = second.f5.split("\\|");

                row.setField(10, variable[0]);
                row.setField(11, Integer.parseInt(variable[1]));

                row.setField(12, RemindDateUtils.getLastYear());
                return row;
            }
        });


        String insertQuery = "INSERT INTO  RE_LP_ALARM_YEAR(IEDNAME,ASSETYPE,LOCATION,PRODUCTMODELC," +
                "ALARMLEVEL1COUNT,ALARMLEVEL1RATIO,ALARMLEVEL2COUNT,ALARMLEVEL2RATIO,ALARMLEVEL3COUNT,ALARMLEVEL3RATIO," +
                "VARIABLENAMEMAX,VARIABLENAMECOUNT,CREATETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        result.output(outputBuilder.finish());

        env.execute("LowPressureAlarmYear");

    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,
                Types.INTEGER, Types.DOUBLE,Types.INTEGER, Types.DOUBLE,Types.INTEGER,
                Types.DOUBLE, Types.VARCHAR,Types.INTEGER, Types.VARCHAR,};
    }



}
