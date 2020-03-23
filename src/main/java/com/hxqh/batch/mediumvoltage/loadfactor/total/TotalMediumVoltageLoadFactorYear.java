package com.hxqh.batch.mediumvoltage.loadfactor.total;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.hxqh.domain.info.DataStartEnd;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.hxqh.constant.Constant.*;

/**
 * 总体中压开关柜负荷率-年度
 * <p>
 * Created by Ocean lin on 2020/3/19.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class TotalMediumVoltageLoadFactorYear {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, String, Double>> list = new ArrayList<>();
        Properties properties = new Properties();
        properties.put("url", JDBC_ES_URL);
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        dds.setInitialSize(1);
        Connection connection = dds.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
        String sql = "select IEDName,assetYpe,productModel,location,avg(APhaseCurrent) as avgA,avg(BPhaseCurrent) as avgB,avg(CPhaseCurrent) as avgC from yc_mediumvoltage3" +
                " where CreateTime>'" + start + "' and CreateTime<'" + end + "' group by IEDName,assetYpe,productModel,location";
        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple3<String, String, Double> tuple3 = new Tuple3<>();
            tuple3.f0 = resultSet.getString("IEDName");
            tuple3.f1 = resultSet.getString("assetYpe") + "|" + resultSet.getString("productModel")
                    + "|" + resultSet.getString("location");
            tuple3.f2 = (resultSet.getDouble("avgA") + resultSet.getDouble("avgB") + resultSet.getDouble("avgC")) / 3;
            list.add(tuple3);
        }
        ps.close();
        connection.close();
        dds.close();
        DataSource<Tuple3<String, String, Double>> source = env.fromCollection(list);

        // asset
        String selectQuery = "select ASSETNUM,LOADRATE,productModelB from ASSET where ASSETYPE='中压开关设备'";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery)
                        .setRowTypeInfo(new RowTypeInfo(new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO}))
                        .setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);
        DataSet<Row> assetRow = env.createInput(inputBuilder.finish());
        DataSet<Tuple3<String, Double, String>> assetTuple = assetRow.map(new MapFunction<Row, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(Row row) throws Exception {
                return Tuple3.of(row.getField(0).toString(), Double.parseDouble(row.getField(1).toString()), row.getField(2).toString());
            }
        });

        DataSet<Tuple2<String, Double>> joinDataSet = source.join(assetTuple).where(0).equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, Double>, Tuple3<String, Double, String>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> join(Tuple3<String, String, Double> first, Tuple3<String, Double, String> second) throws Exception {
                        // 获取分组项目，评分乘权重
                        Double v = 0d;
                        if (second.f2.endsWith("A")) {
                            String currentRated = second.f2.replaceAll("A", "");
                            v = first.f2 / Double.parseDouble(currentRated) * second.f1;
                        }
                        return Tuple2.of(first.f1, v);
                    }
                });

        DataSet<Tuple2<String, Double>> reduce = joinDataSet.groupBy(0).reduce(new ReduceFunction<Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) throws Exception {
                v1.f1 = v1.f1 + v2.f1;
                return v1;
            }
        });
        reduce.print();

        DataSet<Row> sink = reduce.map(new MapFunction<Tuple2<String, Double>, Row>() {
            @Override
            public Row map(Tuple2<String, Double> value) throws Exception {
                Row row = new Row(5);
                String[] split = value.f0.split("\\|");
                row.setField(0, split[0]);
                row.setField(1, split[1]);
                row.setField(2, split[2]);
                row.setField(3, value.f1);
                row.setField(4, RemindDateUtils.getLastYear());
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_TOTAL_LOAD_YEAR(ASSETYPE,PRODUCTMODEL,LOCATION,LOADFACTOR,CREATETIME) VALUES(?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR})
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        sink.output(outputBuilder.finish());

        env.execute("MediumVoltageLoadFactorQuarter");

    }
}
