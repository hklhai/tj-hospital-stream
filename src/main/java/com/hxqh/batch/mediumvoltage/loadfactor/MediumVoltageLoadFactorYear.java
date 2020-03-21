package com.hxqh.batch.mediumvoltage.loadfactor;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.hxqh.domain.info.DataStartEnd;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
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
 * 单台中压开关柜负荷率-年度
 * <p>
 * Created by Ocean lin on 2020/3/18.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MediumVoltageLoadFactorYear {
    public static void main(String[] args) throws Exception {
        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Double>> list = new ArrayList<>();
        Properties properties = new Properties();
        properties.put("url", JDBC_ES_URL);
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        dds.setInitialSize(1);
        Connection connection = dds.getConnection();
        DataStartEnd startEnd = RemindDateUtils.getLastYearStartEndTime();
        String start = startEnd.getStart();
        String end = startEnd.getEnd();
        String sql = "select IEDName,productModelB,avg(APhaseCurrent) as avgA,avg(BPhaseCurrent) as avgB,avg(CPhaseCurrent) as avgC from yc_mediumvoltage3" +
                " where CreateTime>'" + start + "' and CreateTime<'" + end + "' group by IEDName,productModelB";
        System.out.println(sql);
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple2<String, Double> tuple2 = new Tuple2<>();
            tuple2.f0 = resultSet.getString("IEDName") + "|" + resultSet.getString("productModelB");
            tuple2.f1 = (resultSet.getDouble("avgA") + resultSet.getDouble("avgB") + resultSet.getDouble("avgC")) / 3;
            list.add(tuple2);
        }
        ps.close();
        connection.close();
        dds.close();
        DataSource<Tuple2<String, Double>> source = env.fromCollection(list);

        DataSet<Tuple3<String, Double, String>> result = source.map(new MapFunction<Tuple2<String, Double>, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(Tuple2<String, Double> value) throws Exception {
                Tuple3<String, Double, String> tuple3 = new Tuple3();
                String[] split = value.f0.split("\\|");
                tuple3.f0 = split[0];
                if (split[1].endsWith("A")) {
                    String currentRated = split[1].replaceAll("A", "");
                    tuple3.f1 = value.f1 / Double.parseDouble(currentRated);
                }
                tuple3.f2 = RemindDateUtils.getLastYear();
                return tuple3;
            }
        });


        // asset
        RowTypeInfo assetTypeInfo = new RowTypeInfo(fieldTypes);
        String selectQuery = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,LOCATION,FRACTIONRATIO from ASSET where ASSETYPE='中压开关设备'";
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(assetTypeInfo).setUsername(DB2_USERNAME)
                        .setPassword(DB2_PASSWORD);

        DataSet<Row> assetRow = env.createInput(inputBuilder.finish());

        DataSet<Tuple5<String, String, String, String, Double>> asset = assetRow.map(new MapFunction<Row, Tuple5<String, String, String, String, Double>>() {
            @Override
            public Tuple5<String, String, String, String, Double> map(Row row) throws Exception {
                return Tuple5.of(row.getField(0).toString(),
                        row.getField(1).toString(),
                        row.getField(2).toString(),
                        row.getField(3).toString(),
                        Double.parseDouble(row.getField(4).toString())
                );
            }
        });

        DataSet<Row> sink = result.join(asset).where(0).equalTo(0).with(new JoinFunction<Tuple3<String, Double, String>, Tuple5<String, String, String, String, Double>, Row>() {
            @Override
            public Row join(Tuple3<String, Double, String> first, Tuple5<String, String, String, String, Double> second) throws Exception {
                Row row = new Row(6);
                row.setField(0, first.f0);
                row.setField(1, first.f1);
                row.setField(2, first.f2);
                row.setField(3, second.f1);
                row.setField(4, second.f2);
                row.setField(5, second.f3);
                return row;
            }
        });

        String insertQuery = "INSERT INTO RE_LOAD_YEAR (IEDNAME,LOADFACTOR,CREATETIME,ASSETYPE,PRODUCTMODEL,LOCATION) VALUES(?,?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(new int[]{Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR})
                        .setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        sink.output(outputBuilder.finish());

        env.execute("MediumVoltageLoadFactorYear");

    }

    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO
        };
    }
}
