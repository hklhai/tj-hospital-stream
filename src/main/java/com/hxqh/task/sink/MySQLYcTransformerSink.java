package com.hxqh.task.sink;

import com.hxqh.domain.YcTransformer;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYcTransformerSink extends RichSinkFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;


    @Override
    public void invoke(Row row, Context context) throws Exception {
        connection = JdbcUtil.getConnection();

        YcTransformer ycTransformer = ConvertUtils.convert2YcTransformer(row);

        String countSql = "select count(*) from yc_transformer_current where IEDNAME=? ";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycTransformer.getIEDName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select  YCRANSFORMERID from yc_transformer_current where IEDNAME=?";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycTransformer.getIEDName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update yc_transformer_current set COLTIME=?,CREATETIME=?,APhaseTemperature=?,BPhaseTemperature=?,CPhaseTemperature=?,DRoadTemperature=? where YCRANSFORMERID =?";

            preparedStatement = connection.prepareStatement(updateSql);

            preparedStatement.setTimestamp(1, new Timestamp(ycTransformer.getColTime().getTime()));
            preparedStatement.setTimestamp(2, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setDouble(3, ycTransformer.getAPhaseTemperature());
            preparedStatement.setDouble(4, ycTransformer.getBPhaseTemperature());
            preparedStatement.setDouble(5, ycTransformer.getCPhaseTemperature());
            preparedStatement.setDouble(6, ycTransformer.getDRoadTemperature());

            preparedStatement.setInt(7, pkId);
            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            // 新增设备
            String insertSql = "INSERT INTO yc_transformer_current (IEDNAME,COLTIME,CREATETIME,APhaseTemperature,BPhaseTemperature,CPhaseTemperature,DRoadTemperature) VALUES(?,?,?,?,?,?,?)";
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setString(1, ycTransformer.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(ycTransformer.getColTime().getTime()));
            preparedStatement.setTimestamp(3, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setDouble(4, ycTransformer.getAPhaseTemperature());
            preparedStatement.setDouble(5, ycTransformer.getBPhaseTemperature());
            preparedStatement.setDouble(6, ycTransformer.getCPhaseTemperature());
            preparedStatement.setDouble(7, ycTransformer.getDRoadTemperature());

            preparedStatement.executeUpdate();
            JdbcUtil.close(resultSet, preparedStatement, connection);
        }
    }


}
