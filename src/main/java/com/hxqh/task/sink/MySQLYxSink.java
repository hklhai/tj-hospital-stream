package com.hxqh.task.sink;

import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * Created by Ocean lin on 2020/3/6.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYxSink extends RichSinkFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;

    @Override
    public void invoke(Row row, Context context) throws Exception {
        connection = JdbcUtil.getConnection();

        String iedName = row.getField(0).toString();
        Timestamp colTime = new Timestamp(DateUtils.formatDate(row.getField(2).toString()).getTime());
        String variableName = ((Row[]) row.getField(7))[0].getField(0).toString();
        Integer val = Integer.parseInt(((Row[]) row.getField(7))[0].getField(1).toString());

        String countSql = "select count(*) from YX_CURRENT  where IEDNAME=? and VariableName=?";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, iedName);
        preparedStatement.setString(2, variableName);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select  YXID from YX_CURRENT where IEDNAME=? and VariableName=? ";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, iedName);
            preparedStatement.setString(2, variableName);
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update YX_CURRENT set COLTIME=?,VAL=?,CREATETIME=? where YXID=? ";
            preparedStatement = connection.prepareStatement(updateSql);

            preparedStatement.setTimestamp(1, colTime);
            preparedStatement.setInt(2, val);
            preparedStatement.setTimestamp(3, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setInt(4, pkId);

            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            // 新增设备
            String insertSql = "INSERT INTO YX_CURRENT (IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME) VALUES(?,?,?,?,?)";
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setString(1, iedName);
            preparedStatement.setTimestamp(2, colTime);
            preparedStatement.setString(3, variableName);
            preparedStatement.setInt(4, val);
            preparedStatement.setTimestamp(5, new Timestamp(DateUtils.getFormatDate().getTime()));

            preparedStatement.executeUpdate();
            JdbcUtil.close(preparedStatement, connection);
        }

    }


}
