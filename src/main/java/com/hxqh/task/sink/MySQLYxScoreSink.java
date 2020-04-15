package com.hxqh.task.sink;

import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import static com.hxqh.constant.Constant.*;

/**
 * 处理设备实时得分
 * <p>
 * Created by Ocean lin on 2020/4/15.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYxScoreSink extends RichSinkFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;
    Integer highLevel = 0;
    Integer score = 0;
    Integer lowScore = 0;
    Integer actualScore = 0;
    Integer actualHighLevel = 0;


    @Override
    public void invoke(Row row, Context context) throws Exception {
        connection = JdbcUtil.getConnection();

        String iedName = row.getField(0).toString();
        Timestamp colTime = new Timestamp(DateUtils.formatDate(row.getField(2).toString()).getTime());
        String variableName = ((Row[]) row.getField(11))[0].getField(0).toString();
        Integer val = Integer.parseInt(((Row[]) row.getField(11))[0].getField(1).toString());
        Integer alarmLevel = ALARM_MAP.get(variableName);


        String countSql = "select count(*) from YX_SCORE where IEDNAME=? ";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, iedName);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select YXSCOREID,HIGHLEVEL,SCORE from YX_SCORE where IEDNAME=? ";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, iedName);
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
                highLevel = edNameResult.getInt(2);
                score = edNameResult.getInt(3);
            }

            // 以最高等级的最低分为准
            if (highLevel <= alarmLevel) {
                lowScore = ALARM_SCORE_LOW_MAP.get(highLevel);
                if (val == 1) {
                    score -= ALARM_SCORE_MAP.get(highLevel);
                } else {
                    score += ALARM_SCORE_MAP.get(highLevel);
                }
                actualHighLevel = highLevel;
            } else {
                lowScore = ALARM_SCORE_LOW_MAP.get(alarmLevel);
                if (val == 1) {
                    score -= ALARM_SCORE_MAP.get(alarmLevel);
                } else {
                    score += ALARM_SCORE_MAP.get(alarmLevel);
                }
                actualHighLevel = alarmLevel;
            }
            actualScore = lowScore > score ? lowScore : score;

            // 更新设备
            String updateSql = "update YX_SCORE set COLTIME=?,HIGHLEVEL=?,SCORE=?,CREATETIME=?,VARIABLENAME=?,VAL=? where YXSCOREID=?";
            preparedStatement = connection.prepareStatement(updateSql);

            preparedStatement.setTimestamp(1, colTime);
            preparedStatement.setInt(2, actualHighLevel);
            preparedStatement.setInt(3, actualScore);
            preparedStatement.setTimestamp(4, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setString(5, variableName);
            preparedStatement.setInt(6, val);
            preparedStatement.setInt(7, pkId);

            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            if (val == 1) {
                // 新增设备
                String insertSql = "INSERT INTO YX_SCORE (IEDNAME,COLTIME,SCORE,HIGHLEVEL,CREATETIME,VARIABLENAME,VAL) VALUES(?,?,?,?,?,?,?)";
                preparedStatement = connection.prepareStatement(insertSql);
                preparedStatement.setString(1, iedName);
                preparedStatement.setTimestamp(2, colTime);
                preparedStatement.setInt(3, SOCRE_ONEHUNDRED - ALARM_SCORE_MAP.get(alarmLevel));
                preparedStatement.setInt(4, alarmLevel);
                preparedStatement.setTimestamp(5, new Timestamp(DateUtils.getFormatDate().getTime()));
                preparedStatement.setString(6, variableName);
                preparedStatement.setInt(7, val);

                preparedStatement.executeUpdate();
                JdbcUtil.close(preparedStatement, connection);
            }
        }

    }
}
