package com.hxqh.task.sink;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JdbcUtil;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import static com.hxqh.constant.Constant.MEDIUMVOLTAGE_RUN;
import static com.hxqh.constant.Constant.MEDIUMVOLTAGE_STOP;

/**
 * Created by Ocean lin on 2020/4/8.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYcMediumVoltageRunStatusMonthSink extends RichSinkFunction<String> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;
    Timestamp colTime = new Timestamp(DateUtils.getFormatDate().getTime());
    Double runningTime = 0.0d;
    Double downtime = 0.0d;
    Integer runstatus = MEDIUMVOLTAGE_STOP;

    @Override
    public void invoke(String value, Context context) throws Exception {
        connection = JdbcUtil.getConnection();
        IEDEntity entity = JSON.parseObject(value, IEDEntity.class);
        YcMediumVoltage ycMediumVoltage = ConvertUtils.convert2YcMediumVoltage(entity);
        Double nowPhaseCurrent = (ycMediumVoltage.getAPhaseCurrent() + ycMediumVoltage.getBPhaseCurrent() + ycMediumVoltage.getCPhaseCurrent()) / 3;

        String countSql = "select count(*) from YC_MEDIUM_VOLTAGE_RUN_MONTH where IEDNAME=? and PARTICULARTIME=?";
        preparedStatement = this.connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycMediumVoltage.getIEDName());
        preparedStatement.setString(2, RemindDateUtils.getNowMonth());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select YCMEDIUMVOLTAGERUNMONTHID,COLTIME,RUNNINGTIME,DOWNTIME,RUNSTATUS from YC_MEDIUM_VOLTAGE_RUN_MONTH where IEDNAME=? and PARTICULARTIME=? ";
            preparedStatement = this.connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycMediumVoltage.getIEDName());
            preparedStatement.setString(2, RemindDateUtils.getNowMonth());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
                colTime = edNameResult.getTimestamp(2);
                runningTime = edNameResult.getDouble(3);
                downtime = edNameResult.getDouble(4);
                runstatus = edNameResult.getInt(5);
            }

            /**
             * 1. 电流超过1A，  上一状态是1，状态不变，运行时间累加
             *                 上一状态是0，状态变1，停机时间累积
             * 2. 电流不超过1A，上一状态是1，状态变0，运行时间累加
             *                 上一状态是0，状态不变，停机时间累积
             */
            // 分钟
            long sub = (ycMediumVoltage.getColTime().getTime() - colTime.getTime()) / 1000 / 60;
            if (nowPhaseCurrent >= 1) {
                if (runstatus.equals(MEDIUMVOLTAGE_RUN)) {
                    runningTime += sub;
                } else {
                    runstatus = MEDIUMVOLTAGE_RUN;
                    downtime += sub;
                }
            } else {
                if (runstatus.equals(MEDIUMVOLTAGE_RUN)) {
                    runstatus = MEDIUMVOLTAGE_STOP;
                    runningTime += sub;
                } else {
                    downtime += sub;
                }
            }

            String updateSql = "update YC_MEDIUM_VOLTAGE_RUN_MONTH set COLTIME=?,RUNNINGTIME=?,DOWNTIME=?,RUNSTATUS=?,CREATETIME=?,PARTICULARTIME=? where YCMEDIUMVOLTAGERUNMONTHID=? ";
            preparedStatement = this.connection.prepareStatement(updateSql);
            preparedStatement.setTimestamp(1, new Timestamp(ycMediumVoltage.getColTime().getTime()));
            preparedStatement.setDouble(2, runningTime);
            preparedStatement.setDouble(3, downtime);
            preparedStatement.setInt(4, runstatus);
            preparedStatement.setTimestamp(5, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setString(6, RemindDateUtils.getNowMonth());
            preparedStatement.setInt(7, pkId);
            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            String insertSql = "INSERT INTO YC_MEDIUM_VOLTAGE_RUN_MONTH (IEDNAME,COLTIME,RUNNINGTIME,DOWNTIME,RUNSTATUS,CREATETIME,PARTICULARTIME) VALUES(?,?,?,?,?,?,?)";
            preparedStatement = this.connection.prepareStatement(insertSql);
            preparedStatement.setString(1, ycMediumVoltage.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(ycMediumVoltage.getColTime().getTime()));
            preparedStatement.setDouble(3, 0.0d);
            preparedStatement.setDouble(4, 0.0d);
            preparedStatement.setInt(5, nowPhaseCurrent > 1 ? MEDIUMVOLTAGE_RUN : MEDIUMVOLTAGE_STOP);
            preparedStatement.setTimestamp(6, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setString(7, RemindDateUtils.getNowMonth());
            preparedStatement.executeUpdate();
            JdbcUtil.close(preparedStatement, connection);
        }


    }


}