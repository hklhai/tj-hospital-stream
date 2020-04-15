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
 * Created by Ocean lin on 2020/4/7.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYcMediumVoltageRunStatusSink extends RichSinkFunction<String> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;
    Timestamp colTime = new Timestamp(DateUtils.getFormatDate().getTime());
    Double phaseCurrent = 0.0d;
    Double runningTime1 = 0.0d, runningTime2 = 0.0d, runningTime3 = 0.0d, runningTime4 = 0.0d;
    Double downtime1 = 0.0d, downtime2 = 0.0d, downtime3 = 0.0d, downtime4 = 0.0d;
    Integer runstatus = MEDIUMVOLTAGE_STOP;

    @Override
    public void invoke(String value, Context context) throws Exception {
        connection = JdbcUtil.getConnection();
        IEDEntity entity = JSON.parseObject(value, IEDEntity.class);
        YcMediumVoltage ycMediumVoltage = ConvertUtils.convert2YcMediumVoltage(entity);
        Double nowPhaseCurrent = (ycMediumVoltage.getAPhaseCurrent() + ycMediumVoltage.getBPhaseCurrent() + ycMediumVoltage.getCPhaseCurrent()) / 3;

        String countSql = "select count(*) from YC_MEDIUM_VOLTAGE_RUN where IEDNAME=? and PARTICULARYEAR=?";
        preparedStatement = this.connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycMediumVoltage.getIEDName());
        preparedStatement.setInt(2, RemindDateUtils.getNowYear());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select YCMEDIUMVOLTAGERUNID,COLTIME,PHASECURRENT,RUNNINGTIME1,DOWNTIME1,RUNNINGTIME2,DOWNTIME2,RUNNINGTIME3,DOWNTIME3,RUNNINGTIME4,DOWNTIME4,RUNSTATUS from YC_MEDIUM_VOLTAGE_RUN where IEDNAME=? and PARTICULARYEAR=? ";
            preparedStatement = this.connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycMediumVoltage.getIEDName());
            preparedStatement.setInt(2, RemindDateUtils.getNowYear());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
                colTime = edNameResult.getTimestamp(2);
                phaseCurrent = edNameResult.getDouble(3);
                runningTime1 = edNameResult.getDouble(4);
                downtime1 = edNameResult.getDouble(5);
                runningTime2 = edNameResult.getDouble(6);
                downtime2 = edNameResult.getDouble(7);
                runningTime3 = edNameResult.getDouble(8);
                downtime3 = edNameResult.getDouble(9);
                runningTime4 = edNameResult.getDouble(10);
                downtime4 = edNameResult.getDouble(11);
                runstatus = edNameResult.getInt(12);
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
                    if (DateUtils.getQuarter(colTime) == 1) {
                        runningTime1 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 2) {
                        runningTime2 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 3) {
                        runningTime3 += sub;
                    } else {
                        runningTime4 += sub;
                    }
                } else {
                    runstatus = MEDIUMVOLTAGE_RUN;
                    if (DateUtils.getQuarter(colTime) == 1) {
                        downtime1 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 2) {
                        downtime2 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 3) {
                        downtime3 += sub;
                    } else {
                        downtime4 += sub;
                    }
                }
            } else {
                if (runstatus.equals(MEDIUMVOLTAGE_RUN)) {
                    runstatus = MEDIUMVOLTAGE_STOP;
                    if (DateUtils.getQuarter(colTime) == 1) {
                        runningTime1 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 2) {
                        runningTime2 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 3) {
                        runningTime3 += sub;
                    } else {
                        runningTime4 += sub;
                    }
                } else {
                    if (DateUtils.getQuarter(colTime) == 1) {
                        downtime1 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 2) {
                        downtime2 += sub;
                    } else if (DateUtils.getQuarter(colTime) == 3) {
                        downtime3 += sub;
                    } else {
                        downtime4 += sub;
                    }
                }
            }

            String updateSql = "update YC_MEDIUM_VOLTAGE_RUN set COLTIME=?,PHASECURRENT=?,RUNNINGTIME1=?,DOWNTIME1=?,RUNNINGTIME2=?,DOWNTIME2=?,RUNNINGTIME3=?,DOWNTIME3=?,RUNNINGTIME4=?,DOWNTIME4=?,RUNSTATUS=?,CREATETIME=?,PARTICULARYEAR=? where YCMEDIUMVOLTAGERUNID=? ";
            preparedStatement = this.connection.prepareStatement(updateSql);
            preparedStatement.setTimestamp(1, new Timestamp(ycMediumVoltage.getColTime().getTime()));
            preparedStatement.setDouble(2, nowPhaseCurrent);
            preparedStatement.setDouble(3, runningTime1);
            preparedStatement.setDouble(4, downtime1);
            preparedStatement.setDouble(5, runningTime2);
            preparedStatement.setDouble(6, downtime2);
            preparedStatement.setDouble(7, runningTime3);
            preparedStatement.setDouble(8, downtime3);
            preparedStatement.setDouble(9, runningTime4);
            preparedStatement.setDouble(10, downtime4);
            preparedStatement.setInt(11, runstatus);
            preparedStatement.setTimestamp(12, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setInt(13, RemindDateUtils.getNowYear());
            preparedStatement.setInt(14, pkId);
            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            String insertSql = "INSERT INTO  YC_MEDIUM_VOLTAGE_RUN (IEDNAME,COLTIME,PHASECURRENT,RUNNINGTIME1,DOWNTIME1,RUNNINGTIME2,DOWNTIME2,RUNNINGTIME3,DOWNTIME3,RUNNINGTIME4,DOWNTIME4,RUNSTATUS,CREATETIME,PARTICULARYEAR) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            preparedStatement = this.connection.prepareStatement(insertSql);
            preparedStatement.setString(1, ycMediumVoltage.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(ycMediumVoltage.getColTime().getTime()));
            preparedStatement.setDouble(3, nowPhaseCurrent);
            preparedStatement.setDouble(4, runningTime1);
            preparedStatement.setDouble(5, downtime1);
            preparedStatement.setDouble(6, runningTime2);
            preparedStatement.setDouble(7, downtime2);
            preparedStatement.setDouble(8, runningTime3);
            preparedStatement.setDouble(9, downtime3);
            preparedStatement.setDouble(10, runningTime4);
            preparedStatement.setDouble(11, downtime4);

            preparedStatement.setInt(12, nowPhaseCurrent > 1 ? MEDIUMVOLTAGE_RUN : MEDIUMVOLTAGE_STOP);
            preparedStatement.setTimestamp(13, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setInt(14, RemindDateUtils.getNowYear());
            preparedStatement.executeUpdate();
            JdbcUtil.close(preparedStatement, connection);
        }


    }


}