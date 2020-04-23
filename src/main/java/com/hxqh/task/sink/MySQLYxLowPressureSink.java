package com.hxqh.task.sink;

import com.hxqh.batch.lowpressure.useefficiency.LowpressureUeRunTimeUpdateMonth;
import com.hxqh.batch.lowpressure.useefficiency.LowpressureUseEfficiencyRunTimeMonth;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JdbcUtil;
import com.hxqh.utils.JdbcUtil4Db2;
import com.hxqh.utils.RemindDateUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import static com.hxqh.constant.Constant.DEVICE_RUN;
import static com.hxqh.constant.Constant.DEVICE_STOP;

/**
 * 处理低压设备运行时长统计:0:通；1：断
 * <p>
 * 1. 定时任务每月月末将下月时间存入，默认RUNSTATUS=0，默认运行时长为全月小时数，默认停机时长0小时 {@link LowpressureUseEfficiencyRunTimeMonth }
 * 2. 定时任务每月月初根据上月RUNSTATUS更新当月RUNSTATUS，并计算结算上月运行时间及故障时间 {@link LowpressureUeRunTimeUpdateMonth}
 * 3. 实时处理遥信变压器风机信号量
 * 若RUNSTATUS状态为0，遥信数据1，记录时间，变更状态（正常-->故障）
 * 若RUNSTATUS状态为0，遥信数据0，记录时间，状态不变（正常-->正常）
 * <p>
 * 若RUNSTATUS状态为1，遥信数据0，记录时间，当前事件时间减数据库上一事件时间得div，运行时间-div，停机时间+div，改变状态（故障-->正常）
 * 若RUNSTATUS状态为1，遥信数据1，记录时间，当前事件时间减数据库上一事件时间得div，运行时间-div，停机时间+div，状态不变（故障-->故障）
 * <p>
 * Created by Ocean lin on 2020/4/23.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYxLowPressureSink extends RichSinkFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;

    Double runningTime = 0.0d;
    Double downTime = 0.0d;
    Integer runStatus = 0;
    Timestamp preColTime = new Timestamp(System.currentTimeMillis());
    Integer finalRunStatus = 0;

    @Override
    public void invoke(Row row, Context context) throws Exception {
        connection = JdbcUtil4Db2.getConnection();

        String particularTime = RemindDateUtils.getNowMonth();
        String iedName = row.getField(0).toString();
        Timestamp colTime = new Timestamp(DateUtils.formatDate(row.getField(2).toString()).getTime());
        Integer val = Integer.parseInt(((Row[]) row.getField(11))[0].getField(1).toString());

        String countSql = "select count(*) from RE_LP_UE_RUN_MONTH where IEDNAME=? and PARTICULARTIME=?";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, iedName);
        preparedStatement.setString(2, particularTime);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select RELPUERUNMONTHID,RUNNINGTIME,DOWNTIME,RUNSTATUS,COLTIME from RE_LP_UE_RUN_MONTH where IEDNAME=? and PARTICULARTIME=?";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, iedName);
            preparedStatement.setString(2, particularTime);
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
                runningTime = edNameResult.getDouble(2);
                downTime = edNameResult.getDouble(3);
                runStatus = edNameResult.getInt(4);
                preColTime = edNameResult.getTimestamp(5);
            }

            // 更新设备
            String updateSql = "update RE_LP_UE_RUN_MONTH set COLTIME=?,RUNNINGTIME=?,DOWNTIME=?,RUNSTATUS=? where RELPUERUNMONTHID=?";
            preparedStatement = connection.prepareStatement(updateSql);

            if (DEVICE_RUN.equals(runStatus)) {
                if (DEVICE_STOP.equals(val)) {
                    // 若RUNSTATUS状态为0，遥信数据1，记录时间，变更状态（正常-- > 故障）
                    finalRunStatus = DEVICE_STOP;
                } else {
                    // 若RUNSTATUS状态为0，遥信数据0，记录时间，状态不变（正常-- > 正常）
                }
            } else {
                // 分钟
                Double div = (colTime.getTime() - preColTime.getTime()) * 1.0 / 1000 / 60;
                if (DEVICE_RUN.equals(val)) {
                    // 若RUNSTATUS状态为1，遥信数据0，记录时间，当前事件时间减数据库上一事件时间得div，运行时间 - div，停机时间 + div，改变状态（故障-- > 正常）
                    finalRunStatus = DEVICE_RUN;
                } else {
                    // 若RUNSTATUS状态为1，遥信数据1，记录时间，当前事件时间减数据库上一事件时间得div，运行时间 - div，停机时间 + div，状态不变（故障-- > 故障）
                }
                runningTime -= div;
                downTime += div;
            }

            preparedStatement.setTimestamp(1, colTime);
            preparedStatement.setDouble(2, runningTime);
            preparedStatement.setDouble(3, downTime);
            preparedStatement.setInt(4, finalRunStatus);
            preparedStatement.setInt(5, pkId);

            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        }

    }
}
