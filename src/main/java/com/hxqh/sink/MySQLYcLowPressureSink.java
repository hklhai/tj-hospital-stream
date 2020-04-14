package com.hxqh.sink;

import com.hxqh.domain.YcLowPressure;
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
public class MySQLYcLowPressureSink extends RichSinkFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;


    @Override
    public void invoke(Row row, Context context) throws Exception {
        connection = JdbcUtil.getConnection();

        YcLowPressure ycLowPressure = ConvertUtils.convert2YcLowPressure(row);

        String countSql = "select count(*) from yc_lowpressure_current where IEDNAME=? ";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycLowPressure.getIEDName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }

        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select YCLOWPRESSURECURRENTID from yc_lowpressure_current where IEDNAME=?";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycLowPressure.getIEDName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update yc_lowpressure_current set COLTIME=?,CREATETIME=?,PhaseL1CurrentPercent=?,PhaseL1L2Voltage=?,PhaseL2CurrentPercent=?,PhaseL2L3Voltage=?,PhaseL3CurrentPercent=?,PhaseL3L1Voltage=?" +
                    " PositiveActive=?,PositiveReactive=?,PowerFactor=?,OperationNumber=? where YCLOWPRESSURECURRENTID =?";

            preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setTimestamp(1, new Timestamp(ycLowPressure.getColTime().getTime()));
            preparedStatement.setTimestamp(2, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setDouble(3, ycLowPressure.getPhaseL1CurrentPercent());
            preparedStatement.setDouble(4, ycLowPressure.getPhaseL1L2Voltage());
            preparedStatement.setDouble(5, ycLowPressure.getPhaseL2CurrentPercent());
            preparedStatement.setDouble(6, ycLowPressure.getPhaseL2L3Voltage());
            preparedStatement.setDouble(7, ycLowPressure.getPhaseL3CurrentPercent());
            preparedStatement.setDouble(8, ycLowPressure.getPhaseL3L1Voltage());

            preparedStatement.setDouble(9, ycLowPressure.getPositiveActive());
            preparedStatement.setDouble(10, ycLowPressure.getPositiveReactive());
            preparedStatement.setDouble(11, ycLowPressure.getPowerFactor());
            preparedStatement.setInt(12, ycLowPressure.getOperationNumber());

            preparedStatement.setInt(13, pkId);
            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            // 新增设备
            String insertSql = "INSERT INTO yc_lowpressure_current (IEDNAME,COLTIME,CREATETIME,PhaseL1CurrentPercent,PhaseL1L2Voltage,PhaseL2CurrentPercent,PhaseL2L3Voltage,PhaseL3CurrentPercent,PhaseL3L1Voltage,PositiveActive,PositiveReactive,PowerFactor,OperationNumber) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setString(1, ycLowPressure.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(ycLowPressure.getColTime().getTime()));
            preparedStatement.setTimestamp(3, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setDouble(4, ycLowPressure.getPhaseL1CurrentPercent());
            preparedStatement.setDouble(5, ycLowPressure.getPhaseL1L2Voltage());
            preparedStatement.setDouble(6, ycLowPressure.getPhaseL2CurrentPercent());
            preparedStatement.setDouble(7, ycLowPressure.getPhaseL2L3Voltage());
            preparedStatement.setDouble(8, ycLowPressure.getPhaseL3CurrentPercent());
            preparedStatement.setDouble(9, ycLowPressure.getPhaseL3L1Voltage());
            preparedStatement.setDouble(10, ycLowPressure.getPositiveActive());
            preparedStatement.setDouble(11, ycLowPressure.getPositiveReactive());
            preparedStatement.setDouble(12, ycLowPressure.getPowerFactor());
            preparedStatement.setInt(13, ycLowPressure.getOperationNumber());

            preparedStatement.executeUpdate();
            JdbcUtil.close(resultSet, preparedStatement, connection);
        }
    }


}
