package com.hxqh.task.sink;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

import static com.hxqh.constant.StringConstant.*;

/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MySQLYcMediumVoltageSink extends RichSinkFunction<String> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;

    @Override
    public void invoke(String value, Context context) throws Exception {
        connection = JdbcUtil.getConnection();
        IEDEntity entity = JSON.parseObject(value, IEDEntity.class);
        YcMediumVoltage ycMediumVoltage = ConvertUtils.convert2YcMediumVoltage(entity);

        String countSql = "select count(*) from YC_MEDIUM_VOLTAGE_CURRENT  where IEDNAME=?";
        preparedStatement = this.connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycMediumVoltage.getIEDName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }


        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select  YCMEDIUMVOLTAGEID from YC_MEDIUM_VOLTAGE_CURRENT where IEDNAME=? ";
            preparedStatement = this.connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycMediumVoltage.getIEDName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update YC_MEDIUM_VOLTAGE_CURRENT set  COLTIME=?," + YCMEDIUMVOLTAGE_UPDATE + "CREATETIME=? where YCMEDIUMVOLTAGEID=? ";

            preparedStatement = this.connection.prepareStatement(updateSql);
            setValue(ycMediumVoltage);

            preparedStatement.setTimestamp(46, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setInt(47, pkId);
            preparedStatement.executeUpdate();
            JdbcUtil.close(edNameResult, preparedStatement, connection);
        } else {
            // 新增设备
            String insertSql = "INSERT INTO  YC_MEDIUM_VOLTAGE_CURRENT  (COLTIME," + YCMEDIUMVOLTAGE_INSERT + "CREATETIME,IEDNAME) VALUES(?,"
                    + YCMEDIUMVOLTAGECOMMA + "?,?)";
            preparedStatement = this.connection.prepareStatement(insertSql);
            setValue(ycMediumVoltage);

            preparedStatement.setTimestamp(46, new Timestamp(DateUtils.getFormatDate().getTime()));
            preparedStatement.setString(47, ycMediumVoltage.getIEDName());
            preparedStatement.executeUpdate();
            JdbcUtil.close(preparedStatement, connection);
        }

    }

    private void setValue(YcMediumVoltage ycMediumVoltage) throws SQLException {
        preparedStatement.setTimestamp(1, new Timestamp(ycMediumVoltage.getColTime().getTime()));
        preparedStatement.setDouble(2, ycMediumVoltage.getCircuitBreaker());
        preparedStatement.setDouble(3, ycMediumVoltage.getPositiveReactive());
        preparedStatement.setDouble(4, ycMediumVoltage.getPositiveActive());
        preparedStatement.setDouble(5, ycMediumVoltage.getEarthKnife());
        preparedStatement.setDouble(6, ycMediumVoltage.getReverseReactive());
        preparedStatement.setDouble(7, ycMediumVoltage.getReverseActive());
        preparedStatement.setDouble(8, ycMediumVoltage.getHandcartPosition());
        preparedStatement.setDouble(9, ycMediumVoltage.getAmbientTemperature());
        preparedStatement.setDouble(10, ycMediumVoltage.getCCableTemperature());
        preparedStatement.setDouble(11, ycMediumVoltage.getBCableTemperature());
        preparedStatement.setDouble(12, ycMediumVoltage.getACableTemperature());
        preparedStatement.setDouble(13, ycMediumVoltage.getCLowerArmTemperature());
        preparedStatement.setDouble(14, ycMediumVoltage.getBLowerArmTemperature());
        preparedStatement.setDouble(15, ycMediumVoltage.getALowerArmTemperature());
        preparedStatement.setDouble(16, ycMediumVoltage.getCUpperArmTemperature());
        preparedStatement.setDouble(17, ycMediumVoltage.getBUpperArmTemperature());
        preparedStatement.setDouble(18, ycMediumVoltage.getAUpperArmTemperature());
        preparedStatement.setDouble(19, ycMediumVoltage.getAPhaseCurrent());
        preparedStatement.setDouble(20, ycMediumVoltage.getBPhaseCurrent());
        preparedStatement.setDouble(21, ycMediumVoltage.getCPhaseCurrent());
        preparedStatement.setDouble(22, ycMediumVoltage.getABLineVoltage());
        preparedStatement.setDouble(23, ycMediumVoltage.getBCLineVoltage());
        preparedStatement.setDouble(24, ycMediumVoltage.getCALineVoltage());
        preparedStatement.setDouble(25, ycMediumVoltage.getZeroSequenceCurrent());
        preparedStatement.setDouble(26, ycMediumVoltage.getFrequency());
        preparedStatement.setDouble(27, ycMediumVoltage.getActivePower());
        preparedStatement.setDouble(28, ycMediumVoltage.getReactivePower());
        preparedStatement.setDouble(29, ycMediumVoltage.getApparentPower());
        preparedStatement.setDouble(30, ycMediumVoltage.getActiveElectricDegree());
        preparedStatement.setDouble(31, ycMediumVoltage.getReactiveElectricDegree());

        preparedStatement.setDouble(32, ycMediumVoltage.getLineVoltage());
        preparedStatement.setDouble(33, ycMediumVoltage.getLineCurrent());
        preparedStatement.setDouble(34, ycMediumVoltage.getCapacitanceReactivePower());
        preparedStatement.setDouble(35, ycMediumVoltage.getReactivePowerSymbol());
        preparedStatement.setDouble(36, ycMediumVoltage.getCapacitanceActivePower());
        preparedStatement.setDouble(37, ycMediumVoltage.getNo1OpeningVoltage());
        preparedStatement.setDouble(38, ycMediumVoltage.getNo1BCapacitanceCurrent());
        preparedStatement.setDouble(39, ycMediumVoltage.getNo1CCapacitanceCurrent());
        preparedStatement.setDouble(40, ycMediumVoltage.getNo2OpeningVoltage());
        preparedStatement.setDouble(41, ycMediumVoltage.getNo2BCapacitanceCurrent());
        preparedStatement.setDouble(42, ycMediumVoltage.getNo2CCapacitanceCurrent());
        preparedStatement.setDouble(43, ycMediumVoltage.getNo3OpeningVoltage());
        preparedStatement.setDouble(44, ycMediumVoltage.getNo3BCapacitanceCurrent());
        preparedStatement.setDouble(45, ycMediumVoltage.getNo3CCapacitanceCurrent());
    }
}