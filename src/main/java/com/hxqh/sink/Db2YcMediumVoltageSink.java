package com.hxqh.sink;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.utils.ConvertUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Date;

import static com.hxqh.constant.Constant.*;
import static com.hxqh.constant.StringConstant.YCMEDIUMVOLTAGE;
import static com.hxqh.constant.StringConstant.YCMEDIUMVOLTAGECOMMA;

/**
 * Created by Ocean lin on 2020/2/29.
 *
 * @author Ocean lin
 */
public class Db2YcMediumVoltageSink extends RichSinkFunction<String> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;

    @Override
    public void invoke(String value, Context context) throws Exception {
        Date now = new Date();
        Class.forName(DRIVER_NAME);
        connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

        IEDEntity entity = JSON.parseObject(value, IEDEntity.class);
        YcMediumVoltage ycMediumVoltage = ConvertUtils.convert2YcMediumVoltage(entity);

        String countSql = "select count(*) from YC_MEDIUM_VOLTAGE_CURRENT  where IEDNAME=?";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycMediumVoltage.getIEDName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }


        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select  YCMEDIUMVOLTAGEID from YC_MEDIUM_VOLTAGE_CURRENT where IEDNAME=? ";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycMediumVoltage.getIEDName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update YC_MEDIUM_VOLTAGE_CURRENT set  COLTIME=?," + YCMEDIUMVOLTAGE + "CREATETIME=? where YCMEDIUMVOLTAGEID=? ";

            preparedStatement = connection.prepareStatement(updateSql);
            setValue(ycMediumVoltage);

            preparedStatement.setTimestamp(31, new Timestamp(now.getTime()));
            preparedStatement.setInt(32, pkId);
            preparedStatement.executeUpdate();

        } else {
            // 新增设备
            String insertSql = "INSERT INTO  YC_MEDIUM_VOLTAGE_CURRENT  (COLTIME," + YCMEDIUMVOLTAGE + "CREATETIME,IEDNAME,YCMEDIUMVOLTAGEID) VALUES(?,"
                    + YCMEDIUMVOLTAGECOMMA + "?,?,NEXTVAL FOR  YC_MEDIUM_VOLTAGE_CURRENT_SEQ)";
            preparedStatement = connection.prepareStatement(insertSql);
            setValue(ycMediumVoltage);

            preparedStatement.setTimestamp(31, new Timestamp(now.getTime()));
            preparedStatement.setString(32, ycMediumVoltage.getIEDName());
            preparedStatement.executeUpdate();
        }

        // log 表新增
        String insertLogSql = "INSERT INTO YC_MEDIUM_VOLTAGE_LOG  (COLTIME," + YCMEDIUMVOLTAGE + "CREATETIME,IEDNAME,YCMEDIUMVOLTAGELOGID) VALUES(?,"
                + YCMEDIUMVOLTAGECOMMA + "?,?,NEXTVAL FOR  YCATS_CURRENT_SEQ)";
        preparedStatement = connection.prepareStatement(insertLogSql);
        setValue(ycMediumVoltage);

        preparedStatement.setTimestamp(31, new Timestamp(now.getTime()));
        preparedStatement.setString(32, ycMediumVoltage.getIEDName());
        preparedStatement.executeUpdate();

    }

    private void setValue(YcMediumVoltage ycMediumVoltage) throws SQLException {
        preparedStatement.setTimestamp(1, new Timestamp(ycMediumVoltage.getColTime().getTime()));
        preparedStatement.setInt(2, ycMediumVoltage.getCIRCUITBREAKER());
        preparedStatement.setInt(3, ycMediumVoltage.getPOSITIVEREACTIVE());
        preparedStatement.setInt(4, ycMediumVoltage.getPOSITIVEACTIVE());
        preparedStatement.setInt(5, ycMediumVoltage.getEARTHKNIFE());
        preparedStatement.setInt(6, ycMediumVoltage.getREVERSEREACTIVE());
        preparedStatement.setInt(7, ycMediumVoltage.getREVERSEACTIVE());
        preparedStatement.setInt(8, ycMediumVoltage.getHANDCARTPOSITION());
        preparedStatement.setInt(9, ycMediumVoltage.getAMBIENTTEMPERATURE());
        preparedStatement.setInt(10, ycMediumVoltage.getCCABLETEMPERATURE());
        preparedStatement.setInt(11, ycMediumVoltage.getBCABLETEMPERATURE());
        preparedStatement.setInt(12, ycMediumVoltage.getACABLETEMPERATURE());
        preparedStatement.setInt(13, ycMediumVoltage.getCLOWERARMTEMPERATURE());
        preparedStatement.setInt(14, ycMediumVoltage.getBLOWERARMTEMPERATURE());
        preparedStatement.setInt(15, ycMediumVoltage.getALOWERARMTEMPERATURE());
        preparedStatement.setInt(16, ycMediumVoltage.getCUPPERARMTEMPERATURE());
        preparedStatement.setInt(17, ycMediumVoltage.getBUPPERARMTEMPERATURE());
        preparedStatement.setInt(18, ycMediumVoltage.getAUPPERARMTEMPERATURE());
        preparedStatement.setInt(19, ycMediumVoltage.getAPHASECURRENT());
        preparedStatement.setInt(20, ycMediumVoltage.getBPHASECURRENT());
        preparedStatement.setInt(21, ycMediumVoltage.getCPHASECURRENT());
        preparedStatement.setInt(22, ycMediumVoltage.getABLINEVOLTAGE());
        preparedStatement.setInt(23, ycMediumVoltage.getBCLINEVOLTAGE());
        preparedStatement.setInt(24, ycMediumVoltage.getCALINEVOLTAGE());
        preparedStatement.setInt(25, ycMediumVoltage.getZEROSEQUENCECURRENT());
        preparedStatement.setInt(26, ycMediumVoltage.getFREQUENCY());
        preparedStatement.setInt(27, ycMediumVoltage.getACTIVEPOWER());
        preparedStatement.setInt(28, ycMediumVoltage.getREACTIVEPOWER());
        preparedStatement.setInt(29, ycMediumVoltage.getACTIVEELECTRICDEGREE());
        preparedStatement.setInt(30, ycMediumVoltage.getREACTIVEELECTRICDEGREE());
    }
}