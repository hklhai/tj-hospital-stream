package com.hxqh.sink;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.utils.ConvertUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Date;

import static com.hxqh.constant.Constant.*;
import static com.hxqh.constant.StringConstant.*;

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
        Class.forName(MYSQL_DRIVER_NAME);
        connection = DriverManager.getConnection(MYSQL_DB_URL, MYSQL_USERNAME, MYSQL_PASSWORD);

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
            String updateSql = "update YC_MEDIUM_VOLTAGE_CURRENT set  COLTIME=?," + YCMEDIUMVOLTAGE_UPDATE + "CREATETIME=? where YCMEDIUMVOLTAGEID=? ";

            preparedStatement = connection.prepareStatement(updateSql);
            setValue(ycMediumVoltage);

            preparedStatement.setTimestamp(46, new Timestamp(now.getTime()));
            preparedStatement.setInt(47, pkId);
            preparedStatement.executeUpdate();

        } else {
            // 新增设备
            String insertSql = "INSERT INTO  YC_MEDIUM_VOLTAGE_CURRENT  (COLTIME," + YCMEDIUMVOLTAGE_INSERT + "CREATETIME,IEDNAME,YCMEDIUMVOLTAGEID) VALUES(?,"
                    + YCMEDIUMVOLTAGECOMMA + "?,?,NEXTVAL FOR  YC_MEDIUM_VOLTAGE_CURRENT_SEQ)";
            preparedStatement = connection.prepareStatement(insertSql);
            setValue(ycMediumVoltage);

            preparedStatement.setTimestamp(46, new Timestamp(now.getTime()));
            preparedStatement.setString(47, ycMediumVoltage.getIEDName());
            preparedStatement.executeUpdate();
        }

        // log 表新增
        String insertLogSql = "INSERT INTO YC_MEDIUM_VOLTAGE_LOG  (COLTIME," + YCMEDIUMVOLTAGE_INSERT + "CREATETIME,IEDNAME,YCMEDIUMVOLTAGELOGID) VALUES(?,"
                + YCMEDIUMVOLTAGECOMMA + "?,?,NEXTVAL FOR  YCATS_CURRENT_SEQ)";
        preparedStatement = connection.prepareStatement(insertLogSql);
        setValue(ycMediumVoltage);

        preparedStatement.setTimestamp(46, new Timestamp(now.getTime()));
        preparedStatement.setString(47, ycMediumVoltage.getIEDName());
        preparedStatement.executeUpdate();

    }

    private void setValue(YcMediumVoltage ycMediumVoltage) throws SQLException {
        preparedStatement.setTimestamp(1, new Timestamp(ycMediumVoltage.getColTime().getTime()));
        preparedStatement.setDouble(2, ycMediumVoltage.getCIRCUITBREAKER());
        preparedStatement.setDouble(3, ycMediumVoltage.getPOSITIVEREACTIVE());
        preparedStatement.setDouble(4, ycMediumVoltage.getPOSITIVEACTIVE());
        preparedStatement.setDouble(5, ycMediumVoltage.getEARTHKNIFE());
        preparedStatement.setDouble(6, ycMediumVoltage.getREVERSEREACTIVE());
        preparedStatement.setDouble(7, ycMediumVoltage.getREVERSEACTIVE());
        preparedStatement.setDouble(8, ycMediumVoltage.getHANDCARTPOSITION());
        preparedStatement.setDouble(9, ycMediumVoltage.getAMBIENTTEMPERATURE());
        preparedStatement.setDouble(10, ycMediumVoltage.getCCABLETEMPERATURE());
        preparedStatement.setDouble(11, ycMediumVoltage.getBCABLETEMPERATURE());
        preparedStatement.setDouble(12, ycMediumVoltage.getACABLETEMPERATURE());
        preparedStatement.setDouble(13, ycMediumVoltage.getCLOWERARMTEMPERATURE());
        preparedStatement.setDouble(14, ycMediumVoltage.getBLOWERARMTEMPERATURE());
        preparedStatement.setDouble(15, ycMediumVoltage.getALOWERARMTEMPERATURE());
        preparedStatement.setDouble(16, ycMediumVoltage.getCUPPERARMTEMPERATURE());
        preparedStatement.setDouble(17, ycMediumVoltage.getBUPPERARMTEMPERATURE());
        preparedStatement.setDouble(18, ycMediumVoltage.getAUPPERARMTEMPERATURE());
        preparedStatement.setDouble(19, ycMediumVoltage.getAPHASECURRENT());
        preparedStatement.setDouble(20, ycMediumVoltage.getBPHASECURRENT());
        preparedStatement.setDouble(21, ycMediumVoltage.getCPHASECURRENT());
        preparedStatement.setDouble(22, ycMediumVoltage.getABLINEVOLTAGE());
        preparedStatement.setDouble(23, ycMediumVoltage.getBCLINEVOLTAGE());
        preparedStatement.setDouble(24, ycMediumVoltage.getCALINEVOLTAGE());
        preparedStatement.setDouble(25, ycMediumVoltage.getZEROSEQUENCECURRENT());
        preparedStatement.setDouble(26, ycMediumVoltage.getFREQUENCY());
        preparedStatement.setDouble(27, ycMediumVoltage.getACTIVEPOWER());
        preparedStatement.setDouble(28, ycMediumVoltage.getREACTIVEPOWER());
        preparedStatement.setDouble(29, ycMediumVoltage.getAPPARENTPOWER());
        preparedStatement.setDouble(30, ycMediumVoltage.getACTIVEELECTRICDEGREE());
        preparedStatement.setDouble(31, ycMediumVoltage.getREACTIVEELECTRICDEGREE());

        preparedStatement.setDouble(32, ycMediumVoltage.getLINEVOLTAGE());
        preparedStatement.setDouble(33, ycMediumVoltage.getLINECURRENT());
        preparedStatement.setDouble(34, ycMediumVoltage.getCAPACITANCEREACTIVEPOWER());
        preparedStatement.setDouble(35, ycMediumVoltage.getREACTIVEPOWERSYMBOL());
        preparedStatement.setDouble(36, ycMediumVoltage.getCAPACITANCEREACTIVEPOWER());
        preparedStatement.setDouble(37, ycMediumVoltage.getNO1OPENINGVOLTAGE());
        preparedStatement.setDouble(38, ycMediumVoltage.getNO1BCAPACITANCECURRENT());
        preparedStatement.setDouble(39, ycMediumVoltage.getNO1CCAPACITANCECURRENT());
        preparedStatement.setDouble(40, ycMediumVoltage.getNO2OPENINGVOLTAGE());
        preparedStatement.setDouble(41, ycMediumVoltage.getNO2BCAPACITANCECURRENT());
        preparedStatement.setDouble(42, ycMediumVoltage.getNO2CCAPACITANCECURRENT());
        preparedStatement.setDouble(43, ycMediumVoltage.getNO3OPENINGVOLTAGE());
        preparedStatement.setDouble(44, ycMediumVoltage.getNO3BCAPACITANCECURRENT());
        preparedStatement.setDouble(45, ycMediumVoltage.getNO3CCAPACITANCECURR());
    }
}