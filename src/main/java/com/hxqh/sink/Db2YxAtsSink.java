package com.hxqh.sink;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YxAts;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.utils.ConvertUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Date;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/2/21.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
@Deprecated
public class Db2YxAtsSink extends RichSinkFunction<String> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    Integer count = 0;
    Integer pkId = 0;

    Timestamp colTime = null;
    String variableName = "";
    Integer val = -1;

    Double runningTime = 0.0;
    Double downTime = 0.0;
    Integer switchOnCount = 0;
    Integer switchOffCount = 0;

    @Override
    public void invoke(String value, Context context) throws Exception {
        Date now = new Date();
        Class.forName(MYSQL_DRIVER_NAME);
        connection = DriverManager.getConnection(MYSQL_DB_URL, MYSQL_USERNAME, MYSQL_PASSWORD);

        IEDEntity entity = JSON.parseObject(value, IEDEntity.class);
        YxAts yxAts = ConvertUtils.convert2YxAts(entity);

        String countSql = "select count(*) from YXATS_CURRENT  where IEDNAME=? and VARIABLENAME=?";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, yxAts.getIEDName());
        preparedStatement.setString(2, yxAts.getVariableName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }


        if (1 == count) {
            String iedNameSQL = "select YXATSID,COLTIME,VARIABLENAME,VAL,RUNNINGTIME,DOWNTIME,SWITCHONCOUNT,SWITCHOFFCOUNT from YXATS_CURRENT where IEDNAME=? and VARIABLENAME=?";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, yxAts.getIEDName());
            preparedStatement.setString(2, yxAts.getVariableName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
                colTime = edNameResult.getTimestamp(2);
                variableName = edNameResult.getString(3);
                val = edNameResult.getInt(4);
                runningTime = edNameResult.getDouble(5);
                downTime = edNameResult.getDouble(6);
                switchOnCount = edNameResult.getInt(7);
                switchOffCount = edNameResult.getInt(8);
            }

            if (variableName.equals(YX_ATS_QUICK_BREAK) || YX_ATS_QUICK_BREAK.equals(YX_ATS_OVER_CURRENT)) {
                // 分钟
                Double div = (yxAts.getColTime().getTime() - colTime.getTime()) * 1.0 / 1000 / 60 / 60;
                if (val == 0) {
                    runningTime += div;
                } else if (val == 1) {
                    downTime += div;
                }
            } else if (variableName.equals(YX_ATS_SWITCH_POSITION)) {
                if (val == 0) {
                    switchOffCount += 1;
                } else if (val == 1) {
                    switchOnCount += 1;
                }

            }

            // 更新设备
            String updateSql = "update YXATS_CURRENT set COLTIME=?,VARIABLENAME=?,VAL=?,CREATETIME=?,RUNNINGTIME=?,DOWNTIME=?,SWITCHONCOUNT=?,SWITCHOFFCOUNT=? where YXATSID=? ";
            preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setTimestamp(1, new Timestamp(entity.getColTime().getTime()));
            preparedStatement.setString(2, yxAts.getVariableName());
            preparedStatement.setInt(3, yxAts.getValue());
            preparedStatement.setTimestamp(4, new Timestamp(now.getTime()));
            preparedStatement.setDouble(5, runningTime);
            preparedStatement.setDouble(6, downTime);
            preparedStatement.setDouble(7, switchOnCount);
            preparedStatement.setDouble(8, switchOffCount);
            preparedStatement.setInt(9, pkId);
            preparedStatement.executeUpdate();

        } else {
            // 新增设备
            String insertSql = "INSERT INTO YXATS_CURRENT (YXATSID,IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME,RUNNINGTIME,DOWNTIME,SWITCHONCOUNT,SWITCHOFFCOUNT) VALUES(NEXTVAL FOR  YXATS_CURRENT_SEQ,?,?,?,?,?,?,?,?,?)";
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setString(1, yxAts.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(yxAts.getColTime().getTime()));
            preparedStatement.setString(3, yxAts.getVariableName());
            preparedStatement.setInt(4, yxAts.getValue());
            preparedStatement.setTimestamp(5, new Timestamp(now.getTime()));
            preparedStatement.setDouble(6, 0.0f);
            preparedStatement.setDouble(7, 0.0f);
            preparedStatement.setDouble(8, 0.0f);
            preparedStatement.setDouble(9, 0.0f);
            preparedStatement.executeUpdate();
        }

        // log 表新增
        String insertLogSql = "INSERT INTO YXATS_LOG  (YXATSID,IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME) VALUES(NEXTVAL FOR  YXATS_LOG_SEQ,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(insertLogSql);
        preparedStatement.setString(1, yxAts.getIEDName());
        preparedStatement.setTimestamp(2, new Timestamp(yxAts.getColTime().getTime()));
        preparedStatement.setString(3, yxAts.getVariableName());
        preparedStatement.setInt(4, yxAts.getValue());
        preparedStatement.setTimestamp(5, new Timestamp(now.getTime()));
        preparedStatement.executeUpdate();
    }


}
