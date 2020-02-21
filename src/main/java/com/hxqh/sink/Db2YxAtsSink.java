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
public class Db2YxAtsSink extends RichSinkFunction<String> {

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
        YxAts yxAts = ConvertUtils.convert2YxAts(entity);

        String countSql = "select count(*) from YXAST_CURRENT  where IEDNAME=? and VARIABLENAME=?";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, yxAts.getIEDName());
        preparedStatement.setString(2, yxAts.getVariableName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }


        if (1 == count) {
            String iedNameSQL = "select YXASTID from YXAST_CURRENT where IEDNAME=? and VARIABLENAME=?";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, yxAts.getIEDName());
            preparedStatement.setString(2, yxAts.getVariableName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update YXAST_CURRENT set COLTIME=?,VARIABLENAME=?,VAL=?,CREATETIME=? where YXASTID=? ";
            preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setTimestamp(1, new Timestamp(entity.getColTime().getTime()));
            preparedStatement.setString(2, yxAts.getVariableName());
            preparedStatement.setInt(3, yxAts.getValue());
            preparedStatement.setTimestamp(4, new Timestamp(now.getTime()));
            preparedStatement.setInt(5, pkId);
            preparedStatement.executeUpdate();

        } else {
            // 新增设备
            String insertSql = "INSERT INTO YXAST_CURRENT (YXASTID,IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME) VALUES(NEXTVAL FOR  YXATS_CURRENT_SEQ,?,?,?,?,?)";
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setString(1, yxAts.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(yxAts.getColTime().getTime()));
            preparedStatement.setString(3, yxAts.getVariableName());
            preparedStatement.setInt(4, yxAts.getValue());
            preparedStatement.setTimestamp(5, new Timestamp(now.getTime()));
            preparedStatement.executeUpdate();
        }

        // log 表新增
        String insertLogSql = "INSERT INTO YXATS_LOG  (YXASTID,IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME) VALUES(NEXTVAL FOR  YXATS_LOG_SEQ,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(insertLogSql);
        preparedStatement.setString(1, yxAts.getIEDName());
        preparedStatement.setTimestamp(2, new Timestamp(yxAts.getColTime().getTime()));
        preparedStatement.setString(3, yxAts.getVariableName());
        preparedStatement.setInt(4, yxAts.getValue());
        preparedStatement.setTimestamp(5, new Timestamp(now.getTime()));
        preparedStatement.executeUpdate();


    }
}
