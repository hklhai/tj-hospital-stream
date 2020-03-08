package com.hxqh.sink;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcAts;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.utils.ConvertUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Date;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/2/17.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
@Deprecated
public class Db2YcAtsSink extends RichSinkFunction<String> {


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
        YcAts ycAts = ConvertUtils.convert2YcAts(entity);

        String countSql = "select count(*) from YCATS_CURRENT  where IEDNAME=?";
        preparedStatement = connection.prepareStatement(countSql);
        preparedStatement.setString(1, ycAts.getIEDName());
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }


        if (1 == count) {
            // 获取主键
            String iedNameSQL = "select YCATSID from YCATS_CURRENT where IEDNAME=? ";
            preparedStatement = connection.prepareStatement(iedNameSQL);
            preparedStatement.setString(1, ycAts.getIEDName());
            ResultSet edNameResult = preparedStatement.executeQuery();
            if (edNameResult.next()) {
                pkId = edNameResult.getInt(1);
            }

            // 更新设备
            String updateSql = "update YCATS_CURRENT set COLTIME=?,UA=?,UB=?,UC=?,IA=?,IB=?,IC=?,CREATETIME=? where YCATSID=? ";
            preparedStatement = connection.prepareStatement(updateSql);
            preparedStatement.setTimestamp(1, new Timestamp(ycAts.getColTime().getTime()));
            preparedStatement.setDouble(2, ycAts.getUA());
            preparedStatement.setDouble(3, ycAts.getUB());
            preparedStatement.setDouble(4, ycAts.getUC());
            preparedStatement.setDouble(5, ycAts.getIA());
            preparedStatement.setDouble(6, ycAts.getIB());
            preparedStatement.setDouble(7, ycAts.getIC());
            preparedStatement.setTimestamp(8, new Timestamp(now.getTime()));
            preparedStatement.setInt(9, pkId);
            preparedStatement.executeUpdate();

        } else {
            // 新增设备
            String insertSql = "INSERT INTO YCATS_CURRENT  (YCATSID,IEDNAME,COLTIME,UA,UB,UC,IA,IB,IC,CREATETIME) VALUES(NEXTVAL FOR  YCATS_CURRENT_SEQ,?,?,?,?,?,?,?,?,?)";
            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.setString(1, ycAts.getIEDName());
            preparedStatement.setTimestamp(2, new Timestamp(ycAts.getColTime().getTime()));
            preparedStatement.setDouble(3, ycAts.getUA());
            preparedStatement.setDouble(4, ycAts.getUB());
            preparedStatement.setDouble(5, ycAts.getUC());
            preparedStatement.setDouble(6, ycAts.getIA());
            preparedStatement.setDouble(7, ycAts.getIB());
            preparedStatement.setDouble(8, ycAts.getIC());
            preparedStatement.setTimestamp(9, new Timestamp(now.getTime()));
            preparedStatement.executeUpdate();
        }

        // log 表新增
        String insertLogSql = "INSERT INTO YCATS_LOG  (YCATSID,IEDNAME,COLTIME,UA,UB,UC,IA,IB,IC,CREATETIME) VALUES(NEXTVAL FOR  YCATS_LOG_SEQ,?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(insertLogSql);
        preparedStatement.setString(1, ycAts.getIEDName());
        preparedStatement.setTimestamp(2, new Timestamp(ycAts.getColTime().getTime()));
        preparedStatement.setDouble(3, ycAts.getUA());
        preparedStatement.setDouble(4, ycAts.getUB());
        preparedStatement.setDouble(5, ycAts.getUC());
        preparedStatement.setDouble(6, ycAts.getIA());
        preparedStatement.setDouble(7, ycAts.getIB());
        preparedStatement.setDouble(8, ycAts.getIC());
        preparedStatement.setTimestamp(9, new Timestamp(now.getTime()));
        preparedStatement.executeUpdate();
    }
}
