package com.hxqh.sync;


import com.hxqh.utils.JdbcUtil4Db2;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

import static com.hxqh.constant.Constant.*;

/**
 * 变压器、中压设备、低压设备遥测数据同步
 * <p>
 * Created by Ocean lin on 2020/3/9.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class SyncYxMySQL2Db2 {

    public static void main(String[] args) throws Exception {
        final TypeInformation<?>[] fieldTypes = getFieldTypes();
        final int[] type = getType();

        final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        String selectQuery = "select IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME from yx_current";

        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
                JDBCInputFormat.buildJDBCInputFormat().setDrivername(MYSQL_DRIVER_NAME).setDBUrl(MYSQL_DB_URL)
                        .setQuery(selectQuery).setRowTypeInfo(rowTypeInfo).setUsername(MYSQL_USERNAME)
                        .setPassword(MYSQL_PASSWORD);

        DataSet<Row> source = environment.createInput(inputBuilder.finish());


        Connection connection = JdbcUtil4Db2.getConnection();
        String truncateSql = "TRUNCATE TABLE YX_CURRENT IMMEDIATE";
        PreparedStatement preparedStatement = connection.prepareStatement(truncateSql);
        preparedStatement.execute();
        JdbcUtil4Db2.close(preparedStatement, connection);

        String insertQuery = "INSERT INTO yx_current (IEDNAME,COLTIME,VARIABLENAME,VAL,CREATETIME) VALUES(?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder outputBuilder =
                JDBCOutputFormat.buildJDBCOutputFormat().setDrivername(DB2_DRIVER_NAME).setDBUrl(DB2_DB_URL)
                        .setQuery(insertQuery).setSqlTypes(type).setUsername(DB2_USERNAME).setPassword(DB2_PASSWORD);
        source.output(outputBuilder.finish());

        environment.execute("SyncYxMySQL2Db2");

    }


    private static TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{BasicTypeInfo.STRING_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP,
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP
        };
    }


    private static int[] getType() {
        return new int[]{Types.VARCHAR, Types.TIMESTAMP,
                Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP
        };
    }
}
