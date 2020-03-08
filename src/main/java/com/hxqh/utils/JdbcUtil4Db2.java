package com.hxqh.utils;


import java.sql.*;

import static com.hxqh.constant.Constant.*;


/**
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class JdbcUtil4Db2 {
    private static String url = DB2_DB_URL;
    private static String user = DB2_USERNAME;
    private static String password = DB2_PASSWORD;
    private static String driverClass = DB2_DRIVER_NAME;


    /**
     * 只注册一次，静态代码块
     */
    static {

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取连接方法
     */
    public static Connection getConnection() {
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(Statement stmt, Connection conn) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 释放资源的方法
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(JdbcUtil4Db2.getConnection());
    }

}

