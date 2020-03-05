package com.hxqh.constant;

import java.io.Serializable;

/**
 * 在生产上一般通过配置中心来管理
 */
public class GlobalConfig implements Serializable {


    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://tj-hospital.com:3306/canal?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "canal";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "canal";
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    /**
     * HBase相关配置
     */
    public static final String HBASE_ZOOKEEPER_QUORUM = "tj-hospital.com";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";



}
