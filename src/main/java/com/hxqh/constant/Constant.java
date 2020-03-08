package com.hxqh.constant;

/**
 * Created by Ocean lin on 2019/7/9.
 *
 * @author Ocean lin
 */
public interface Constant {


    /**
     * Parameters
     */
    String BOOTSTRAP_SERVERS = "bootstrap.servers";
    String GROUP_ID = "group.id";
    String INPUT_EVENT_TOPIC = "input-event-topic";
    String INPUT_CONFIG_TOPIC = "input-config-topic";
    String OUTPUT_TOPIC = "output-topic";
    String RETRIES = "retries";


    Integer NUM = 6;

    Integer NUM_4 = 4;
    /**
     * 遥测
     */
    String YC = "YC";

    /**
     * 遥信
     */
    String YX = "YX";


    /**
     * 设备类别
     */
    String MEDIUM_VOLTAG_ESWITCH = "中压开关设备";
    String TRANSFORMER = "变压器";

    String LOW_VOLTAGE_SWITCHGEAR = "低压开关设备";

    String ATS = "低压开关设备-ATS";
    String CAPACITOR = "低压开关设备-电容器";
    String DRAWER_CABINET = "低压开关设备-抽屉柜";
    String ACB = "低压开关设备-ACB";



    /**
     * DB2 连接信息
     */
    String DB2_DRIVER_NAME = "com.ibm.db2.jcc.DB2Driver";
    String DB2_DB_URL = "jdbc:db2://tj-maximo.com:50005/maxdb";
    String DB2_USERNAME = "maximo";
    String DB2_PASSWORD = "maximo123";


    /**
     * ElasticSearch
     */

    String ES_HOST = "tj-hospital.com";
    Integer ES_PORT = 9200;


    /**
     * 遥测-AST
     */
    String INDEX_YC_ATS = "yc_ats";
    String TYPE_YC_ATS = "ats";


    /**
     * 遥信-AST
     */
    String INDEX_YX_ATS = "yx_ats";
    String TYPE_YX_ATS = "ats";

    String YX_ATS_QUICK_BREAK = "速断";
    String YX_ATS_OVER_CURRENT = "过流";
    String YX_ATS_SWITCH_POSITION = "开关位置";


    /**
     * 遥测-中压
     */
    String INDEX_YC_MEDIUMVOLTAGE_ = "yc_mediumvoltage";
    String TYPE_YC_MEDIUMVOLTAGE = "mediumvoltage";


    /**
     * 遥测-变压器
     */
    String INDEX_YC_TRANSFORMER = "yc_transformer";
    String TYPE_YC_TRANSFORMER = "transformer";

    /**
     * 遥信
     */
    String INDEX_YX = "yx";
    String TYPE_YX = "yx";

    /**
     * MySQL 连接信息
     */
    String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    String MYSQL_DB_URL = "jdbc:mysql://tj-hospital.com:3306/tj?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true";
    String MYSQL_USERNAME = "tj";
    String MYSQL_PASSWORD = "mko09ijn*";

}
