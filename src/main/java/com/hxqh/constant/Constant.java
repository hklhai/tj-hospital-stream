package com.hxqh.constant;

/**
 * Created by Ocean lin on 2019/7/9.
 *
 * @author Ocean lin
 */
public interface Constant {
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
    String ATS = "ATS";

    /**
     * DB2 连接信息
     */
    String DRIVER_NAME = "com.ibm.db2.jcc.DB2Driver";
    String DB_URL = "jdbc:db2://tj-maximo.com:50005/maxdb";
    String USERNAME = "maximo";
    String PASSWORD = "maximo123";


    /**
     * ATS-遥测
     */
    String Ua = "Ua";
    String Ub = "Ub";
    String Uc = "Uc";
    String Ia = "Ia";
    String Ib = "Ib";
    String Ic = "Ic";


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

}
