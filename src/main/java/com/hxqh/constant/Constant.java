package com.hxqh.constant;

import com.hxqh.enums.FirstAlarmLevel;
import com.hxqh.enums.OtherAlarmLevel;
import com.hxqh.enums.SecondAlarmLevel;
import com.hxqh.enums.ThirdAlarmLevel;

import java.util.HashMap;
import java.util.Map;

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



    /**
     * 低压开关柜细分类
     */
    String LOW_VOLTAGE_ATS = "低压开关设备-ATS";
    String LOW_VOLTAGE_CAPACITOR = "低压开关设备-电容器";
    String LOW_VOLTAGE_DRAWER_CABINET = "低压开关设备-抽屉柜";
    String LOW_VOLTAGE_INCOMING_CABINET = "低压开关设备-进线柜";
    String LOW_VOLTAGE_COUPLER_CABINET = "低压开关设备-母联柜";
    String LOW_VOLTAGE_FEEDER_CABINET = "低压开关设备-馈线柜";

    /**
     * 低压开关柜细分类
     */
    String ATS = "ATS";
    String CAPACITOR = "电容器";
    String DRAWER_CABINET = "抽屉柜";
    String INCOMING_CABINET = "进线柜";
    String COUPLER_CABINET = "母联柜";
    String FEEDER_CABINET = "馈线柜";


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
    String HTTP = "http";
    String ES_HOST = "tj-hospital.com";
    Integer ES_PORT = 9200;

    String JDBC_ES_URL= "jdbc:elasticsearch://tj-hospital.com:9300/";

    /**
     * 遥测-中压
     */
    String INDEX_YC_MEDIUMVOLTAGE = "yc_mediumvoltage";
    String TYPE_YC_MEDIUMVOLTAGE = "mediumvoltage";


    /**
     * 遥测-变压器
     */
    String INDEX_YC_TRANSFORMER = "yc_transformer";
    String TYPE_YC_TRANSFORMER = "transformer";

    String YX_ATS_QUICK_BREAK = "速断";
    String YX_ATS_OVER_CURRENT = "过流";
    String YX_ATS_SWITCH_POSITION = "开关位置";

    /**
     * 遥测-AST
     */
    String INDEX_YC_ATS = "yc_ats";
    String TYPE_YC_ATS = "ats";


    /**
     * 遥信
     */
    String INDEX_YX = "yx";
    String TYPE_YX = "yx";



    /**
     * 错误
     */
    String INDEX_ERR = "tjerr";
    String TYPE_Err = "err";


    /**
     *
     */
    String ERROR = "error-";

    String CHARSET = "gbk";

    /**
     * 遥测-低压设备
     */
    String INDEX_YC_LOWPRESSURE = "yc_lowpressure";
    String TYPE_YC_LOWPRESSURE = "lowpressure";


    /**
     * MySQL 连接信息
     */
    String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    String MYSQL_DB_URL = "jdbc:mysql://tj-hospital.com:3306/tj?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true";
    String MYSQL_USERNAME = "tj";
    String MYSQL_PASSWORD = "mko09ijn*";

    Map<String, Integer> ALARM_MAP = new HashMap(12) {
        {
            // 中压设备
            put(FirstAlarmLevel.QuickBreak.getCode(), 1);
            put(FirstAlarmLevel.OverCurrent.getCode(), 1);
            put(FirstAlarmLevel.OverCurrentDelay.getCode(), 1);
            put(FirstAlarmLevel.DifferentialProtection.getCode(), 1);

            put(SecondAlarmLevel.NoEnergyDtorage.getCode(), 2);
            put(SecondAlarmLevel.CircuitDisconnection.getCode(), 2);
            put(SecondAlarmLevel.CCableOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.BCableOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.ACableOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.CLowerArmOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.BLowerArmOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.ALowerArmOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.CUpperArmOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.BUpperArmOvertemperature.getCode(), 2);
            put(SecondAlarmLevel.AUpperArmOvertemperature.getCode(), 2);


            //  变压器
            put(FirstAlarmLevel.WindingOvertemperatureTrip.getCode(), 1);
            put(SecondAlarmLevel.WindingOvertemperatureAlarm.getCode(), 2);
            put(ThirdAlarmLevel.TemperatureControlFailure.getCode(), 3);
            put(FirstAlarmLevel.FanOperationStatus.getCode(), 1);


            // 低压设备-1级
            put(FirstAlarmLevel.InsertionCycles.getCode(), 1);
            put(FirstAlarmLevel.OverVoltage.getCode(), 1);
            put(FirstAlarmLevel.UnderVoltage.getCode(), 1);
            put(FirstAlarmLevel.ContactWear.getCode(), 1);
            put(FirstAlarmLevel.LifeBit.getCode(), 1);
            put(FirstAlarmLevel.GPI1.getCode(), 1);
            put(FirstAlarmLevel.OperationNumber500.getCode(), 1);
            put(FirstAlarmLevel.RunVoltage690.getCode(), 1);
            put(FirstAlarmLevel.NoMaintenanceMore4Years.getCode(), 1);


            // 低压设备-2级
            put(SecondAlarmLevel.SlightOverCurrent.getCode(), 2);
            put(SecondAlarmLevel.ContactWear90.getCode(), 2);
            put(SecondAlarmLevel.OperationNumber100.getCode(), 2);
            put(SecondAlarmLevel.NoMaintenanceMore2Years.getCode(), 2);


            // 低压设备-3级
            put(ThirdAlarmLevel.UnderCurrent.getCode(), 3);
            put(ThirdAlarmLevel.SlightOverVoltage.getCode(), 3);
            put(ThirdAlarmLevel.SlightUnderVoltage.getCode(), 3);
            put(ThirdAlarmLevel.NoMaintenanceNearlyYears.getCode(), 3);


            // 未提供
            put(ThirdAlarmLevel.NO_INFO, 3);
        }
    };


    Map<Integer, Integer> ALARM_SCORE_MAP = new HashMap(12) {
        {
            put(1, 40);
            put(2, 10);
            put(3, 5);
        }
    };


    Map<Integer, Integer> ALARM_SCORE_LOW_MAP = new HashMap(12) {
        {
            put(1, 0);
            put(2, 70);
            put(3, 80);
        }
    };

    Float Proportion = 0.05f;

    Map<Integer, String> HOUR_MAP = new HashMap(12) {
        {
            put(0, "00:00:00|07:59:59");
            put(1, "08:00:00|15:59:59");
            put(2, "16:00:00|23:59:59");
        }
    };

    Map<Integer, String> HOUR_PER6_MAP = new HashMap(12) {
        {
            put(0, "00:00:00|05:59:59");
            put(1, "06:00:00|11:59:59");
            put(2, "12:00:00|17:59:59");
            put(3, "18:00:00|23:59:59");
        }
    };



    Integer DEVICE_RUN = 0;
    Integer DEVICE_STOP = 1;


    Integer SOCRE_ONEHUNDRED = 100;


    Double OverCurrentRatio_UP = 1.05d;

    Double OverVoltageRatio_UP = 1.1d;
    Double OverVoltageRatio_DOWN = 0.85d;


    /**
     *
     */
    Double ContactWear90 = 0.9d;


    Double One = 1.0d;
    /**
     * 低压设备额定电压
     */
    Integer Rated_Voltage = 400;


    Double Ten_Percent = 0.1d;
    Double Fifteen_Percent = 0.15d;

    /**
     * 抽屉插拔次数超过500次
     */
    Integer OperationNumber500 = 500;

    /**
     * 抽屉插拔次数超过100次
     */
    Integer OperationNumber100 = 100;

    /**
     * 运行电压超过690V（大于等于）
     */
    Integer Run_Voltage690 = 690;

}
