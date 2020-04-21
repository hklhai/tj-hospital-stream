package com.hxqh.enums;

import com.hxqh.constant.Constant;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 需确认报警等级并增加至 {@link  Constant#ALARM_MAP}
 * <p>
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum FirstAlarmLevel implements AlarmLevel {

    // 中压开关柜
    QuickBreak("QuickBreak", "速断"),

    OverCurrent("OverCurrent", "过流"),

    OverCurrentDelay("OverCurrentDelay", "延时过流"),

    DifferentialProtection("DifferentialProtection", "差动保护"),

    // 变压器
    WindingOvertemperatureTrip("WindingOvertemperatureTrip", "超温跳闸"),
    // 变压器-风机
    FanOperationStatus("FanOperationStatus", "风机运行报警"),


    // 低压开关柜
    InsertionCycles("InsertionCycles", "插入周期报警"),

    OverVoltage("OverVoltage", "电压过高"),
    UnderVoltage("UnderVoltage", "电压过低"),
    ContactWear("ContactWear", "触头磨损100%"),
    LifeBit("LifeBit", "表计/ACB不在线"),
    GPI1("GPI1", "一次断路器故障脱扣"),




    // todo 未提供
    noinfo("0", "未提供");

    private String code;

    private String message;


}
