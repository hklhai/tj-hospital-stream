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
public enum SecondAlarmLevel implements AlarmLevel {

    // 中压开关柜
    NoEnergyDtorage("NoEnergyDtorage", "未储能告警"),
    CircuitDisconnection("CircuitDisconnection", "回路断线告警"),

    CCableOvertemperature("CCableOvertemperature", "C相电缆头超温报警"),
    BCableOvertemperature("BCableOvertemperature", "B相电缆头超温报警"),
    ACableOvertemperature("ACableOvertemperature", "A相电缆头超温报警"),

    CLowerArmOvertemperature("CLowerArmOvertemperature", "C相下触臂超温报警"),
    BLowerArmOvertemperature("BLowerArmOvertemperature", "B相下触臂超温报警"),
    ALowerArmOvertemperature("ALowerArmOvertemperature", "A相下触臂超温报警"),

    CUpperArmOvertemperature("CUpperArmOvertemperature", "C相上触臂超温报警"),
    BUpperArmOvertemperature("BUpperArmOvertemperature", "B相上触臂超温报警"),
    AUpperArmOvertemperature("AUpperArmOvertemperature", "A相上触臂超温报警"),

    // 变压器
    WindingOvertemperatureAlarm("WindingOvertemperatureAlarm", "超温报警"),

    // todo 低压设备
    SlightOverCurrent("SlightOverCurrent", "轻微过流"),
    ContactWear90("ContactWear90", "触头磨损90%"),




    // todo 未提供
    NO_INFO("0", "小电流接地选线报警");

    private String code;

    private String message;
}
