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
public enum ThirdAlarmLevel implements AlarmLevel {

    // todo 中压开关

    // 变压器
    TemperatureControlFailure("TemperatureControlFailure", "温控装置故障"),

    // todo 低压设备


    // todo 未提供
    NO_INFO("0", "温度预警（zenon处提供的三相不平衡计算结果）");

    private String code;

    private String message;
}
