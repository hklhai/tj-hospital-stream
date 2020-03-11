package com.hxqh.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */


@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum ThirdAlarmLevel implements AlarmLevel {

    // todo 未提供
    NO_INFO("0", "温度预警（zenon处提供的三相不平衡计算结果）");

    private String code;

    private String message;
}
