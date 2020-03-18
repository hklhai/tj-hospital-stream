package com.hxqh.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by Ocean lin on 2020/3/17.
 *
 * @author Ocean lin
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum HealthLevel implements AlarmLevel {

    excellent("优秀", "优秀"),
    good("良好", "良好"),
    range("极差", "极差");

    private String code;

    private String message;

}
