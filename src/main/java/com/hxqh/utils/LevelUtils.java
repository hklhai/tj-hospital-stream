package com.hxqh.utils;

import com.hxqh.enums.HealthLevel;

/**
 * 优秀—绿色：80<分数≤100；
 * 良好—黄色：60<分数≤80；
 * 极差—红色：分数≤60
 * <p>
 * Created by Ocean lin on 2020/3/17.
 *
 * @author Ocean lin
 */
public class LevelUtils {

    private static final Integer Sixty = 60;
    private static final Integer Eighty = 80;
    private static final Integer OneHundred = 100;

    public static String computeLevel(Double score) {

        if (Eighty < score && score <= OneHundred) {
            return HealthLevel.excellent.getCode();
        } else if (Sixty < score && score <= Eighty) {
            return HealthLevel.good.getCode();
        } else {
            return HealthLevel.range.getCode();
        }
    }


}
