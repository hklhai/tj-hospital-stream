package com.hxqh.utils;

import com.hxqh.domain.info.DataStartEnd;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */
public class RemindDateUtils {


    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");


    public static DataStartEnd getCurrentMonthStartTime() {

        //获取当前月第一天：
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, -1);
        c.set(Calendar.DAY_OF_MONTH, 1);
        String first = format.format(c.getTime());

        //获取当前月最后一天
        Calendar ca = Calendar.getInstance();
        ca.add(Calendar.MONTH, -1);
        ca.set(Calendar.DAY_OF_MONTH, ca.getActualMaximum(Calendar.DAY_OF_MONTH));
        String last = format.format(ca.getTime());

        return new DataStartEnd(first + " 00:00:00", last + " 23:59:59");
    }


}
