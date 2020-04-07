package com.hxqh.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/17.
 *
 * @author Ocean lin
 */
public class DateUtils {

    public static Date getFormatDate() {
        Date now = new Date();
        String s = formatDate(now);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 日期转换为时间戳
     *
     * @param timers
     * @return
     */
    public static long timeToStamp(String timers) {
        Date d = new Date();
        long timeStemp = 0;
        try {
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // 日期转换为时间戳
            d = sf.parse(timers);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        timeStemp = d.getTime();
        return timeStemp;
    }


    public static Date formatDate(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String formatDate(Date time) {
        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = format1.format(time);
        return s;
    }

    public static int getQuarter(Timestamp timestamp) {
        int month = timestamp.getMonth() + 1;
        return month % 3 + 1;
    }

}
