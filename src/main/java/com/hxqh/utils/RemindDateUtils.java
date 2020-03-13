package com.hxqh.utils;

import com.hxqh.domain.info.DataStartEnd;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class RemindDateUtils {


    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private final static SimpleDateFormat formatMonth = new SimpleDateFormat("yyyy-MM");
    private final static SimpleDateFormat formatYear = new SimpleDateFormat("yyyy");


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


    /**
     * 获取上一月 年月信息
     *
     * @return
     */
    public static String getLastMonth() {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, -1);
        return formatMonth.format(c.getTime());
    }


    /**
     * 获取前季度 年月信息
     *
     * @return
     */
    public static String getLastTwoQuarterString() {
        // ('2020-1','2019-4')
        StringBuilder stringBuilder = new StringBuilder(64);
        stringBuilder.append("(");
        int season = getSeason(new Date()) - 1;
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        if (season == 0) {
            year -= 1;
            season = 4;
        }
        stringBuilder.append("'").append(year).append("-").append(season).append("'").append(",");
        season--;
        if (season == 0) {
            year -= 1;
            season = 4;
        }
        stringBuilder.append("'").append(year).append("-").append(season).append("'");
        return stringBuilder.toString() + ")";
    }


    /**
     * 获取前两年份信息
     *
     * @return
     */
    public static String getLastTwoYearString() {
        // ('2020','2019')
        Calendar c = Calendar.getInstance();
        c.add(Calendar.YEAR, -1);
        String y1 = formatYear.format(c.getTime());
        Calendar c1 = Calendar.getInstance();
        c1.add(Calendar.YEAR, -2);
        String y2 = formatYear.format(c1.getTime());
        StringBuilder stringBuilder = new StringBuilder(64);
        stringBuilder.append("(").append("'").append(y1).append("'").append(",");
        stringBuilder.append("'").append(y2).append("'").append(")");
        return stringBuilder.toString();
    }

    /**
     * 获取上一年份
     *
     * @return
     */
    public static String getLastYear() {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.YEAR, -1);
        return formatYear.format(c.getTime());
    }


    /**
     * 获取上一季度的所有月份字符串
     *
     * @return
     */
    public static String getLastQuarterString() {
        // ('2020-02','2020-01','2020-03')
        StringBuilder stringBuilder = new StringBuilder(64);
        stringBuilder.append("(");
        int season = getSeason(new Date()) - 1;
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        if (season == 0) {
            year -= 1;
            season = 4;
        }

        season *= 3;
        for (int i = 0; i < 3; i++, season--) {
            String format = String.format("%02d", season);
            stringBuilder.append("'").append(year).append("-").append(format).append("'").append(",");
        }

        CharSequence charSequence = stringBuilder.subSequence(0, stringBuilder.length() - 1);
        return charSequence.toString() + ")";
    }


    /**
     * 获取上一季度
     *
     * @return
     */
    public static String getLastQuarter() {
        StringBuilder stringBuilder = new StringBuilder(64);
        int season = getSeason(new Date()) - 1;
        Calendar cal = Calendar.getInstance();
        int year = cal.get(Calendar.YEAR);
        if (season == 0) {
            year -= 1;
            season = 4;
        }
        stringBuilder.append(year).append("-").append(season);
        return stringBuilder.toString();
    }


    public static int getSeason(Date date) {
        int season = 0;

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int month = c.get(Calendar.MONTH);
        switch (month) {
            case Calendar.JANUARY:
            case Calendar.FEBRUARY:
            case Calendar.MARCH:
                season = 1;
                break;
            case Calendar.APRIL:
            case Calendar.MAY:
            case Calendar.JUNE:
                season = 2;
                break;
            case Calendar.JULY:
            case Calendar.AUGUST:
            case Calendar.SEPTEMBER:
                season = 3;
                break;
            case Calendar.OCTOBER:
            case Calendar.NOVEMBER:
            case Calendar.DECEMBER:
                season = 4;
                break;
            default:
                break;
        }
        return season;
    }

}
