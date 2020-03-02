package com.hxqh.utils;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcAts;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.YxAts;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.domain.base.IEDParam;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Created by Ocean lin on 2020/2/18.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ConvertUtils {

    public static YcAts convert2YcAts(IEDEntity entity) {
        YcAts ycAts = new YcAts();
        ycAts.setIEDName(entity.getIEDName());
        ycAts.setColTime(entity.getColTime());
        ycAts.setAssetYpe(entity.getAssetYpe());
        ycAts.setProductModel(entity.getProductModel().trim());

        List<IEDParam> iedParam = entity.getIEDParam();
        Map<String, List<IEDParam>> parameterMap = iedParam.stream().collect(Collectors.groupingBy(IEDParam::getVariableName));

        Field[] declaredFields = ycAts.getClass().getDeclaredFields();

        for (Field field : declaredFields) {
            String attr = StringUtils.capitalize(field.getName());
            if (!"SerialVersionUID".equals(attr)) {
                try {
                    Method getMethod = ycAts.getClass().getDeclaredMethod("get" + attr);
                    Type genericReturnType = getMethod.getGenericReturnType();
                    if (genericReturnType.getTypeName().equals(Double.class.getName())) {
                        Method setMethod = ycAts.getClass().getDeclaredMethod("set" + attr, Double.class);
                        setMethod.invoke(ycAts, null == parameterMap.get(attr) ? 0 : parameterMap.get(attr).get(0).getValue());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return ycAts;
    }

    public static YxAts convert2YxAts(IEDEntity entity) {
        YxAts yxAts = new YxAts();
        yxAts.setIEDName(entity.getIEDName());
        yxAts.setColTime(entity.getColTime());
        yxAts.setAssetYpe(entity.getAssetYpe());
        yxAts.setProductModel(entity.getProductModel().trim());
        yxAts.setVariableName(entity.getIEDParam().get(0).getVariableName());
        yxAts.setValue(entity.getIEDParam().get(0).getValue().intValue());
        return yxAts;
    }


    public static YcMediumVoltage convert2YcMediumVoltage(IEDEntity entity) {
        YcMediumVoltage ycMediumVoltage = new YcMediumVoltage();
        ycMediumVoltage.setIEDName(entity.getIEDName());
        ycMediumVoltage.setColTime(entity.getColTime());
        ycMediumVoltage.setAssetYpe(entity.getAssetYpe());
        ycMediumVoltage.setProductModel(entity.getProductModel().trim());
        ycMediumVoltage.setLocation(entity.getLocation());
        ycMediumVoltage.setParent(entity.getParent());

        List<IEDParam> iedParam = entity.getIEDParam();
        Map<String, List<IEDParam>> parameterMap = iedParam.stream().collect(Collectors.groupingBy(IEDParam::getVariableName));

        Field[] declaredFields = ycMediumVoltage.getClass().getDeclaredFields();
        for (Field field : declaredFields) {
            String attr = StringUtils.capitalize(field.getName());
            if (!"SerialVersionUID".equals(attr)) {
                try {
                    Method getMethod = ycMediumVoltage.getClass().getDeclaredMethod("get" + attr);
                    Type genericReturnType = getMethod.getGenericReturnType();
                    if (genericReturnType.getTypeName().equals(Double.class.getName())) {
                        Method setMethod = ycMediumVoltage.getClass().getDeclaredMethod("set" + attr, Double.class);
                        setMethod.invoke(ycMediumVoltage, null == parameterMap.get(attr) ? 0 : parameterMap.get(attr).get(0).getValue());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return ycMediumVoltage;
    }


    public static TreeMap<String, Object> objToMap(Object object) throws IllegalAccessException {

        Class clazz = object.getClass();
        TreeMap<String, Object> treeMap = new TreeMap<>();

        while (null != clazz.getSuperclass()) {
            Field[] declaredFields1 = clazz.getDeclaredFields();

            for (Field field : declaredFields1) {
                String name = field.getName();

                // 获取原来的访问控制权限
                boolean accessFlag = field.isAccessible();
                // 修改访问控制权限
                field.setAccessible(true);
                Object value = field.get(object);
                // 恢复访问控制权限
                field.setAccessible(accessFlag);

                if (null != value && StringUtils.isNotBlank(value.toString())) {
                    //如果是List,将List转换为json字符串
                    if (value instanceof List) {
                        value = JSON.toJSONString(value);
                    }
                    treeMap.put(name, value);
                }
            }

            clazz = clazz.getSuperclass();
        }
        return treeMap;
    }
}
