package com.hxqh.utils;

import com.hxqh.domain.YcAts;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.YxAts;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.domain.base.IEDParam;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
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

        for (Map.Entry<String, List<IEDParam>> entry : parameterMap.entrySet()) {
            String attr = entry.getKey().toUpperCase();
            try {
                Method getMethod = ycAts.getClass().getDeclaredMethod("get" + attr);
                Type genericReturnType = getMethod.getGenericReturnType();
                if (genericReturnType.getTypeName().equals(Double.class.getName())) {
                    Method setMethod = ycAts.getClass().getDeclaredMethod("set" + attr, Double.class);
                    setMethod.invoke(ycAts, null == entry.getValue() ? 0.0f : entry.getValue().get(0).getValue());
                }
            } catch (Exception e) {
                e.printStackTrace();
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

        List<IEDParam> iedParam = entity.getIEDParam();
        Map<String, List<IEDParam>> parameterMap = iedParam.stream().collect(Collectors.groupingBy(IEDParam::getVariableName));

        for (Map.Entry<String, List<IEDParam>> entry : parameterMap.entrySet()) {
            String attr = entry.getKey().toUpperCase();
            try {
                Method getMethod = ycMediumVoltage.getClass().getDeclaredMethod("get" + attr);
                Type genericReturnType = getMethod.getGenericReturnType();
                if (genericReturnType.getTypeName().equals(Integer.class.getName())) {
                    Method setMethod = ycMediumVoltage.getClass().getDeclaredMethod("set" + attr, Integer.class);
                    setMethod.invoke(ycMediumVoltage, null == entry.getValue() ? 0 : entry.getValue().get(0).getValue().intValue());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ycMediumVoltage;
    }
}
