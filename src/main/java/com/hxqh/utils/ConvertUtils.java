package com.hxqh.utils;

import com.hxqh.domain.YcAts;
import com.hxqh.domain.YxAts;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.domain.base.IEDParam;

import java.util.List;
import java.util.Map;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/2/18.
 *
 * @author Ocean lin
 */
public class ConvertUtils {


    public static YcAts convert2YcAts(IEDEntity entity) {
        YcAts ycAts = new YcAts();
        ycAts.setIEDName(entity.getIEDName());
        ycAts.setColTime(entity.getColTime());

        List<IEDParam> iedParam = entity.getIEDParam();

        Map<String, List<IEDParam>> parameterMap = GroupListUtil.group(iedParam, new GroupListUtil.GroupBy<String>() {
            @Override
            public String groupby(Object obj) {
                IEDParam d = (IEDParam) obj;
                return d.getVariableName();
            }
        });
        ycAts.setUA(null == parameterMap.get(Ua) ? 0.0f : parameterMap.get(Ua).get(0).getValue());
        ycAts.setUB(null == parameterMap.get(Ub) ? 0.0f : parameterMap.get(Ub).get(0).getValue());
        ycAts.setUC(null == parameterMap.get(Uc) ? 0.0f : parameterMap.get(Uc).get(0).getValue());
        ycAts.setIA(null == parameterMap.get(Ia) ? 0.0f : parameterMap.get(Ia).get(0).getValue());
        ycAts.setIB(null == parameterMap.get(Ib) ? 0.0f : parameterMap.get(Ib).get(0).getValue());
        ycAts.setIC(null == parameterMap.get(Ic) ? 0.0f : parameterMap.get(Ic).get(0).getValue());
        return ycAts;
    }


    public static YxAts convert2YxAts(IEDEntity entity) {
        YxAts yxAts = new YxAts();
        yxAts.setIEDName(entity.getIEDName());
        yxAts.setColTime(entity.getColTime());
        yxAts.setVariableName(entity.getIEDParam().get(0).getVariableName());
        yxAts.setValue(entity.getIEDParam().get(0).getValue().intValue());
        return yxAts;
    }
}
