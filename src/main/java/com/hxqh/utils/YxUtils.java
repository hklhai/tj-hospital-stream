package com.hxqh.utils;

import com.hxqh.domain.Yx;
import com.hxqh.domain.base.IEDParameter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.hxqh.constant.Constant.YX;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class YxUtils {

    /**
     * @param IEDName      设备编码
     * @param ColTime      事件时间
     * @param VariableName 报警名称
     * @return
     */
    public static Yx alarm(String IEDName, Date ColTime, String VariableName) {
        Yx yx = new Yx();
        yx.setIEDName(IEDName);
        yx.setColTime(ColTime);
        yx.setCKType(YX);
        IEDParameter iedParam = new IEDParameter(VariableName, 1);
        List<IEDParameter> list = new ArrayList<>();
        list.add(iedParam);
        yx.setIEDParam(list);
        return yx;
    }


    /**
     * @param IEDName      设备编码
     * @param ColTime      事件时间
     * @param VariableName 报警名称
     * @param val          报警状态信号
     * @return
     */
    public static Yx alarm(String IEDName, Date ColTime, String VariableName, Integer val) {
        Yx yx = new Yx();
        yx.setIEDName(IEDName);
        yx.setColTime(ColTime);
        yx.setCKType(YX);
        IEDParameter iedParam = new IEDParameter(VariableName, val);
        List<IEDParameter> list = new ArrayList<>();
        list.add(iedParam);
        yx.setIEDParam(list);
        return yx;
    }


}
