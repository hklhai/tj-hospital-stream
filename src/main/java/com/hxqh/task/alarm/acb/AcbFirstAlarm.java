package com.hxqh.task.alarm.acb;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.FirstAlarmLevel;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class AcbFirstAlarm implements FlatMapFunction<Row, Yx> {


    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure acb = ConvertUtils.rowConvert2YcLowPressure(row);

        // 1、 运行电流超过额定电流105%
        String productModelB = acb.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (acb.getPhaseL1CurrentPercent() + acb.getPhaseL2CurrentPercent() + acb.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        if (avgCurrent >= ratedCurrent * OverCurrentRatio_UP) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), FirstAlarmLevel.OverCurrent.getCode());
            out.collect(yx);
        }

        // todo 2、 运行时间超过4年未进行维保


        Double avgVoltage = (acb.getPhaseL1L2Voltage() + acb.getPhaseL2L3Voltage() + acb.getPhaseL3L1Voltage()) * 1.0 / 3;
        // 3、 超过额定工作电压110%（大于等于）
        if (avgVoltage >= Rated_Voltage * OverVoltageRatio_UP) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), FirstAlarmLevel.OverVoltage.getCode());
            out.collect(yx);
        }

        // 4、 低于额定工作电压85%
        if (avgVoltage < Rated_Voltage * OverVoltageRatio_DOWN) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), FirstAlarmLevel.UnderVoltage.getCode());
            out.collect(yx);
        }


        // 7、  触头磨损100%
        Double contactWear = acb.getContactWear();
        if (One.equals(contactWear)) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), FirstAlarmLevel.ContactWear.getCode());
            out.collect(yx);
        }
    }
}
