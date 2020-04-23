package com.hxqh.task.alarm.acb;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.ThirdAlarmLevel;
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
public class AcbThirdAlarm implements FlatMapFunction<Row, Yx> {
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure acb = ConvertUtils.rowConvert2YcLowPressure(row);

        String productModelB = acb.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (acb.getPhaseL1CurrentPercent() + acb.getPhaseL2CurrentPercent() + acb.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        // 1、	100%>=运行电流≥额定运行电流90%
        if (avgCurrent >= ratedCurrent * ContactWear90 && avgCurrent <= ratedCurrent) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), ThirdAlarmLevel.UnderCurrent.getCode());
            out.collect(yx);
        }

        // todo 2、 	运行时间达到1年11个月（距离维保时间1个月）

        Double avgVoltage = (acb.getPhaseL1L2Voltage() + acb.getPhaseL2L3Voltage() + acb.getPhaseL3L1Voltage()) * 1.0 / 3;
        // 3、	运行电压超过额定工作电压100%，且小于110%
        if (avgVoltage < Rated_Voltage * OverVoltageRatio_UP && avgVoltage > Rated_Voltage) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), ThirdAlarmLevel.SlightOverVoltage.getCode());
            out.collect(yx);
        }

        // 4、	运行电压低于额定工作电压90%，大于等于85%
        if (avgVoltage >= Rated_Voltage * OverVoltageRatio_DOWN && avgVoltage < Rated_Voltage * ContactWear90) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), ThirdAlarmLevel.SlightUnderVoltage.getCode());
            out.collect(yx);
        }
    }
}
