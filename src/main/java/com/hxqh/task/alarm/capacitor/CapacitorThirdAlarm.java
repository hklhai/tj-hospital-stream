package com.hxqh.task.alarm.capacitor;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.ThirdAlarmLevel;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.ContactWear90;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
@SuppressWarnings("DuplicatedCode")
public class CapacitorThirdAlarm implements FlatMapFunction<Row, Yx> {
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure capacitor = ConvertUtils.rowConvert2YcLowPressure(row);

        String productModelB = capacitor.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (capacitor.getPhaseL1CurrentPercent() + capacitor.getPhaseL2CurrentPercent() + capacitor.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        // 1、	100%>=运行电流≥额定运行电流90%
        if (avgCurrent >= ratedCurrent * ContactWear90 && avgCurrent <= ratedCurrent) {
            Yx yx = YxUtils.alarm(capacitor.getIEDName(), capacitor.getColTime(), ThirdAlarmLevel.UnderCurrent.getCode());
            out.collect(yx);
        }

        // todo 2、运行时间达到1年11个月（距离维保时间1个月）


    }
}
