package com.hxqh.task.alarm.ats;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.SecondAlarmLevel;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.OverCurrentRatio_UP;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean row
 */
@SuppressWarnings("DuplicatedCode")
public class AtsSecondAlarm implements FlatMapFunction<Row, Yx> {
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure ats = ConvertUtils.rowConvert2YcLowPressure(row);

        // 1、运行电流超过额定运行电流100%但小于105%
        String productModelB = ats.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (ats.getPhaseL1CurrentPercent() + ats.getPhaseL2CurrentPercent() + ats.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        if (avgCurrent < ratedCurrent * OverCurrentRatio_UP && avgCurrent > ratedCurrent) {
            Yx yx = YxUtils.alarm(ats.getIEDName(), ats.getColTime(), SecondAlarmLevel.SlightOverCurrent.getCode());
            out.collect(yx);
        }


        // todo 2、运行时间超过2年未进行维保

    }
}
