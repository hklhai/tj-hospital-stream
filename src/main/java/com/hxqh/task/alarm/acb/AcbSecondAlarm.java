package com.hxqh.task.alarm.acb;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.FirstAlarmLevel;
import com.hxqh.enums.SecondAlarmLevel;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.ContactWear90;
import static com.hxqh.constant.Constant.OverCurrentRatio_UP;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class AcbSecondAlarm implements FlatMapFunction<Row, Yx> {
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure acb = ConvertUtils.convert2YcLowPressure(row);

        // 1、运行电流小于额定电流105%但大于100%
        String productModelB = acb.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (acb.getPhaseL1CurrentPercent() + acb.getPhaseL2CurrentPercent() + acb.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        if (avgCurrent < ratedCurrent * OverCurrentRatio_UP && avgCurrent > ratedCurrent) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), SecondAlarmLevel.SlightOverCurrent.getCode());
            out.collect(yx);
        }


        // todo 2、运行时间超过2年未进行维保



        // 3、触头磨损90%
        Double contactWear = acb.getContactWear();
        if (contactWear>= ContactWear90) {
            Yx yx = YxUtils.alarm(acb.getIEDName(), acb.getColTime(), SecondAlarmLevel.ContactWear90.getCode());
            out.collect(yx);
        }

        // todo 4、机械老化百分比90%


        // todo 5、电气老化百分比90%

    }
}
