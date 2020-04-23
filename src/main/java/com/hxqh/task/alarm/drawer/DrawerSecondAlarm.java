package com.hxqh.task.alarm.drawer;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.SecondAlarmLevel;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.OperationNumber100;
import static com.hxqh.constant.Constant.OverCurrentRatio_UP;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean row
 */
public class DrawerSecondAlarm implements FlatMapFunction<Row, Yx> {
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure drawer = ConvertUtils.rowConvert2YcLowPressure(row);

        // 1、运行电流超过额定运行电流100%但小于105%
        String productModelB = drawer.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (drawer.getPhaseL1CurrentPercent() + drawer.getPhaseL2CurrentPercent() + drawer.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        if (avgCurrent < ratedCurrent * OverCurrentRatio_UP && avgCurrent > ratedCurrent) {
            Yx yx = YxUtils.alarm(drawer.getIEDName(), drawer.getColTime(), SecondAlarmLevel.SlightOverCurrent.getCode());
            out.collect(yx);
        }

        // 2、抽屉插拔次数超过100次
        Integer operationNumber = drawer.getOperationNumber();
        if (operationNumber > OperationNumber100) {
            Yx yx = YxUtils.alarm(drawer.getIEDName(), drawer.getColTime(), SecondAlarmLevel.OperationNumber100.getCode());
            out.collect(yx);
        }

        // todo 3、运行时间超过2年未进行维保
    }
}
