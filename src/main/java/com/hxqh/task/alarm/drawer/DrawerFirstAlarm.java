package com.hxqh.task.alarm.drawer;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.enums.FirstAlarmLevel;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.hxqh.constant.Constant.*;
import static com.hxqh.constant.Constant.OverVoltageRatio_UP;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class DrawerFirstAlarm implements FlatMapFunction<Row, Yx> {

    /**
     * @param row
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure drawer = ConvertUtils.rowConvert2YcLowPressure(row);

        // 1、运行电流超过额定电流105%
        String productModelB = drawer.getProductModelB();
        Integer ratedCurrent = Integer.parseInt(productModelB.replace("A", ""));
        Double avgCurrent = (drawer.getPhaseL1CurrentPercent() + drawer.getPhaseL2CurrentPercent() + drawer.getPhaseL3CurrentPercent()) * ratedCurrent * 1.0 / 3;
        if (avgCurrent >= ratedCurrent * OverCurrentRatio_UP) {
            Yx yx = YxUtils.alarm(drawer.getIEDName(), drawer.getColTime(), FirstAlarmLevel.OverCurrent.getCode());
            out.collect(yx);
        }


        // 2、抽屉插拔次数超过500次
        Integer operationNumber = drawer.getOperationNumber();
        if (operationNumber > OperationNumber500) {
            Yx yx = YxUtils.alarm(drawer.getIEDName(), drawer.getColTime(), FirstAlarmLevel.OperationNumber500.getCode());
            out.collect(yx);
        }

        // todo 3、运行时间超过4年未进行维保


        // 4、运行电压超过690V（大于等于）
        Double avgVoltage = (drawer.getPhaseL1L2Voltage() + drawer.getPhaseL2L3Voltage() + drawer.getPhaseL3L1Voltage()) * 1.0 / 3;
        if (avgVoltage >= Run_Voltage690) {
            Yx yx = YxUtils.alarm(drawer.getIEDName(), drawer.getColTime(), FirstAlarmLevel.RunVoltage690.getCode());
            out.collect(yx);
        }

    }
}
