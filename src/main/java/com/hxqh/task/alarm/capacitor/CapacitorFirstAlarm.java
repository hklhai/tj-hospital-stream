package com.hxqh.task.alarm.capacitor;

import com.hxqh.domain.Yx;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class CapacitorFirstAlarm implements FlatMapFunction<Row, Yx> {

    /**
     * 1、	一次断路器故障脱扣（GPI DI1）
     * 2、	运行时间超过4年未进行维保
     * 3、	表计不在线
     *
     * @param row
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {



    }
}
