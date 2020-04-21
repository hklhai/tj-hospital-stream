package com.hxqh.task.alarm.acb;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.domain.Yx;
import com.hxqh.utils.ConvertUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class AcbThirdAlarm implements FlatMapFunction<Row, Yx> {
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {
        YcLowPressure acb = ConvertUtils.convert2YcLowPressure(row);

    }
}
