package com.hxqh.task.alarm.drawer;

import com.hxqh.domain.Yx;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Created by Ocean lin on 2020/4/21.
 *
 * @author Ocean lin
 */
public class DrawerFirstAlarm implements FlatMapFunction<Row, Yx> {

    /**
     *
     * @param row
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Row row, Collector<Yx> out) throws Exception {



    }
}
