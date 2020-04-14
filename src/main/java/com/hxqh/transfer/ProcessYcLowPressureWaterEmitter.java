package com.hxqh.transfer;

import com.hxqh.utils.DateUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/**
 * Created by Ocean lin on 2020/4/14.
 *
 * @author Ocean lin
 */

public class ProcessYcLowPressureWaterEmitter implements AssignerWithPunctuatedWatermarks<Row> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Row row, long l) {
        long time = DateUtils.formatDate(row.getField(2).toString()).getTime();
        return new Watermark(time);
    }

    @Override
    public long extractTimestamp(Row row, long l) {
        long time = DateUtils.formatDate(row.getField(2).toString()).getTime();
        return time;
    }
}
