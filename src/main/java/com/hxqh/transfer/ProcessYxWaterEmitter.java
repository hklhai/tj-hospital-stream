package com.hxqh.transfer;

import com.hxqh.utils.DateUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Created by Ocean lin on 2020/3/6.
 *
 * @author Ocean lin
 */

public class ProcessYxWaterEmitter implements AssignerWithPunctuatedWatermarks<Row> {
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
