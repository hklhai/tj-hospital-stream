package com.hxqh.transfer;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.IEDEntity;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.JsonUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by Ocean lin on 2020/2/17.
 *
 * @author Ocean lin
 */
public class ProcessWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long l) {
        if (lastElement != null && !"".equals(lastElement) && JsonUtils.isjson(lastElement)) {
            IEDEntity iedEntity = JSON.parseObject(lastElement, IEDEntity.class);

            if (iedEntity.getColTime() == null) {
                long triggerExecTime = DateUtils.timeToStamp(DateUtils.formatDate(iedEntity.getIEDParam().get(0).getColTime()));
                return new Watermark(triggerExecTime);
            } else {
                long triggerExecTime = DateUtils.timeToStamp(DateUtils.formatDate(iedEntity.getColTime()));
                return new Watermark(triggerExecTime);
            }
        }
        return null;
    }

    @Override
    public long extractTimestamp(String element, long l) {

        if (element != null && !"".equals(element) && JsonUtils.isjson(element)) {
            IEDEntity iedEntity = JSON.parseObject(element, IEDEntity.class);
            if (iedEntity.getColTime() == null) {
                long triggerExecTime = DateUtils.timeToStamp(DateUtils.formatDate(iedEntity.getIEDParam().get(0).getColTime()));
                return triggerExecTime;
            } else {
                long triggerExecTime = DateUtils.timeToStamp(DateUtils.formatDate(iedEntity.getColTime()));
                return triggerExecTime;
            }
        }
        return 0L;
    }
}
