package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.Yx;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * Created by Ocean lin on 2020/4/20.
 *
 * @author Ocean lin
 */
public class YxRowSchema implements KeyedSerializationSchema<Yx> {
    @Override
    public byte[] serializeKey(Yx element) {
        return element.getIEDName().getBytes();
    }

    @Override
    public byte[] serializeValue(Yx element) {
        return JSON.toJSONString(element).getBytes();
    }

    @Override
    public String getTargetTopic(Yx element) {
        return null;
    }
}
