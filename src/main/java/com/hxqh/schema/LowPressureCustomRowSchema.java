package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.AssetType;
import com.hxqh.domain.YcLowPressure;
import com.hxqh.utils.ConvertUtils;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

/**
 * Created by Ocean lin on 2020/04/20.
 *
 * @author Ocean lin
 */
public class LowPressureCustomRowSchema implements KeyedSerializationSchema<Row> {

    @Override
    public byte[] serializeKey(Row element) {
        return element.getField(0).toString().getBytes();
    }

    @Override
    public byte[] serializeValue(Row row) {
        YcLowPressure lowPressure = ConvertUtils.convert2YcLowPressure(row);
        return JSON.toJSONString(lowPressure).getBytes();
    }

    @Override
    public String getTargetTopic(Row element) {
        return null;
    }
}
