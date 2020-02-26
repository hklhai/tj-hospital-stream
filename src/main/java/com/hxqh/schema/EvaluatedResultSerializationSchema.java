package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.EvaluatedResult;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * Created by Ocean lin on 2020/2/25.
 *
 * @author Ocean lin
 */
public class EvaluatedResultSerializationSchema implements KeyedSerializationSchema<EvaluatedResult> {
    @Override
    public byte[] serializeKey(EvaluatedResult evaluatedResult) {
        return evaluatedResult.getUserId().getBytes();
    }

    @Override
    public byte[] serializeValue(EvaluatedResult evaluatedResult) {
        return JSON.toJSONString(evaluatedResult).getBytes();
    }

    @Override
    public String getTargetTopic(EvaluatedResult evaluatedResult) {
        return null;
    }
}
