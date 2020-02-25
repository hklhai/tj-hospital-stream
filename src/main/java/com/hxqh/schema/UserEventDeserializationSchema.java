package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hxqh.domain.UserEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * Created by Ocean lin on 2020/2/24.
 *
 * @author Ocean lin
 */
public class UserEventDeserializationSchema implements KeyedDeserializationSchema {
    @Override
    public Object deserialize(byte[] bytes, byte[] message, String s, int i, long l) throws IOException {
        return JSON.parseObject(new String(message), new TypeReference<UserEvent>() {
        });
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<UserEvent>() {
        });
    }
}
