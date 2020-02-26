package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hxqh.domain.Config;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * 配置流反序列化类
 * <p>
 * Created by Ocean lin on 2020/2/25.
 *
 * @author Ocean lin
 */
public class ConfigDeserializationSchema implements KeyedDeserializationSchema<Config> {

    @Override
    public Config deserialize(byte[] bytes, byte[] bytes1, String s, int i, long l) throws IOException {
        return JSON.parseObject(new String(s), new TypeReference<Config>() {
        });
    }

    @Override
    public boolean isEndOfStream(Config config) {
        return false;
    }

    @Override
    public TypeInformation<Config> getProducedType() {
        return TypeInformation.of(new TypeHint<Config>() {
        });
    }
}
