package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hxqh.domain.AssetType;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * Created by Ocean lin on 2020/2/26.
 *
 * @author Ocean lin
 */
public class AssetTypeDeserializationSchema implements KeyedDeserializationSchema<AssetType> {
    @Override
    public AssetType deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return JSON.parseObject(new String(message), new TypeReference<AssetType>() {
        });
    }

    @Override
    public boolean isEndOfStream(AssetType nextElement) {
        return false;
    }

    @Override
    public TypeInformation<AssetType> getProducedType() {
        return TypeInformation.of(new TypeHint<AssetType>() {
        });
    }
}
