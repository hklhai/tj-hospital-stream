package com.hxqh.schema;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.AssetType;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

/**
 * Created by Ocean lin on 2020/2/26.
 *
 * @author Ocean lin
 */
public class CustomRowSchema implements KeyedSerializationSchema<Row> {

    @Override
    public byte[] serializeKey(Row element) {
        return element.getField(0).toString().getBytes();
    }

    @Override
    public byte[] serializeValue(Row element) {
        AssetType assetType = new AssetType();
        assetType.setAssetnum(element.getField(0).toString());
        assetType.setAssetYpe(element.getField(1).toString());
        assetType.setProductModel(element.getField(2).toString());
        return JSON.toJSONString(assetType).getBytes();
    }

    @Override
    public String getTargetTopic(Row element) {
        return null;
    }
}
