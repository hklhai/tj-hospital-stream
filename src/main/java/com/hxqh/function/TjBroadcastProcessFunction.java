package com.hxqh.function;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.AssetType;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.task.ProcessTask;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * Created by Ocean lin on 2020/2/28.
 *
 * @author Ocean lin
 */
public class TjBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, String, AssetType, String> {
    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        IEDEntity entity = JSON.parseObject(s, IEDEntity.class);

        ReadOnlyBroadcastState<String, AssetType> broadcastState = readOnlyContext.getBroadcastState(ProcessTask.configStateDescriptor);
        AssetType assetType = broadcastState.get(entity.getIEDName());
        if (!Objects.isNull(assetType)) {
            entity.setAssetYpe(assetType.getAssetYpe());
            entity.setProductModel(assetType.getProductModel());
            collector.collect(JSON.toJSONString(entity));
        }
    }

    @Override
    public void processBroadcastElement(AssetType assetType, Context context, Collector<String> collector) throws Exception {
        String assetnum = assetType.getAssetnum();
        BroadcastState<String, AssetType> broadcastState = context.getBroadcastState(ProcessTask.configStateDescriptor);
        broadcastState.put(assetnum, assetType);
    }
}
