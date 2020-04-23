package com.hxqh.task.function;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.AssetType;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.task.ProcessTask;
import com.hxqh.utils.JsonUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

import static com.hxqh.constant.Constant.ERROR;

/**
 * Created by Ocean lin on 2020/2/26.
 *
 * @author Ocean lin
 */
public class ConnectedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, String, AssetType, String> {


    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        if (s != null && !"".equals(s) && JsonUtils.isjson(s)) {
            IEDEntity entity = JSON.parseObject(s, IEDEntity.class);

            ReadOnlyBroadcastState<String, AssetType> broadcastState = readOnlyContext.getBroadcastState(ProcessTask.configStateDescriptor);
            AssetType assetType = broadcastState.get(entity.getIEDName());
            if (!Objects.isNull(assetType)) {
                entity.setAssetYpe(assetType.getAssetYpe());
                entity.setProductModel(assetType.getProductModel());
                entity.setParent(assetType.getParent());
                entity.setLocation(assetType.getLocation());

                entity.setProductModelB(assetType.getProductModelB());
                entity.setProductModelC(assetType.getProductModelC());
                entity.setFractionRatio(assetType.getFractionRatio());
                entity.setLoadRate(assetType.getLoadRate());

                collector.collect(JSON.toJSONString(entity));
            }
        } else {
            collector.collect(ERROR + s);
        }
    }

    @Override
    public void processBroadcastElement(AssetType assetType, Context context, Collector<String> collector) throws Exception {
        String assetnum = assetType.getAssetnum();
        BroadcastState<String, AssetType> broadcastState = context.getBroadcastState(ProcessTask.configStateDescriptor);
        broadcastState.put(assetnum, assetType);
    }
}
