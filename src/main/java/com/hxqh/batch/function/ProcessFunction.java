package com.hxqh.batch.function;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.hxqh.batch.incrementsync.IncrementSyncTask;
import com.hxqh.domain.Flow;
import com.hxqh.enums.FlowStatusEnum;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */
public class ProcessFunction extends KeyedBroadcastProcessFunction<String, FlatMessage, Flow, Tuple2<FlatMessage, Flow>> {

    /**
     * 默认配置
     */
    private Flow defaultFlow = new Flow(1, 0, "canal",
            "orders", "hk_flink:orders", "0", true, 10, "orderId", 2);

    @Override
    public void processElement(FlatMessage value, ReadOnlyContext ctx, Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {

        //获取配置流
        ReadOnlyBroadcastState<String, Flow> broadcastState = ctx.getBroadcastState(IncrementSyncTask.flowBroadcastState);
        Flow flow = broadcastState.get(value.getDatabase() + value.getTable());
        if (Objects.isNull(flow)) {
            flow = defaultFlow;
        }
        if (flow.getStatus() == FlowStatusEnum.FLOWSTATUS_RUNNING.getCode()) {
            out.collect(Tuple2.of(value, flow));
        }
    }

    @Override
    public void processBroadcastElement(Flow flow, Context ctx, Collector<Tuple2<FlatMessage, Flow>> out) throws Exception {
        //获取state 状态
        BroadcastState<String, Flow> broadcastState = ctx.getBroadcastState(IncrementSyncTask.flowBroadcastState);

        //更新state
        broadcastState.put(flow.getDatabaseName() + flow.getTableName(), flow);
    }
}
