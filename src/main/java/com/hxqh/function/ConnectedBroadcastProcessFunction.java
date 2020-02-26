package com.hxqh.function;

import com.hxqh.domain.*;
import com.hxqh.task.JoinTask;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by Ocean lin on 2020/2/25.
 *
 * @author Ocean lin
 */
public class ConnectedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {
    /**
     * 默认配置
     */
    private Config defaultConfig = new Config("APP", "2018-01-01", 0, 3);


    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc = new MapStateDescriptor<>(
            "userEventContainerState",
            BasicTypeInfo.STRING_TYPE_INFO,
            new MapTypeInfo<>(String.class, UserEventContainer.class));


    @Override
    public void processElement(UserEvent userEvent, ReadOnlyContext readOnlyContext, Collector<EvaluatedResult> out) throws Exception {
        //获取用户id
        String userId = userEvent.getUserId();
        //获取渠道
        String channel = userEvent.getChannel();

        //获取事件类型
        EventType eventType = EventType.valueOf(userEvent.getEventType());

        ReadOnlyBroadcastState<String, Config> broadcastState = readOnlyContext.getBroadcastState(JoinTask.configStateDescriptor);
        Config config = broadcastState.get(channel);
        if (Objects.isNull(config)) {
            config = defaultConfig;
        }

        final MapState<String, Map<String, UserEventContainer>> userEventContainerState = getRuntimeContext().getMapState(userMapStateDesc);
        Map<String, UserEventContainer> userEventContainerMap = userEventContainerState.get(channel);
        if (Objects.isNull(userEventContainerMap)) {
            userEventContainerMap = Maps.newHashMap();
            userEventContainerState.put(channel, userEventContainerMap);
        }


        if (!userEventContainerMap.containsKey(userId)) {
            UserEventContainer eventContainer = new UserEventContainer();
            eventContainer.setUserId(userId);
            userEventContainerMap.put(userId, eventContainer);
        }

        userEventContainerMap.get(userId).getUserEvents().add(userEvent);


        if (eventType == EventType.PURCHASE) {
            //计算用户购买路径
            Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));

            result.ifPresent(r -> out.collect(result.get()));
            // 移除
            userEventContainerState.get(channel).remove(userId);
        }

    }

    @Override
    public void processBroadcastElement(Config config, Context context, Collector<EvaluatedResult> collector) throws Exception {
        String channel = config.getChannel();
        BroadcastState<String, Config> broadcastState = context.getBroadcastState(JoinTask.configStateDescriptor);
        broadcastState.put(channel, config);
    }


    /**
     * 计算购买路径长度
     */
    private Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
        Optional<EvaluatedResult> result = Optional.empty();

        String channel = config.getChannel();
        Integer historyPurchaseTimes = config.getHistoryPurchaseTimes();
        Integer maxPurchasePathLength = config.getMaxPurchasePathLength();

        // 当前购买路径
        Integer purchasePath = container.getUserEvents().size();

        if (purchasePath > maxPurchasePathLength && historyPurchaseTimes < 10) {
            // 根据事件时间排序
            container.getUserEvents().sort(Comparator.comparing(UserEvent::getEventTime));

            //定义一个map集合
            final Map<String, Integer> stat = Maps.newHashMap();
            container.getUserEvents().stream().collect(Collectors.groupingBy(UserEvent::getEventType))
                    .forEach((key, events) -> stat.put(key, events.size()));

            final EvaluatedResult evaluatedResult = new EvaluatedResult();
            evaluatedResult.setUserId(container.getUserId());
            evaluatedResult.setChannel(channel);
            evaluatedResult.setEventTypeCounts(stat);
            evaluatedResult.setPurchasePathLength(purchasePath);
            result = Optional.of(evaluatedResult);
        }

        return result;
    }
}
