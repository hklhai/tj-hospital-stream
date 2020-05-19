package com.hxqh.task.function;

import com.hxqh.domain.Yx;
import com.hxqh.enums.FirstAlarmLevel;
import com.hxqh.enums.OtherAlarmLevel;
import com.hxqh.utils.DateUtils;
import com.hxqh.utils.YxUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Ocean lin on 2020/5/18.
 *
 * @author Ocean lin
 */
public class ComputeLifeBitState extends RichFlatMapFunction<Tuple2<String, Row>, Yx> implements CheckpointedFunction {

    /**
     * 托管状态
     */
    private transient ListState<Row> checkPointedCountList;

    /**
     * 原始状态
     */
    private ConcurrentHashMap<String, Row> rowConcurrentHashMap;

    /**
     * 对原始状态做初始化
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        rowConcurrentHashMap = new ConcurrentHashMap<>(3000);
    }


    @Override
    public void flatMap(Tuple2<String, Row> value, Collector<Yx> out) throws Exception {
        String iedName = value.f1.getField(0).toString();
        String variableName = ((Row[]) value.f1.getField(11))[0].getField(0).toString();
        Integer val = Integer.parseInt(((Row[]) value.f1.getField(11))[0].getField(1).toString());
        Timestamp colTime = new Timestamp(DateUtils.formatDate(value.f1.getField(2).toString()).getTime());

        rowConcurrentHashMap.put(iedName + variableName, value.f1);

        if (OtherAlarmLevel.LifeBit1.getCode().equals(variableName)) {
            if (rowConcurrentHashMap.containsKey(iedName + OtherAlarmLevel.LifeBit2.getCode())) {
                Row row = rowConcurrentHashMap.get(iedName + OtherAlarmLevel.LifeBit2.getCode());
                Integer stateVal = Integer.parseInt(((Row[]) row.getField(11))[0].getField(1).toString());
                Integer v = val | stateVal;
                Yx yx = YxUtils.alarm(iedName, colTime, FirstAlarmLevel.LifeBit.getCode(), v);
                out.collect(yx);
            }
        } else {
            if (rowConcurrentHashMap.containsKey(iedName + OtherAlarmLevel.LifeBit1.getCode())) {
                Row row = rowConcurrentHashMap.get(iedName + OtherAlarmLevel.LifeBit1.getCode());
                Integer stateVal = Integer.parseInt(((Row[]) row.getField(11))[0].getField(1).toString());
                Integer v = val | stateVal;

                Yx yx = YxUtils.alarm(iedName, colTime, FirstAlarmLevel.LifeBit.getCode(), v);
                out.collect(yx);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointedCountList.clear();
        for (Map.Entry<String, Row> entry : rowConcurrentHashMap.entrySet()) {
            checkPointedCountList.add(entry.getValue());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Row> listStateDescriptor = new ListStateDescriptor<>("checkPointedCountList", TypeInformation.of(new TypeHint<Row>() {
        }));
        checkPointedCountList = context.getOperatorStateStore().getListState(listStateDescriptor);

        if (context.isRestored()) {
            for (Row row : checkPointedCountList.get()) {
                String iedName = row.getField(0).toString();
                Integer val = Integer.parseInt(((Row[]) row.getField(11))[0].getField(1).toString());
                rowConcurrentHashMap.put(iedName + val, row);
            }
        }
    }
}
