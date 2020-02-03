package com.hxqh.stream.state.operate;

import com.hxqh.mockdata.MockData;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 并行度 1 ：
 * 1> (5,3 4 5 6 7 )
 * 2> (6,4 5 3 9 9 2 )
 * 4> (4,2 3 4 5 )
 * <p>
 * 并行度 3 ：
 * 2> (5,4 3 6 4 9 )
 * 1> (1,5 )
 * 4> (1,3 )
 * <p>
 * Created by Ocean lin on 2020/2/3.
 *
 * @author Ocean lin
 */
public class OperateState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);


        DataStream<Long> inputStream = env.fromElements(MockData.longData);
        DataStream<Tuple2<Integer, String>> operator = inputStream.flatMap(new CountWithOperatorState()).setParallelism(1);
        operator.print();

        env.execute("OperateState");
    }

    private static class CountWithOperatorState extends RichFlatMapFunction<Long, Tuple2<Integer, String>> implements CheckpointedFunction {

        /**
         * 托管状态
         */
        private transient ListState<Long> checkPointedCountList;

        /**
         * 原始状态
         */
        private List<Long> bufferElements;

        /**
         * 对原始状态做初始化
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            bufferElements = new ArrayList<>();

        }

        @Override
        public void flatMap(Long val, Collector<Tuple2<Integer, String>> collector) throws Exception {
            if (1 == val) {
                if (bufferElements.size() > 0) {
                    StringBuffer buffer = new StringBuffer();
                    for (Long item : bufferElements) {
                        buffer.append(item + " ");
                    }
                    collector.collect(Tuple2.of(bufferElements.size(), buffer.toString()));
                    bufferElements.clear();
                }
            } else {
                bufferElements.add(val);
            }
        }

        /**
         * checkpoint时数据快照
         *
         * @param functionSnapshotContext
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkPointedCountList.clear();
            for (Long item : bufferElements) {
                checkPointedCountList.add(item);
            }
        }

        /**
         * 重启恢复数据
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<Long>("checkPointedCountList", TypeInformation.of(new TypeHint<Long>() {
            }));
            checkPointedCountList = context.getOperatorStateStore().getListState(listStateDescriptor);

            if (context.isRestored()) {
                for (Long val : checkPointedCountList.get()) {
                    bufferElements.add(val);
                }
            }
        }
    }
}
