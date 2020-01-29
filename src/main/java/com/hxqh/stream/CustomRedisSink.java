package com.hxqh.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Created by Ocean lin on 2020/1/20.
 *
 * @author Ocean lin
 */
public class CustomRedisSink {


    public static void main(String[] args) {
        //获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("tj-hospital.com", 9001, "\n");

        // 设置时间类型,默认ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String, String>> redisKey = dataStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>("RedisKey", s);
            }
        });

        FlinkJedisPoolConfig build = new FlinkJedisPoolConfig.Builder().setHost("tj-hospital.com").setPort(6379).build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(build, new CustomRedisMapper());
        redisKey.addSink(redisSink);
        try {
            env.execute("CustomRedisSink");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class CustomRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> tuple2) {
            return tuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> tuple2) {
            return tuple2.f1;
        }
    }
}
