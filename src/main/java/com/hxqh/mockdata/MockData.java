package com.hxqh.mockdata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by Ocean lin on 2020/2/2.
 *
 * @author Ocean lin
 */
public class MockData {

    public static final Tuple3[] data = new Tuple3[]{
            Tuple3.of("class1", "hk", 100),
            Tuple3.of("class1", "lee", 78),
            Tuple3.of("class1", "wang", 99),
            Tuple3.of("class2", "zhang", 81),
            Tuple3.of("class2", "zhao", 59),
            Tuple3.of("class2", "liu", 97),
    };

    public static final Long[] longData = new Long[]{1L, 2L, 3L, 4L, 5L, 1L, 3L, 4L, 5L, 6L, 7L, 1L, 4L, 5L, 3L, 9L, 9L, 2L, 1L};

    public static final Tuple2<Long, Long>[] eventData = new Tuple2[]{
            Tuple2.of(1L, 4L),
            Tuple2.of(2L, 3L),
            Tuple2.of(3L, 1L),
            Tuple2.of(1L, 2L),
            Tuple2.of(3L, 2L),
            Tuple2.of(1L, 2L),
            Tuple2.of(2L, 2L),
            Tuple2.of(2L, 9L)};

}
