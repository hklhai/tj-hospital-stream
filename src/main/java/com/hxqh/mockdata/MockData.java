package com.hxqh.mockdata;

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

}
