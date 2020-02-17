package com.hxqh.utils;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by Ocean lin on 2020/2/17.
 *
 * @author Ocean lin
 */
public class JsonUtils {

    public static boolean isjson(String string) {
        try {
            JSONObject jsonStr = JSONObject.parseObject(string);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
