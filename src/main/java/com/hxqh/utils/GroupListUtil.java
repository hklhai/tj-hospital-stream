package com.hxqh.utils;

import java.util.*;

/**
 * Created by Ocean lin on 2020/2/18.
 *
 * @author Lin
 */
public class GroupListUtil {

    /**
     * 分组函数接口
     *
     * @param <T>
     */
    public interface GroupBy<T> {
        /**
         * 分组函数接口
         *
         * @param obj 待分组对象
         * @return
         */
        T groupby(Object obj);
    }

    public static final <T extends Comparable<T>, D> Map<T, List<D>> group(Collection<D> colls, GroupBy<T> gb) {
        Map<T, List<D>> map = new LinkedHashMap<>();
        if (colls == null || colls.isEmpty()) {

            System.out.println("分组集合不能为空!");
            return map;
        }
        if (gb == null) {
            System.out.println("分组依赖不能为空!");
            return null;
        }
        Iterator<D> iter = colls.iterator();

        while (iter.hasNext()) {
            D d = iter.next();
            T t = gb.groupby(d);
            if (map.containsKey(t)) {
                map.get(t).add(d);
            } else {
                List<D> list = new LinkedList<>();
                list.add(d);
                map.put(t, list);
            }
        }
        return map;
    }

}
