package com.hxqh.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by Ocean lin on 2020/3/6.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Boundary {

    private int min;
    private int max;

    public static Boundary of(int min, int max) {
        return new Boundary(min, max);
    }
}
