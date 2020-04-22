package com.hxqh.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by Ocean lin on 2020/3/13.
 *
 * @author Ocean lin
 */

@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum CurrentChangeEnum implements Change {

    Increased("超过设计容量"),

    Decreased("低于设计容量"),

    // （分数相差±5%内）
    Roughly_flat("与设计容量持平");

    private String code;

}
