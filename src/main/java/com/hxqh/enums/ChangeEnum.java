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
public enum ChangeEnum implements Change {

    Increased("有所提升"),

    Decreased("有所下降"),

    // （分数相差±5%内）
    Roughly_flat("大致持平");

    private String code;

}
