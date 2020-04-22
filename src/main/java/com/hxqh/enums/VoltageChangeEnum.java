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
public enum VoltageChangeEnum implements Change {

    Increased("超过额定电压"),

    Decreased("低于额定电压"),

    Roughly_flat("与额定电压持平");

    private String code;

}
