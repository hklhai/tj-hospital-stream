package com.hxqh.enums;

import lombok.Getter;


/**
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
@Getter
public enum HBaseStorageModeEnum implements CodeEnum {
    /**
     * STRING
     */
    STRING(0, "STRING"),
    /**
     * NATIVE
     */
    NATIVE(1, "NATIVE"),
    /**
     * PHOENIX
     */
    PHOENIX(2, "PHOENIX");

    private Integer code;

    private String message;

    HBaseStorageModeEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }
}
