package com.hxqh.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by Ocean lin on 2020/3/30.
 *
 * @author Ocean lin
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public enum PercentageRreactive  implements Change {



    Concerned("关注", "关注"),

    Reasonable("合理", "合理");
    // 无功电度量占比超过10%需关注

    private String code;

    private String message;

}
