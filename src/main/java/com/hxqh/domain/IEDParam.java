package com.hxqh.domain;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/13.
 *
 * @author Ocean lin
 */
@Getter
@Setter
@NoArgsConstructor
public class IEDParam {

    private String VariableName;

    private String Unit;

    private String Comments;

    private Double Value;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;
}
