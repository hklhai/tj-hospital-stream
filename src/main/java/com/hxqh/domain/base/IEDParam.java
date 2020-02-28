package com.hxqh.domain.base;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/13.
 *
 * @author Ocean lin
 */
@Getter
@Setter
@NoArgsConstructor
public class IEDParam implements Serializable {

    private static final long serialVersionUID = 2293274595061130532L;
    private String VariableName;

    private String Unit;

    private String Comments;

    private Double Value;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;

    public IEDParam(String variableName, Double value) {
        VariableName = variableName;
        Value = value;
    }
}
