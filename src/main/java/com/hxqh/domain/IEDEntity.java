package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.List;

/**
 * Created by Ocean lin on 2020/2/13.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@NoArgsConstructor
public class IEDEntity {

    private String IEDName;

    private String IEDType;

    private String CKType;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;

    private List<com.hxqh.domain.IEDParam> IEDParam;

}
