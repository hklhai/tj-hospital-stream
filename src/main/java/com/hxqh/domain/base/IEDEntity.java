package com.hxqh.domain.base;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
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
public class IEDEntity implements Serializable {

    private static final long serialVersionUID = 6067414668077127586L;

    @JSONField(name = "IEDName")
    private String IEDName;

//    private String IEDType;

    @JSONField(name = "CKType")
    private String CKType;

    @JSONField(name = "ColTime", format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;

    @JSONField(name = "IEDParam")
    private List<com.hxqh.domain.base.IEDParam> IEDParam;

    private String assetYpe;

    private String productModel;

    private String parent;

    private String location;

    private String productModelB;
    private String productModelC;
    private Double fractionRatio;
    private Double loadRate;


}
