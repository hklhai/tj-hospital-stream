package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/20.
 *
 * @author Ocean lin
 */
@Deprecated
@Getter
@Setter
@NoArgsConstructor
public class YxAts implements Serializable {

    private static final long serialVersionUID = -2157864192815826188L;
    private String IEDName;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;

    private String VariableName;

    private Integer Value;

    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

}
