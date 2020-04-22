package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/18.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@NoArgsConstructor
public class YcAts implements Serializable {

    private static final long serialVersionUID = -1353078868954635748L;
    private String IEDName;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;


    private Double Ua;
    private Double Ub;
    private Double Uc;

    private Double Ia;
    private Double Ib;
    private Double Ic;

    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

}
