package com.hxqh.domain;

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

@Getter
@Setter
@NoArgsConstructor
public class YxAts implements Serializable {

    private static final long serialVersionUID = -2157864192815826188L;
    private String IEDName;

    private Date ColTime;

    private String VariableName;

    private Integer Value;

    private String assetYpe;
    private String productModel;

}
