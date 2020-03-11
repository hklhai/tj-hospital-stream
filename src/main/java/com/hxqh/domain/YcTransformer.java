package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */


@Getter
@Setter
@NoArgsConstructor
public class YcTransformer implements Serializable {

    private static final long serialVersionUID = 2567488671780842637L;
    private String IEDName;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;
    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

    private String productModelB;
    private String productModelC;
    private Double fractionRatio;
    private Double loadRate;

    private Double APhaseTemperature;
    private Double BPhaseTemperature;
    private Double CPhaseTemperature;
    private Double DRoadTemperature;

}
