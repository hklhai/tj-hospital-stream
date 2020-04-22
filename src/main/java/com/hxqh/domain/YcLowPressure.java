package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/4/14.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@NoArgsConstructor
public class YcLowPressure implements Serializable {

    private static final long serialVersionUID = 6914597448905862243L;
    private String IEDName;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;
    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

    private String productModelB;
    private String productModelC;


    private Double PhaseL1CurrentPercent;
    private Double PhaseL1L2Voltage;
    private Double PhaseL2CurrentPercent;
    private Double PhaseL2L3Voltage;
    private Double PhaseL3CurrentPercent;
    private Double PhaseL3L1Voltage;
    private Double ActiveElectricDegree;
    private Double ReactiveElectricDegree;
    private Double PowerFactor;
    private Integer OperationNumber;

    private Double ContactWear;


}
