package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/27.
 *
 * @author Ocean lin
 */


@Getter
@Setter
@NoArgsConstructor
public class YcMediumVoltage implements Serializable {

    private static final long serialVersionUID = 8490661548003948059L;
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

    private Double CircuitBreaker;
    private Double PositiveReactive;
    private Double PositiveActive;
    private Double EarthKnife;
    private Double ReverseReactive;
    private Double ReverseActive;
    private Double HandcartPosition;
    private Double AmbientTemperature;
    private Double CCableTemperature;
    private Double BCableTemperature;
    private Double ACableTemperature;
    private Double CLowerArmTemperature;
    private Double BLowerArmTemperature;
    private Double ALowerArmTemperature;
    private Double CUpperArmTemperature;
    private Double BUpperArmTemperature;
    private Double AUpperArmTemperature;
    private Double APhaseCurrent;
    private Double BPhaseCurrent;
    private Double CPhaseCurrent;
    private Double ABLineVoltage;
    private Double BCLineVoltage;
    private Double CALineVoltage;
    private Double ZeroSequenceCurrent;
    private Double Frequency;
    private Double ActivePower;
    private Double ReactivePower;
    private Double ApparentPower;
    private Double ActiveElectricDegree;
    private Double ReactiveElectricDegree;


    private Double LineVoltage;
    private Double LineCurrent;
    private Double CapacitanceReactivePower;
    private Double ReactivePowerSymbol;
    private Double CapacitanceActivePower;
    private Double No1OpeningVoltage;
    private Double No1BCapacitanceCurrent;
    private Double No1CCapacitanceCurrent;
    private Double No2OpeningVoltage;
    private Double No2BCapacitanceCurrent;
    private Double No2CCapacitanceCurrent;
    private Double No3OpeningVoltage;
    private Double No3BCapacitanceCurrent;
    private Double No3CCapacitanceCurrent;


}
