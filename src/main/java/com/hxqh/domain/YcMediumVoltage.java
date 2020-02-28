package com.hxqh.domain;

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

    private String IEDName;

    private Date ColTime;
    private String assetYpe;
    private String productModel;

    private Integer CIRCUITBREAKER;
    private Integer POSITIVEREACTIVE;
    private Integer POSITIVEACTIVE;
    private Integer EARTHKNIFE;
    private Integer REVERSEREACTIVE;
    private Integer REVERSEACTIVE;
    private Integer HANDCARTPOSITION;
    private Integer AMBIENTTEMPERATURE;
    private Integer CCABLETEMPERATURE;
    private Integer BCABLETEMPERATURE;
    private Integer ACABLETEMPERATURE;
    private Integer CLOWERARMTEMPERATURE;
    private Integer BLOWERARMTEMPERATURE;
    private Integer ALOWERARMTEMPERATURE;
    private Integer CUPPERARMTEMPERATURE;
    private Integer BUPPERARMTEMPERATURE;
    private Integer AUPPERARMTEMPERATURE;
    private Integer APHASECURRENT;
    private Integer BPHASECURRENT;
    private Integer CPHASECURRENT;
    private Integer ABLINEVOLTAGE;
    private Integer BCLINEVOLTAGE;
    private Integer CALINEVOLTAGE;
    private Integer ZEROSEQUENCECURRENT;
    private Integer FREQUENCY;
    private Integer ACTIVEPOWER;
    private Integer REACTIVEPOWER;
    private Integer APPARENTPOWER;
    private Integer ACTIVEELECTRICDEGREE;
    private Integer REACTIVEELECTRICDEGREE;


}
