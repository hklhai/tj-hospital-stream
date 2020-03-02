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

    private static final long serialVersionUID = 8490661548003948059L;
    private String IEDName;

    private Date ColTime;
    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

    private Double CIRCUITBREAKER;
    private Double POSITIVEREACTIVE;
    private Double POSITIVEACTIVE;
    private Double EARTHKNIFE;
    private Double REVERSEREACTIVE;
    private Double REVERSEACTIVE;
    private Double HANDCARTPOSITION;
    private Double AMBIENTTEMPERATURE;
    private Double CCABLETEMPERATURE;
    private Double BCABLETEMPERATURE;
    private Double ACABLETEMPERATURE;
    private Double CLOWERARMTEMPERATURE;
    private Double BLOWERARMTEMPERATURE;
    private Double ALOWERARMTEMPERATURE;
    private Double CUPPERARMTEMPERATURE;
    private Double BUPPERARMTEMPERATURE;
    private Double AUPPERARMTEMPERATURE;
    private Double APHASECURRENT;
    private Double BPHASECURRENT;
    private Double CPHASECURRENT;
    private Double ABLINEVOLTAGE;
    private Double BCLINEVOLTAGE;
    private Double CALINEVOLTAGE;
    private Double ZEROSEQUENCECURRENT;
    private Double FREQUENCY;
    private Double ACTIVEPOWER;
    private Double REACTIVEPOWER;
    private Double APPARENTPOWER;
    private Double ACTIVEELECTRICDEGREE;
    private Double REACTIVEELECTRICDEGREE;


    private Double LINEVOLTAGE;
    private Double LINECURRENT;
    private Double CAPACITANCEREACTIVEPOWER;
    private Double REACTIVEPOWERSYMBOL;
    private Double CAPACITANCEACTIVEPOWER;
    private Double NO1OPENINGVOLTAGE;
    private Double NO1BCAPACITANCECURRENT;
    private Double NO1CCAPACITANCECURRENT;
    private Double NO2OPENINGVOLTAGE;
    private Double NO2BCAPACITANCECURRENT;
    private Double NO2CCAPACITANCECURRENT;
    private Double NO3OPENINGVOLTAGE;
    private Double NO3BCAPACITANCECURRENT;
    private Double NO3CCAPACITANCECURR;


}
