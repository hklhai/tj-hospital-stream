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
public class YxMediumVoltage implements Serializable {


    private static final long serialVersionUID = -804354626639724691L;
    private String IEDName;

    private Date ColTime;
    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

    private Integer QUICKBREAK;
    private Integer OVERCURRENT;
    private Integer OVERCURRENTDELAY;
    private Integer NOENERGYDTORAGE;
    private Integer CIRCUITDISCONNECTION;
    private Integer CCABLEOVERTEMPERATURE;
    private Integer BCABLEOVERTEMPERATURE;
    private Integer ACABLEOVERTEMPERATURE;
    private Integer CLOWERARMOVERTEMPERATURE;
    private Integer BLOWERARMOVERTEMPERATURE;
    private Integer ALOWERARMOVERTEMPERATURE;
    private Integer CUPPERARMOVERTEMPERATURE;
    private Integer BUPPERARMOVERTEMPERATURE;
    private Integer AUPPERARMOVERTEMPERATURE;
    private Integer NO1CAPACITORPOSITION;
    private Integer NO1CAPACITORBLOCKING;
    private Integer NO1CAPACITOROVERCURRENT;
    private Integer NO1CAPACITORQUICKBREAK;
    private Integer NO1CAPACITOROPENINGPROTECTION;
    private Integer NO2CAPACITORPOSITION;
    private Integer NO2CAPACITORBLOCKING;
    private Integer NO2CAPACITOROVERCURRENT;
    private Integer NO2CAPACITORQUICKBREAK;
    private Integer NO2CAPACITOROPENINGPROTECTION;
    private Integer NO3CAPACITORPOSITION;
    private Integer NO3CAPACITORBLOCKING;
    private Integer NO3CAPACITOROVERCURRENT;
    private Integer NO3CAPACITORQUICKBREAK;
    private Integer NO3CAPACITOROPENINGPROTECTION;


}
