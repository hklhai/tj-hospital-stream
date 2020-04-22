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

@Deprecated
@Getter
@Setter
@NoArgsConstructor
public class YxMediumVoltage implements Serializable {


    private static final long serialVersionUID = -804354626639724691L;
    private String IEDName;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;
    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

    private Integer QuickBreak;
    private Integer OverCurrent;
    private Integer OverCurrentDelay;
    private Integer NoEnergyDtorage;
    private Integer CircuitDisconnection;
    private Integer CCableOvertemperature;
    private Integer BCableOvertemperature;
    private Integer ACableOvertemperature;
    private Integer CLowerArmOvertemperature;
    private Integer BLowerArmOvertemperature;
    private Integer ALowerArmOvertemperature;
    private Integer CUpperArmOvertemperature;
    private Integer BUpperArmOvertemperature;
    private Integer AUpperArmOvertemperature;


    private Integer No1CapacitorPosition;
    private Integer No1CapacitorBlocking;
    private Integer No1CapacitorOverCurrent;
    private Integer No1CapacitorQuickBreak;
    private Integer No1CapacitorOpeningProtection;
    private Integer No2CapacitorPosition;
    private Integer No2CapacitorBlocking;
    private Integer No2CapacitorOverCurrent;
    private Integer No2CapacitorQuickBreak;
    private Integer No2CapacitorOpeningProtection;
    private Integer No3CapacitorPosition;
    private Integer No3CapacitorBlocking;
    private Integer No3CapacitorOverCurrent;
    private Integer No3CapacitorQuickBreak;
    private Integer No3CapacitorOpeningProtection;


}
