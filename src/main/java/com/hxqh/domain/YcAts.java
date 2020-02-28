package com.hxqh.domain;

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

    private Date ColTime;


    private Double UA;
    private Double UB;
    private Double UC;

    private Double IA;
    private Double IB;
    private Double IC;

    private String assetYpe;
    private String productModel;

}
