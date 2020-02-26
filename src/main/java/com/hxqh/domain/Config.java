package com.hxqh.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by Ocean lin on 2020/2/25.
 *
 * @author Ocean lin
 */

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Config  implements Serializable {

    private static final long serialVersionUID = 879831918912951074L;

    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;

}
