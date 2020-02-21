package com.hxqh.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

/**
 * Created by Ocean lin on 2020/2/21.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@NoArgsConstructor
public class YxAts {

    private String IEDName;

    private Date ColTime;

    private String VariableName;

    private Integer Value;


}
