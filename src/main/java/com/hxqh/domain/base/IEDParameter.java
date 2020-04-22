package com.hxqh.domain.base;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by Ocean lin on 2020/2/13.
 *
 * @author Ocean lin
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IEDParameter implements Serializable {
    private static final long serialVersionUID = 2293274595061130532L;
    private String VariableName;

    private Integer Value;

}
