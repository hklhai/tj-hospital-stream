package com.hxqh.domain;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Ocean lin on 2020/2/25.
 *
 * @author Ocean lin
 */
@Data
@ToString
public class EvaluatedResult implements Serializable {

    private static final long serialVersionUID = 2000889797520391400L;
    private String userId;
    private String channel;
    private Integer purchasePathLength;
    private Map<String, Integer> eventTypeCounts;

}
