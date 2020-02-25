package com.hxqh.domain;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;


/**
 *  用户事件
 *
 * Created by Ocean lin on 2020/2/24.
 *
 * @author Ocean lin
 */
@Data
@ToString
public class UserEvent implements Serializable {

    private static final long serialVersionUID = -5484095591279723900L;
    private String userId;
    private String channel;
    private String eventType;
    private long eventTime;
    private Product data;

}
