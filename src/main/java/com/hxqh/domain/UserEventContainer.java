package com.hxqh.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ocean lin on 2020/2/25.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@ToString
public class UserEventContainer {

    private String userId;
    private List<UserEvent> userEvents = new ArrayList<>();
}
