package com.hxqh.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Created by Ocean lin on 2020/3/1.
 *
 * @author Ocean lin
 */

@Setter
@Getter
@ToString
@AllArgsConstructor
public class UserInfo {

    private Integer userId;
    private String userName;
    private String address;

    public static UserInfo of(Integer userId, String userName, String address) {
        return new UserInfo(userId, userName, address);
    }

}
