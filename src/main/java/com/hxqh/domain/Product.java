package com.hxqh.domain;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * Created by Ocean lin on 2020/2/24.
 *
 * @author Ocean lin
 */

@Data
@ToString
public class Product implements Serializable {

    private static final long serialVersionUID = -5301476465437984361L;

    private Integer productId;
    private Double price;
    private Integer amount;
}
