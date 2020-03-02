package com.hxqh.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by Ocean lin on 2020/2/26.
 *
 * @author Ocean lin
 */

@Getter
@Setter
@NoArgsConstructor
public class AssetType implements Serializable {

    private static final long serialVersionUID = -2982466718374199140L;
    private String assetnum;
    private String assetYpe;
    private String productModel;
    private String parent;
    private String location;

}
