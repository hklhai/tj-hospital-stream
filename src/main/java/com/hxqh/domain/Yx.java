package com.hxqh.domain;

import com.alibaba.fastjson.annotation.JSONField;
import com.hxqh.domain.base.IEDParam;
import com.hxqh.domain.base.IEDParameter;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.List;

/**
 * Created by Ocean lin on 2020/4/20.
 *
 * @author Ocean lin
 */
@Getter
@Setter
@NoArgsConstructor
public class Yx {
    /**
     * {
     *     "IEDName": "1AA1",
     *     "CKType": "YX",
     *     "ColTime": "2020-02-27 10:33:40",
     *     "IEDParam": [
     *         {
     *             "VariableName": "GPI1",
     *             "Value": "0"
     *         }
     *     ]
     * }
     */

    @JSONField(name = "IEDName")
    private String IEDName;

    @JSONField(name = "CKType")
    private String CKType;

    @JSONField(name = "ColTime", format = "yyyy-MM-dd HH:mm:ss")
    private Date ColTime;

    @JSONField(name = "IEDParam")
    private List<IEDParameter> IEDParam;



}
