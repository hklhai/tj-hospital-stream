package com.hxqh.batch.transformer.powersupply;

/**
 * Created by Ocean lin on 2020/4/23.
 *
 * @author Ocean lin
 */
public class TransformerPsRunTimeUpdateMonth {


    // todo

    // 1. 查询上月数据并更新；对于状态为故障的数据需要需要结转时间，即将月末时间-故障事件时间得div，运行时间-div，故障时间+div


    // 2. 查询上月数据后，更新当月数据；查询上月故障数据，采用左外连接更新当月数据状态


}
