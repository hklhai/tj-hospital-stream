package com.hxqh.constant;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */
public interface RowTypeConstants {

    String[] MEDIUM_VOLTAGE_COLUMN = new String[]{"IEDName", "assetYpe", "productModel", "productModelB", "productModelC", "fractionRatio", "loadRate",
            "CircuitBreaker",
            "PositiveReactive",
            "PositiveActive",
            "EarthKnife",
            "ReverseReactive",
            "ReverseActive",
            "HandcartPosition",
            "AmbientTemperature",
            "CCableTemperature",
            "BCableTemperature",
            "ACableTemperature",
            "CLowerArmTemperature",
            "BLowerArmTemperature",
            "ALowerArmTemperature",
            "CUpperArmTemperature",
            "BUpperArmTemperature",
            "AUpperArmTemperature",
            "APhaseCurrent",
            "BPhaseCurrent",
            "CPhaseCurrent",
            "ABLineVoltage",
            "BCLineVoltage",
            "CALineVoltage",
            "ZeroSequenceCurrent",
            "Frequency",
            "ActivePower",
            "ReactivePower",
            "ApparentPower",
            "ActiveElectricDegree",
            "ReactiveElectricDegree",
            "LineVoltage",
            "LineCurrent",
            "CapacitanceReactivePower",
            "ReactivePowerSymbol",
            "CapacitanceActivePower",
            "No1OpeningVoltage",
            "No1BCapacitanceCurrent",
            "No1CCapacitanceCurrent",
            "No2OpeningVoltage",
            "No2BCapacitanceCurrent",
            "No2CCapacitanceCurrent",
            "No3OpeningVoltage",
            "No3BCapacitanceCurrent",
            "No3CCapacitanceCurrent"
    };


    TypeInformation[] MEDIUM_VOLTAGE_TYPE = new TypeInformation[]{
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE,
            Types.DOUBLE
    };


    TypeInformation[] MEDIUM_VOLTAGE_YX_TYPE = new TypeInformation[]{
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
            Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.INT, Types.STRING, Types.INT
    };


    String[] MEDIUM_VOLTAGE_YX_COLUMN = new String[]{"IEDName", "assetYpe", "parent", "location", "productModel",
            "productModelB", "productModelC", "fractionRatio", "loadRate", "alarmLevel", "VariableName", "Value"
    };


}
