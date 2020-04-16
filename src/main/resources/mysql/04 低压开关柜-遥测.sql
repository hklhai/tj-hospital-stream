-- 遥测
CREATE TABLE `yc_lowpressure_current`
(
    `YCLOWPRESSURECURRENTID` int NOT NULL AUTO_INCREMENT,
    `IEDNAME`                varchar(100)  DEFAULT NULL,
    `COLTIME`                datetime      DEFAULT NULL,
    `PhaseL1CurrentPercent`  double(31, 2) DEFAULT NULL,
    `PhaseL1L2Voltage`       double(31, 2) DEFAULT NULL,
    `PhaseL2CurrentPercent`  double(31, 2) DEFAULT NULL,
    `PhaseL2L3Voltage`       double(31, 2) DEFAULT NULL,
    `PhaseL3CurrentPercent`  double(31, 2) DEFAULT NULL,
    `PhaseL3L1Voltage`       double(31, 2) DEFAULT NULL,
    `ActiveElectricDegree`   double(31, 2) DEFAULT NULL,
    `ReactiveElectricDegree` double(31, 2) DEFAULT NULL,
    `PowerFactor`            double(31, 2) DEFAULT NULL,
    `OperationNumber`        int(9)        DEFAULT NULL,
    `CREATETIME`             datetime      DEFAULT NULL,
    PRIMARY KEY (`YCLOWPRESSURECURRENTID`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;



""
:
{
            "type": "double"
        },
        "PositiveActive": {
            "type": "double"
        },
        "PositiveReactive": {
            "type": "double"
        },
        "PowerFactor": {
            "type": "double"
        },
        "OperationNumber": {
            "type": "integer"
        }