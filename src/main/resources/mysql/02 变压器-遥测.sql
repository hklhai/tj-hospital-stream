-- 遥测
CREATE TABLE `yc_transformer_current`
(
    `YCTRANSFORMERID`    int NOT NULL AUTO_INCREMENT,
    `IEDNAME`           varchar(100)  DEFAULT NULL,
    `COLTIME`           datetime      DEFAULT NULL,
    `APhaseTemperature` double(31, 2) DEFAULT NULL,
    `BPhaseTemperature` double(31, 2) DEFAULT NULL,
    `CPhaseTemperature` double(31, 2) DEFAULT NULL,
    `DRoadTemperature`  double(31, 2) DEFAULT NULL,
    `CREATETIME`        datetime      DEFAULT NULL,
    PRIMARY KEY (`YCTRANSFORMERID`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
