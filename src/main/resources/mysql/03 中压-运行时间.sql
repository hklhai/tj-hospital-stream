-- 运行时间-季度
CREATE TABLE `YC_MEDIUM_VOLTAGE_RUN`
(
    `YCMEDIUMVOLTAGERUNID` int NOT NULL AUTO_INCREMENT,
    `IEDNAME`              varchar(100)  DEFAULT NULL,
    `COLTIME`              datetime      DEFAULT NULL,
    `PHASECURRENT`         double(31, 2) DEFAULT NULL,
    `RUNNINGTIME1`         double(31, 4) DEFAULT NULL,
    `DOWNTIME1`            double(31, 4) DEFAULT NULL,
    `RUNNINGTIME2`         double(31, 4) DEFAULT NULL,
    `DOWNTIME2`            double(31, 4) DEFAULT NULL,
    `RUNNINGTIME3`         double(31, 4) DEFAULT NULL,
    `DOWNTIME3`            double(31, 4) DEFAULT NULL,
    `RUNNINGTIME4`         double(31, 4) DEFAULT NULL,
    `DOWNTIME4`            double(31, 4) DEFAULT NULL,
    `RUNSTATUS`            int(1)        DEFAULT NULL,
    `CREATETIME`           datetime      DEFAULT NULL,
    `PARTICULARYEAR`       int(4)        DEFAULT NULL,
    PRIMARY KEY (`YCMEDIUMVOLTAGERUNID`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- 运行时间-月份
CREATE TABLE `YC_MEDIUM_VOLTAGE_RUN_MONTH`
(
    `YCMEDIUMVOLTAGERUNMONTHID` int NOT NULL AUTO_INCREMENT,
    `IEDNAME`                   varchar(100)  DEFAULT NULL,
    `COLTIME`                   datetime      DEFAULT NULL,
    `RUNNINGTIME`               double(31, 4) DEFAULT NULL,
    `DOWNTIME`                  double(31, 4) DEFAULT NULL,
    `RUNSTATUS`                 int(1)        DEFAULT NULL,
    `CREATETIME`                datetime      DEFAULT NULL,
    `PARTICULARTIME`            varchar(50)   DEFAULT NULL,
    PRIMARY KEY (`YCMEDIUMVOLTAGERUNMONTHID`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
