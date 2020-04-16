/*
Navicat MySQL Data Transfer

Source Server         : 116.63.137.109-tj
Source Server Version : 80019
Source Host           : 116.63.137.109:3306
Source Database       : tj

Target Server Type    : MYSQL
Target Server Version : 80019
File Encoding         : 65001

Date: 2020-04-15 11:36:48
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for yc_lowpressure_current
-- ----------------------------
DROP TABLE IF EXISTS `yc_lowpressure_current`;
CREATE TABLE `yc_lowpressure_current` (
  `YCLOWPRESSURECURRENTID` int NOT NULL AUTO_INCREMENT,
  `IEDNAME` varchar(100) DEFAULT NULL,
  `COLTIME` datetime DEFAULT NULL,
  `PhaseL1CurrentPercent` double(31,2) DEFAULT NULL,
  `PhaseL1L2Voltage` double(31,2) DEFAULT NULL,
  `PhaseL2CurrentPercent` double(31,2) DEFAULT NULL,
  `PhaseL2L3Voltage` double(31,2) DEFAULT NULL,
  `PhaseL3CurrentPercent` double(31,2) DEFAULT NULL,
  `PhaseL3L1Voltage` double(31,2) DEFAULT NULL,
  `PositiveActive` double(31,2) DEFAULT NULL,
  `PositiveReactive` double(31,2) DEFAULT NULL,
  `PowerFactor` double(31,2) DEFAULT NULL,
  `OperationNumber` int DEFAULT NULL,
  `CREATETIME` datetime DEFAULT NULL,
  PRIMARY KEY (`YCLOWPRESSURECURRENTID`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_lowpressure_current
-- ----------------------------
INSERT INTO `yc_lowpressure_current` VALUES ('1', '2AA1', '2020-04-15 09:09:03', '89.89', '22.32', '33.00', '44.02', '88.00', '90.03', '555.03', '888.03', '999.03', '234', '2020-04-15 09:15:50');

-- ----------------------------
-- Table structure for yc_medium_voltage_current
-- ----------------------------
DROP TABLE IF EXISTS `yc_medium_voltage_current`;
CREATE TABLE `yc_medium_voltage_current` (
  `YCMEDIUMVOLTAGEID` int NOT NULL AUTO_INCREMENT COMMENT 'yc_medium_voltage_current主键',
  `IEDNAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备编码',
  `COLTIME` datetime DEFAULT NULL COMMENT '时间戳',
  `CIRCUITBREAKER` double(31,2) DEFAULT NULL COMMENT '断路器的位置（0,分闸，1，合闸，2,位置未知状态）',
  `POSITIVEREACTIVE` double(31,2) DEFAULT NULL COMMENT '正向无功电度',
  `POSITIVEACTIVE` double(31,2) DEFAULT NULL COMMENT '正向有功电度',
  `EARTHKNIFE` double(31,2) DEFAULT NULL COMMENT '地刀位置（0,分闸，1，合闸，2,位置未知状态）',
  `REVERSEREACTIVE` double(31,2) DEFAULT NULL COMMENT '反向无功电度',
  `REVERSEACTIVE` double(31,2) DEFAULT NULL COMMENT '反向无功电度',
  `HANDCARTPOSITION` double(31,2) DEFAULT NULL COMMENT '手车位置（0,分闸，1，合闸，2,位置未知状态）',
  `AMBIENTTEMPERATURE` double(31,2) DEFAULT NULL COMMENT '环境温度',
  `CCABLETEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相电缆头温度',
  `BCABLETEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'B相电缆头温度',
  `ACABLETEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'A相电缆头温度',
  `CLOWERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相下触臂温度',
  `BLOWERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'B相下触臂温度',
  `ALOWERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'A相下触臂温度',
  `CUPPERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相上触臂温度',
  `BUPPERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'B相上触臂温度',
  `AUPPERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相上触臂温度',
  `APHASECURRENT` double(31,2) DEFAULT NULL COMMENT 'A相电流',
  `BPHASECURRENT` double(31,2) DEFAULT NULL COMMENT 'B相电流',
  `CPHASECURRENT` double(31,2) DEFAULT NULL COMMENT 'C相电流',
  `ABLINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT 'AB线电压',
  `BCLINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT 'BC线电压',
  `CALINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT 'CA线电压',
  `ZEROSEQUENCECURRENT` double(31,2) DEFAULT NULL COMMENT '零序电流',
  `FREQUENCY` double(31,2) DEFAULT NULL COMMENT '频率',
  `ACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '有功功率',
  `REACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '无功功率',
  `APPARENTPOWER` double(31,2) DEFAULT NULL COMMENT '视在功率',
  `ACTIVEELECTRICDEGREE` double(31,2) DEFAULT NULL COMMENT '有功电度',
  `REACTIVEELECTRICDEGREE` double(31,2) DEFAULT NULL COMMENT '无功电度',
  `LINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT '线路电压',
  `LINECURRENT` double(31,2) DEFAULT NULL COMMENT '线路电流',
  `CAPACITANCEREACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '无功功率',
  `REACTIVEPOWERSYMBOL` double(31,2) DEFAULT NULL COMMENT '无功符号、功率因数',
  `CAPACITANCEACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '有功功率',
  `NO1OPENINGVOLTAGE` double(31,2) DEFAULT NULL COMMENT '1＃开口电压',
  `NO1BCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '1＃B相电容电流',
  `NO1CCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '1＃C相电容电流',
  `NO2OPENINGVOLTAGE` double(31,2) DEFAULT NULL COMMENT '2＃开口电压',
  `NO2BCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '2＃B相电容电流',
  `NO2CCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '2＃C相电容电流',
  `NO3OPENINGVOLTAGE` double(31,2) DEFAULT NULL COMMENT '3＃开口电压',
  `NO3BCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '3＃B相电容电流',
  `NO3CCAPACITANCECURRENT` double(31,2) unsigned DEFAULT NULL COMMENT '3＃C相电容电流',
  `CREATETIME` datetime DEFAULT NULL COMMENT '处理时间',
  PRIMARY KEY (`YCMEDIUMVOLTAGEID`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_medium_voltage_current
-- ----------------------------
INSERT INTO `yc_medium_voltage_current` VALUES ('1', 'AH01', '2020-04-12 14:59:00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '11.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '2020-04-08 15:52:35');
INSERT INTO `yc_medium_voltage_current` VALUES ('2', 'AH02', '2020-03-26 17:54:03', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '2020-04-13 17:55:04');
INSERT INTO `yc_medium_voltage_current` VALUES ('3', 'AH03', '2020-03-16 18:28:03', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '2020-04-14 18:29:01');
INSERT INTO `yc_medium_voltage_current` VALUES ('4', 'AH20', '2020-03-16 18:28:03', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '1.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '0.00', '2020-04-14 18:42:07');

-- ----------------------------
-- Table structure for yc_medium_voltage_log
-- ----------------------------
DROP TABLE IF EXISTS `yc_medium_voltage_log`;
CREATE TABLE `yc_medium_voltage_log` (
  `YCMEDIUMVOLTAGEID` int NOT NULL AUTO_INCREMENT COMMENT 'yc_medium_voltage_current主键',
  `IEDNAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备编码',
  `COLTIME` datetime DEFAULT NULL COMMENT '时间戳',
  `CIRCUITBREAKER` double(31,2) DEFAULT NULL COMMENT '断路器的位置（0,分闸，1，合闸，2,位置未知状态）',
  `POSITIVEREACTIVE` double(31,2) DEFAULT NULL COMMENT '正向无功电度',
  `POSITIVEACTIVE` double(31,2) DEFAULT NULL COMMENT '正向有功电度',
  `EARTHKNIFE` double(31,2) DEFAULT NULL COMMENT '地刀位置（0,分闸，1，合闸，2,位置未知状态）',
  `REVERSEREACTIVE` double(31,2) DEFAULT NULL COMMENT '反向无功电度',
  `REVERSEACTIVE` double(31,2) DEFAULT NULL COMMENT '反向无功电度',
  `HANDCARTPOSITION` double(31,2) DEFAULT NULL COMMENT '手车位置（0,分闸，1，合闸，2,位置未知状态）',
  `AMBIENTTEMPERATURE` double(31,2) DEFAULT NULL COMMENT '环境温度',
  `CCABLETEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相电缆头温度',
  `BCABLETEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'B相电缆头温度',
  `ACABLETEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'A相电缆头温度',
  `CLOWERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相下触臂温度',
  `BLOWERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'B相下触臂温度',
  `ALOWERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'A相下触臂温度',
  `CUPPERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相上触臂温度',
  `BUPPERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'B相上触臂温度',
  `AUPPERARMTEMPERATURE` double(31,2) DEFAULT NULL COMMENT 'C相上触臂温度',
  `APHASECURRENT` double(31,2) DEFAULT NULL COMMENT 'A相电流',
  `BPHASECURRENT` double(31,2) DEFAULT NULL COMMENT 'B相电流',
  `CPHASECURRENT` double(31,2) DEFAULT NULL COMMENT 'C相电流',
  `ABLINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT 'AB线电压',
  `BCLINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT 'BC线电压',
  `CALINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT 'CA线电压',
  `ZEROSEQUENCECURRENT` double(31,2) DEFAULT NULL COMMENT '零序电流',
  `FREQUENCY` double(31,2) DEFAULT NULL COMMENT '频率',
  `ACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '有功功率',
  `REACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '无功功率',
  `APPARENTPOWER` double(31,2) DEFAULT NULL COMMENT '视在功率',
  `ACTIVEELECTRICDEGREE` double(31,2) DEFAULT NULL COMMENT '有功电度',
  `REACTIVEELECTRICDEGREE` double(31,2) DEFAULT NULL COMMENT '无功电度',
  `LINEVOLTAGE` double(31,2) DEFAULT NULL COMMENT '线路电压',
  `LINECURRENT` double(31,2) DEFAULT NULL COMMENT '线路电流',
  `CAPACITANCEREACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '无功功率',
  `REACTIVEPOWERSYMBOL` double(31,2) DEFAULT NULL COMMENT '无功符号、功率因数',
  `CAPACITANCEACTIVEPOWER` double(31,2) DEFAULT NULL COMMENT '有功功率',
  `NO1OPENINGVOLTAGE` double(31,2) DEFAULT NULL COMMENT '1＃开口电压',
  `NO1BCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '1＃B相电容电流',
  `NO1CCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '1＃C相电容电流',
  `NO2OPENINGVOLTAGE` double(31,2) DEFAULT NULL COMMENT '2＃开口电压',
  `NO2BCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '2＃B相电容电流',
  `NO2CCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '2＃C相电容电流',
  `NO3OPENINGVOLTAGE` double(31,2) DEFAULT NULL COMMENT '3＃开口电压',
  `NO3BCAPACITANCECURRENT` double(31,2) DEFAULT NULL COMMENT '3＃B相电容电流',
  `NO3CCAPACITANCECURRENT` double(31,2) unsigned DEFAULT NULL COMMENT '3＃C相电容电流',
  `CREATETIME` datetime DEFAULT NULL COMMENT '处理时间',
  PRIMARY KEY (`YCMEDIUMVOLTAGEID`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_medium_voltage_log
-- ----------------------------

-- ----------------------------
-- Table structure for yc_medium_voltage_run
-- ----------------------------
DROP TABLE IF EXISTS `yc_medium_voltage_run`;
CREATE TABLE `yc_medium_voltage_run` (
  `YCMEDIUMVOLTAGERUNID` int NOT NULL AUTO_INCREMENT,
  `IEDNAME` varchar(100) DEFAULT NULL,
  `COLTIME` datetime DEFAULT NULL,
  `PHASECURRENT` double(31,2) DEFAULT NULL,
  `RUNNINGTIME1` double(31,4) DEFAULT NULL,
  `DOWNTIME1` double(31,4) DEFAULT NULL,
  `RUNNINGTIME2` double(31,4) DEFAULT NULL,
  `DOWNTIME2` double(31,4) DEFAULT NULL,
  `RUNNINGTIME3` double(31,4) DEFAULT NULL,
  `DOWNTIME3` double(31,4) DEFAULT NULL,
  `RUNNINGTIME4` double(31,4) DEFAULT NULL,
  `DOWNTIME4` double(31,4) DEFAULT NULL,
  `RUNSTATUS` int DEFAULT NULL,
  `CREATETIME` datetime DEFAULT NULL,
  `PARTICULARYEAR` int DEFAULT NULL,
  PRIMARY KEY (`YCMEDIUMVOLTAGERUNID`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_medium_voltage_run
-- ----------------------------
INSERT INTO `yc_medium_voltage_run` VALUES ('1', 'AH01', '2020-04-12 14:59:00', '3.67', '4320000.0000', '0.0000', '-226115.0000', '192960.0000', '-51840.0000', '48960.0000', '0.0000', '0.0000', '1', '2020-04-08 15:52:35', '2020');
INSERT INTO `yc_medium_voltage_run` VALUES ('2', 'AH02', '2020-03-26 17:54:03', '1.00', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0', '2020-04-13 17:55:04', '2020');
INSERT INTO `yc_medium_voltage_run` VALUES ('3', 'AH03', '2020-03-16 18:28:03', '1.00', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0', '2020-04-14 18:29:01', '2020');
INSERT INTO `yc_medium_voltage_run` VALUES ('4', 'AH20', '2020-03-16 18:28:03', '1.00', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0.0000', '0', '2020-04-14 18:42:07', '2020');

-- ----------------------------
-- Table structure for yc_medium_voltage_run_month
-- ----------------------------
DROP TABLE IF EXISTS `yc_medium_voltage_run_month`;
CREATE TABLE `yc_medium_voltage_run_month` (
  `YCMEDIUMVOLTAGERUNMONTHID` int NOT NULL AUTO_INCREMENT,
  `IEDNAME` varchar(100) DEFAULT NULL,
  `COLTIME` datetime DEFAULT NULL,
  `RUNNINGTIME` double(31,4) DEFAULT NULL,
  `DOWNTIME` double(31,4) DEFAULT NULL,
  `RUNSTATUS` int DEFAULT NULL,
  `CREATETIME` datetime DEFAULT NULL,
  `PARTICULARTIME` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`YCMEDIUMVOLTAGERUNMONTHID`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_medium_voltage_run_month
-- ----------------------------
INSERT INTO `yc_medium_voltage_run_month` VALUES ('8', 'AH01', '2020-04-12 14:59:00', '2880.0000', '2880.0000', '1', '2020-04-08 15:52:35', '2020-04');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('9', 'AH01', '2020-04-12 14:59:00', '2880.0000', '2880.0000', '1', '2020-04-08 15:52:35', '2020-03');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('10', 'AH01', '2020-04-12 14:59:00', '5880.0000', '5880.0000', '1', '2020-04-08 15:52:35', '2020-02');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('11', 'AH01', '2020-04-12 14:59:00', '1880.0000', '1880.0000', '1', '2020-04-08 15:52:35', '2020-01');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('12', 'AH02', '2020-04-12 14:59:00', '1880.0000', '1880.0000', '1', '2020-04-08 15:52:35', '2020-01');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('13', 'AH02', '2020-04-12 14:59:00', '3880.0000', '3880.0000', '1', '2020-04-08 15:52:35', '2020-02');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('14', 'AH02', '2020-04-12 14:59:00', '8880.0000', '8880.0000', '1', '2020-04-08 15:52:35', '2020-03');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('15', 'AH02', '2020-03-26 17:54:03', '0.0000', '0.0000', '0', '2020-04-13 17:55:04', '2020-04');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('16', 'AH03', '2020-03-16 18:28:03', '0.0000', '0.0000', '0', '2020-04-14 18:29:01', '2020-04');
INSERT INTO `yc_medium_voltage_run_month` VALUES ('17', 'AH20', '2020-03-16 18:28:03', '0.0000', '0.0000', '0', '2020-04-14 18:42:07', '2020-04');

-- ----------------------------
-- Table structure for yc_transformer_current
-- ----------------------------
DROP TABLE IF EXISTS `yc_transformer_current`;
CREATE TABLE `yc_transformer_current` (
  `YCTRANSFORMERID` int NOT NULL AUTO_INCREMENT COMMENT 'yc_transformer_current主键',
  `IEDNAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备编码',
  `COLTIME` datetime DEFAULT NULL COMMENT '时间戳',
  `APhaseTemperature` double(31,2) DEFAULT NULL COMMENT 'A相绕组温度',
  `BPhaseTemperature` double(31,2) DEFAULT '0.00' COMMENT 'B相绕组温度',
  `CPhaseTemperature` double(31,2) DEFAULT '0.00' COMMENT 'C相绕组温度',
  `DRoadTemperature` double(31,2) DEFAULT '0.00' COMMENT 'd路温度',
  `CREATETIME` datetime DEFAULT NULL COMMENT '处理时间',
  PRIMARY KEY (`YCTRANSFORMERID`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_transformer_current
-- ----------------------------
INSERT INTO `yc_transformer_current` VALUES ('1', 'TRA2', '2020-04-13 14:35:03', '90.00', '40.00', '50.00', '31.00', '2020-04-13 14:35:41');

-- ----------------------------
-- Table structure for yc_transformer_log
-- ----------------------------
DROP TABLE IF EXISTS `yc_transformer_log`;
CREATE TABLE `yc_transformer_log` (
  `YCTRANSFORMERID` int NOT NULL AUTO_INCREMENT COMMENT 'yc_transformer_current主键',
  `IEDNAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备编码',
  `COLTIME` datetime DEFAULT NULL COMMENT '时间戳',
  `APhaseTemperature` double(31,2) DEFAULT NULL COMMENT 'A相绕组温度',
  `BPhaseTemperature` double(31,2) DEFAULT '0.00' COMMENT 'B相绕组温度',
  `CPhaseTemperature` double(31,2) DEFAULT '0.00' COMMENT 'C相绕组温度',
  `DRoadTemperature` double(31,2) DEFAULT '0.00' COMMENT 'd路温度',
  `CREATETIME` datetime DEFAULT NULL COMMENT '处理时间',
  PRIMARY KEY (`YCTRANSFORMERID`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yc_transformer_log
-- ----------------------------

-- ----------------------------
-- Table structure for yx_current
-- ----------------------------
DROP TABLE IF EXISTS `yx_current`;
CREATE TABLE `yx_current` (
  `YXID` int NOT NULL AUTO_INCREMENT COMMENT 'yx_current主键',
  `IEDNAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备名称',
  `COLTIME` datetime DEFAULT NULL COMMENT '时间戳',
  `VARIABLENAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '事件名称',
  `VAL` int DEFAULT NULL COMMENT '状态',
  `CREATETIME` datetime DEFAULT NULL COMMENT '处理时间',
  PRIMARY KEY (`YXID`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yx_current
-- ----------------------------
INSERT INTO `yx_current` VALUES ('1', 'AH02', '2020-03-23 17:58:44', 'OverCurrent', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('2', 'AH01', '2020-03-23 17:58:44', 'QuickBreak', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('3', 'AH03', '2020-03-23 17:58:44', 'OverCurrentDelay', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('4', 'AH22', '2020-03-23 17:58:44', 'CircuitBreaker', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('9', 'TRA1', '2020-04-13 11:27:40', 'FanOperationStatus', '1', '2020-04-13 11:27:52');
INSERT INTO `yx_current` VALUES ('10', 'AH01', '2020-03-23 17:58:45', 'CircuitDisconnection', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('11', 'AH01', '2020-03-23 17:58:44', 'NoEnergyDtorage', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('12', 'AH22', '2020-03-23 17:58:45', 'EarthKnife', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('13', 'AH22', '2020-03-23 17:58:45', 'HandcartPosition', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('14', 'AH01', '2020-03-23 17:58:45', 'CCableOvertemperature', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('15', 'AH01', '2020-03-23 17:58:45', 'BCableOvertemperature', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('16', 'AH01', '2020-03-23 17:58:45', 'ACableOvertemperature', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('17', 'AH01', '2020-03-23 17:58:45', 'CLowerArmOvertemperature', '0', '2020-03-25 15:46:44');
INSERT INTO `yx_current` VALUES ('18', 'TRA1', '2020-04-13 14:03:40', 'TemperatureControlFailure', '1', '2020-04-13 14:03:30');
INSERT INTO `yx_current` VALUES ('19', 'TRA2', '2020-03-01 10:05:40', 'WindingOvertemperatureTrip', '1', '2020-04-15 10:52:32');
INSERT INTO `yx_current` VALUES ('20', 'TRA1', '2020-04-15 11:13:40', 'WindingOvertemperatureAlarm', '1', '2020-04-15 11:13:47');
INSERT INTO `yx_current` VALUES ('21', 'TRA1', '2020-04-15 11:15:40', 'WindingOvertemperatureTrip', '0', '2020-04-15 11:15:16');

-- ----------------------------
-- Table structure for yx_log
-- ----------------------------
DROP TABLE IF EXISTS `yx_log`;
CREATE TABLE `yx_log` (
  `YXID` int NOT NULL AUTO_INCREMENT COMMENT 'yx_current主键',
  `IEDNAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备名称',
  `COLTIME` datetime DEFAULT NULL COMMENT '时间戳',
  `VARIABLENAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '事件名称',
  `VAL` int DEFAULT NULL COMMENT '状态',
  `CREATETIME` datetime DEFAULT NULL COMMENT '处理时间',
  PRIMARY KEY (`YXID`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yx_log
-- ----------------------------

-- ----------------------------
-- Table structure for yx_score
-- ----------------------------
DROP TABLE IF EXISTS `yx_score`;
CREATE TABLE `yx_score` (
  `YXSCOREID` int NOT NULL AUTO_INCREMENT,
  `IEDNAME` varchar(100) DEFAULT NULL,
  `COLTIME` datetime DEFAULT NULL,
  `SCORE` int DEFAULT NULL,
  `HIGHLEVEL` int DEFAULT NULL,
  `VARIABLENAME` varchar(100) DEFAULT NULL,
  `VAL` int DEFAULT NULL,
  `CREATETIME` datetime DEFAULT NULL,
  PRIMARY KEY (`YXSCOREID`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of yx_score
-- ----------------------------
INSERT INTO `yx_score` VALUES ('1', 'TRA1', '2020-04-15 11:15:40', '90', '1', 'WindingOvertemperatureTrip', '0', '2020-04-15 11:15:17');
