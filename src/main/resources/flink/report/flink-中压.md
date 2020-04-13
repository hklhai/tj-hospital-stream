## 中压
### 单台-中压-健康状况
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.MediumVoltageScoreQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageScoreQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.MediumVoltageScoreYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageScoreYear.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.MediumVoltageCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageCompareQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.MediumVoltageCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageCompareYear.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.MediumVoltageCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageCompareYear.log 2>&1 &


### 总体-中压-健康状况
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageScoreMonth /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageScoreMonth.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageScoreQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageScoreQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageScoreYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageScoreYear.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageCompareQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageCompareYear.log 2>&1 &


nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageHealthLevelQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageHealthLevelQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.total.TotalMediumVoltageHealthLevelYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageHealthLevelYear.log 2>&1 &

### 单台-中压-负荷率
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.MediumVoltageLoadFactorQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageLoadFactorQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.MediumVoltageLoadFactorYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageLoadFactorYear.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.MediumVoltageLoadFactorCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageLoadFactorCompareQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.MediumVoltageLoadFactorCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageLoadFactorCompareYear.log 2>&1 &


### 总体-中压-负荷率
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.total.TotalMediumVoltageLoadFactorQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageLoadFactorQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.total.TotalMediumVoltageLoadFactorYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageLoadFactorYear.log 2>&1 &

### 总体-中压-负荷率对比
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.total.TotalMediumVoltageLoadFactorCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageLoadFactorCompareQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.total.TotalMediumVoltageLoadFactorCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageLoadFactorCompareYear.log 2>&1 &


### 总体-中压-负荷率评分
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.total.TotalMediumVoltageLoadFactorLevelQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageLoadFactorLevelQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.loadfactor.total.TotalMediumVoltageLoadFactorLevelYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageLoadFactorLevelYear.log 2>&1 &

### 中压-电压状况
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.condition.MediumVoltageCondition /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageCondition.log 2>&1 &

### 中压-平均功率因数
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.condition.MediumVoltagePowerFactor /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltagePowerFactor.log 2>&1 &

### 单台中压设备-季度、年度有功电度量和无功电度量
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.MediumVoltageElectricalMeasurementQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageElectricalMeasurementQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.MediumVoltageElectricalMeasurementYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageElectricalMeasurementYear.log 2>&1 &

### 单台中压设备-季度、年度总度量对比
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.MediumVoltageElectricalMeasurementCompQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageElectricalMeasurementCompQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.MediumVoltageElectricalMeasurementCompYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageElectricalMeasurementCompYear.log 2>&1 &


### 整体中压设备-季度、年度有功电度量和无功电度量
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.total.TotalMediumVoltageElectricalMeasurementQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageElectricalMeasurementQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.total.TotalMediumVoltageElectricalMeasurementYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageElectricalMeasurementYear.log 2>&1 &

### 整体中压设备-季度、年度总度量对比
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.total.TotalMediumVoltageElectricalMeasurementCompQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageElectricalMeasurementCompQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.electricalmeasurement.total.TotalMediumVoltageElectricalMeasurementCompYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageElectricalMeasurementCompYear.log 2>&1 &




## 温度统计
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.temperature.MediumVoltageTemperature /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageTemperature.log 2>&1 &


## 中压开关柜运行时长及使用效率
### 单台
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyYear.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyCompareQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyCompareYear.log 2>&1 &

### 单台中压开关柜季度统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyStatisticsQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyStatisticsQuarter.log 2>&1 &

### 单台中压开关柜年度统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.MediumVoltageUseEfficiencyStatisticsYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageUseEfficiencyStatisticsYear.log 2>&1 &


### 整体
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyYear.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyCompareQuarter.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyCompareYear.log 2>&1 &

### 整体中压开关柜季度统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyStatisticsQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyStatisticsQuarter.log 2>&1 &

### 整体中压开关柜年度统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyStatisticsYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyStatisticsYear.log 2>&1 &

### 整体中压开关柜季度评分信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyLevelQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyLevelQuarter.log 2>&1 &

### 整体中压开关柜年度评分信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUseEfficiencyLevelYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUseEfficiencyLevelYear.log 2>&1 &




## 报警统计
### 单台中压设备-季度报警统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.alarm.MediumVoltageAlarmQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageAlarmQuarter.log 2>&1 &
### 单台中压设备-年度报警统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.alarm.MediumVoltageAlarmYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageAlarmYear.log 2>&1 &


### 整体中压设备-季度报警统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.alarm.total.TotalMediumVoltageAlarmQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageAlarmQuarter.log 2>&1 &
### 整体中压设备-年度报警统计信息
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.alarm.total.TotalMediumVoltageAlarmYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageAlarmYear.log 2>&1 &

### 整体中压设备速断和过流、延时过流、差动保护最多的设备-季度
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUEMostFailuresQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUEMostFailuresQuarter.log 2>&1 &

### 整体中压设备速断和过流、延时过流、差动保护最多的设备-年度
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.useefficiency.total.TotalMediumVoltageUEMostFailuresYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalMediumVoltageUEMostFailuresYear.log 2>&1 &
