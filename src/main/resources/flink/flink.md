## 启动Flink作业

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessTask /root/TJJar/base/tj-hospital.jar > /root/TJJar/processTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessErrorTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessErrorTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcMediumVoltageTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcMediumVoltageTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYxTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYxTask.log 2>&1 &

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.task.ProcessYcTransformerTask /root/TJJar/tj-hospital.jar > /root/TJJar/ProcessYcTransformerTask.log 2>&1 &


### Report

nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.mediumvoltage.MediumVoltageScore /root/TJJar/batch/tj-hospital.jar > /root/TJJar/MediumVoltageScore.log 2>&1 &

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


## 批量停止Flink作业
ps -ef | grep flink | grep -v grep | awk '{print "kill -9 "$2}'|sh



## Flink
```
/root/app/flink-1.8.0/bin/start-cluster.sh 
```
