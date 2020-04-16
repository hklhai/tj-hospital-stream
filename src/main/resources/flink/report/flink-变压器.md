## 健康状况
### 单台变压器运行健康状况-季度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.health.TransFormerHealthCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TransFormerHealthCompareQuarter.log 2>&1 &

### 单台变压器运行健康状况-年度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.health.TransFormerHealthCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TransFormerHealthCompareYear.log 2>&1 &

### 整体器运行健康状况-季度
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.health.total.TotalTransFormerHealthQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransFormerHealthQuarter.log 2>&1 &

### 整体器运行健康状况-年度
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.health.total.TotalTransFormerHealthYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransFormerHealthYear.log 2>&1 &

### 整体器运行健康状况-季度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.health.total.TotalTransFormerHealthCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransFormerHealthCompareQuarter.log 2>&1 &

### 整体器运行健康状况-年度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.health.total.TotalTransFormerHealthCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransFormerHealthCompareYear.log 2>&1 &



## 供用电状况
### 单台变压器供用电状况-季度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.powersupply.TransformerPowerSupplyCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TransformerPowerSupplyCompareQuarter.log 2>&1 &

### 单台变压器供用电状况-年度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.powersupply.TransformerPowerSupplyCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TransformerPowerSupplyCompareYear.log 2>&1 &

### 整体变压器供用电状况-季度
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.powersupply.total.TotalTransformerPowerSupplyQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransformerPowerSupplyQuarter.log 2>&1 &

### 整体变压器供用电状况-年度
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.powersupply.total.TotalTransformerPowerSupplyYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransformerPowerSupplyYear.log 2>&1 &

### 整体变压器供用电状况-季度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.powersupply.total.TotalTransformerPowerSupplyCompareQuarter /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransformerPowerSupplyCompareQuarter.log 2>&1 &

### 整体变压器供用电状况-年度比对
nohup /root/app/flink-1.8.0/bin/flink run -c com.hxqh.batch.transformer.powersupply.total.TotalTransformerPowerSupplyCompareYear /root/TJJar/batch/tj-hospital.jar > /root/TJJar/TotalTransformerPowerSupplyCompareYear.log 2>&1 &

