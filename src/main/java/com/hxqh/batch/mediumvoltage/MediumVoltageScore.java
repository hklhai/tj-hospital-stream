package com.hxqh.batch.mediumvoltage;

import com.hxqh.batch.mediumvoltage.input.MediumVoltageInput;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.*;
import static com.hxqh.constant.RowTypeConstants.MEDIUM_VOLTAGE_YX_COLUMN;
import static com.hxqh.constant.RowTypeConstants.MEDIUM_VOLTAGE_YX_TYPE;

/**
 * Created by Ocean lin on 2020/3/11.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class MediumVoltageScore {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT));


        RowTypeInfo rowTypeInfo = new RowTypeInfo(MEDIUM_VOLTAGE_YX_TYPE, MEDIUM_VOLTAGE_YX_COLUMN);

        MediumVoltageInput build = MediumVoltageInput.builder(
                httpHosts, INDEX_YX)
                .setRowTypeInfo(rowTypeInfo)
                .build();
        DataSource<Row> input = env.createInput(build);

        tEnv.registerDataSet("score", input,
                "IEDName,assetYpe,parent,location,productModel,productModelB,productModelC,fractionRatio,loadRate,alarmLevel,VariableName,Value");
        Table mediumvoltage = tEnv.sqlQuery("select * from score");

        DataSet<Row> dataSet = tEnv.toDataSet(mediumvoltage, Row.class);
        dataSet.print();


    }
}
