package com.hxqh.batch.es;

import com.hxqh.custom.ElasticsearchInput;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.hxqh.constant.Constant.ES_HOST;
import static com.hxqh.constant.Constant.ES_PORT;

/**
 * Created by Ocean lin on 2020/3/9.
 *
 * @author Ocean lin
 */
public class ReadElasticSearchDemo {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT));

        String time1 = "2020-03-09 14:52:01";
        String time2 = "2020-03-09 15:37:01";

        RangeQueryBuilder rangequerybuilder = QueryBuilders
                //传入时间，目标格式 2020-01-02T03:17:37.638Z
                .rangeQuery("@ColTime")
                .from(time1).to(time2);


        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(rangequerybuilder);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(boolQueryBuilder));

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.INT, Types.STRING, Types.INT},
                new String[]{"IEDName", "assetYpe", "VariableName", "Value"});

        ElasticsearchInput es = ElasticsearchInput.builder(
                httpHosts, "yx", searchSourceBuilder)
                .setRowTypeInfo(rowTypeInfo)
                .build();


        DataSource<Row> input = env.createInput(es);

        tEnv.registerDataSet("yx", input, "IEDName,assetYpe,VariableName,Value");
        Table order = tEnv.sqlQuery("select IEDName,count(*) as cn from yx group by IEDName");

        DataSet<Row> dataSet = tEnv.toDataSet(order, Row.class);
        dataSet.print();
    }


}
