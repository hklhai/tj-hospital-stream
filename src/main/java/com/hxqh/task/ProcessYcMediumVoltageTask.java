package com.hxqh.task;

import com.alibaba.fastjson.JSON;
import com.hxqh.domain.YcMediumVoltage;
import com.hxqh.domain.base.IEDEntity;
import com.hxqh.sink.Db2YcMediumVoltageSink;
import com.hxqh.transfer.ProcessWaterEmitter;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.DateUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/2/27.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ProcessYcMediumVoltageTask {

    public static void main(String[] args) {
        args = new String[]{"--input-topic", "yctest", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "yctest"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < NUM_4) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(parameterTool);
        // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));


        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("input-topic"), new SimpleStringSchema(), parameterTool.getProperties());

        FlinkKafkaConsumerBase kafkaConsumerBase = flinkKafkaConsumer.assignTimestampsAndWatermarks(new ProcessWaterEmitter());
        DataStream<String> input = env.addSource(kafkaConsumerBase);
        DataStream<String> filter = input.filter(s -> {
            IEDEntity entity = JSON.parseObject(s, IEDEntity.class);
            return entity.getAssetYpe().equals(MEDIUM_VOLTAG_ESWITCH) ? true : false;
        }).name("YC-MediumVoltage-Filter");

        filter.addSink(new Db2YcMediumVoltageSink()).name("YC-MediumVoltage-DB2-Sink");

        persistEs(filter);

        try {
            env.execute("ProcessYcMediumVoltageTask");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static void persistEs(DataStream<String> input) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, "http"));
        Date now = new Date();

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        IEDEntity entity = JSON.parseObject(element, IEDEntity.class);
                        YcMediumVoltage ycMediumVoltage = ConvertUtils.convert2YcMediumVoltage(entity);

                        TreeMap<String, Object> map = new TreeMap<>();

                        try {
                            map = ConvertUtils.objToMap(ycMediumVoltage);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                        map.put("ColTime", DateUtils.formatDate(ycMediumVoltage.getColTime()));
                        map.put("CreateTime", DateUtils.formatDate(now));
                        return Requests.indexRequest().index(INDEX_YC_MEDIUMVOLTAGE_).type(TYPE_YC_MEDIUMVOLTAGE).source(map);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> restClientBuilder.setMaxRetryTimeoutMillis(300)
        );
        input.addSink(esSinkBuilder.build()).name("YC-MediumVoltage-ElasticSearch-Sink");
    }
}