package com.hxqh.task;

import com.hxqh.task.sink.MySQLYxScoreSink;
import com.hxqh.task.sink.MySQLYxSink;
import com.hxqh.transfer.ProcessYxWaterEmitter;
import com.hxqh.utils.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/3/6.
 * <p>
 * 告警    0-无告警  1-是告警
 * 分合闸  0-分闸    1-是合闸
 * <p>
 * <p>
 * 告警    速断和过流
 * 分合闸  开关位置
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ProcessYxTask {

    public static void main(String[] args) {

        args = new String[]{"--input-topic", "yxtest", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "yxtest"};

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < NUM_4) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic>" +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        // create a checkpoint every 5 seconds 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(parameterTool);
        // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));


        tableEnvironment.connect(new Kafka()
                .version("0.10")
                .topic(parameterTool.getRequired("input-topic"))
                .startFromLatest()
                .property("connector.type", "kafka")
                .property("group.id", parameterTool.getRequired("group.id"))
                .property("bootstrap.servers", parameterTool.getRequired("bootstrap.servers")))
                .withFormat(
                        // 指定字段缺失是否允许失败
                        new Json().failOnMissingField(false)
                                .deriveSchema()
                ).withSchema(
                new Schema()
                        .field("IEDName", Types.STRING())
                        .field("CKType", Types.STRING())
                        .field("ColTime", Types.STRING())
                        .field("assetYpe", Types.STRING())
                        .field("location", Types.STRING())
                        .field("parent", Types.STRING())
                        .field("productModel", Types.STRING())
                        .field("productModelB", Types.STRING())
                        .field("productModelC", Types.STRING())
                        .field("fractionRatio", Types.DOUBLE())
                        .field("loadRate", Types.DOUBLE())
                        .field("IEDParam", ObjectArrayTypeInfo.getInfoFor(
                                Row[].class,
                                Types.ROW(new String[]{"variableName", "value"},
                                        new TypeInformation[]{Types.STRING(), Types.INT()})))

        ).inAppendMode().registerTableSource("yx");

        Table table = tableEnvironment.sqlQuery("select * from yx");
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnvironment.toRetractStream(table, Row.class);

        DataStream<Row> data = rowDataStream.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
                return value.f0;
            }
        }).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> value) throws Exception {
                return value.f1;
            }
        });

        data.assignTimestampsAndWatermarks(new ProcessYxWaterEmitter());

        data.addSink(new MySQLYxSink()).name("YX-MySQL-Sink");
        // 处理实时得分
        data.addSink(new MySQLYxScoreSink()).name("YX-MySQL-Score-Sink");

        persistEs(data);

        try {
            env.execute("ProcessYxTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void persistEs(DataStream<Row> input) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, HTTP));
        Date now = new Date();

        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Row>() {
                    public IndexRequest createIndexRequest(Row row) {

                        String iedName = row.getField(0).toString();
                        Timestamp colTime = new Timestamp(DateUtils.formatDate(row.getField(2).toString()).getTime());
                        String assetYpe = row.getField(3).toString();
                        String location = row.getField(4).toString();
                        String parent = row.getField(5).toString();
                        String productModel = row.getField(6).toString();

                        String productModelB = row.getField(7).toString();
                        String productModelC = row.getField(8).toString();
                        Double fractionRatio = Double.parseDouble(row.getField(9).toString());
                        Double loadRate = Double.parseDouble(row.getField(10).toString());

                        String variableName = ((Row[]) row.getField(11))[0].getField(0).toString();
                        Integer val = Integer.parseInt(((Row[]) row.getField(11))[0].getField(1).toString());

                        Map<String, Object> map = new HashMap<>(24);
                        map.put("IEDName", iedName);
                        map.put("ColTime", DateUtils.formatDate(new Date(colTime.getTime())));
                        map.put("VariableName", variableName);
                        map.put("Val", val);

                        map.put("assetYpe", assetYpe);
                        map.put("location", location);
                        map.put("parent", parent);
                        map.put("productModel", productModel);
                        map.put("productModelB", productModelB);
                        map.put("productModelC", productModelC);
                        map.put("fractionRatio", fractionRatio);
                        map.put("loadRate", loadRate);
                        map.put("alarmLevel", ALARM_MAP.get(variableName));

                        map.put("CreateTime", DateUtils.formatDate(now));

                        return Requests.indexRequest().index(INDEX_YX).type(TYPE_YX).source(map);
                    }

                    @Override
                    public void process(Row row, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(row));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(50);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> restClientBuilder.setMaxRetryTimeoutMillis(300)
        );
        input.addSink(esSinkBuilder.build()).name("YC-ATS-ElasticSearch-Sink");
    }
}
