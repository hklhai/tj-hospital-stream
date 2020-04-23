package com.hxqh.task;

import com.hxqh.domain.YcTransformer;
import com.hxqh.task.sink.MySQLYcTransformerSink;
import com.hxqh.transfer.ProcessYcTransformerWaterEmitter;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.DateUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.hxqh.constant.Constant.*;

/**
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ProcessYcTransformerTask {


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
                        .field("fractionRatio", Types.STRING())
                        .field("loadRate", Types.STRING())
                        .field("IEDParam", ObjectArrayTypeInfo.getInfoFor(
                                Row[].class,
                                Types.ROW(new String[]{"variableName", "value"},
                                        new TypeInformation[]{Types.STRING(), Types.DOUBLE()})))

        ).inAppendMode().registerTableSource("yc_transformer");

        Table table = tableEnvironment.sqlQuery("select * from yc_transformer where assetYpe='" + TRANSFORMER + "'");
        DataStream<Row> data = tableEnvironment.toAppendStream(table, Row.class);

        data.assignTimestampsAndWatermarks(new ProcessYcTransformerWaterEmitter());

        data.addSink(new MySQLYcTransformerSink()).name("Yc-Transformer-MySQL-Sink");

        persistEs(data);

        try {
            env.execute("ProcessYcTransformerTask");
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
                        YcTransformer ycTransformer = ConvertUtils.convert2YcTransformer(row);
                        TreeMap<String, Object> map = new TreeMap<>();

                        try {
                            map = ConvertUtils.objToMap(ycTransformer);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                        map.put("ColTime", DateUtils.formatDate(ycTransformer.getColTime()));
                        map.put("CreateTime", DateUtils.formatDate(now));
                        map.remove("serialVersionUID");

                        return Requests.indexRequest().index(INDEX_YC_TRANSFORMER).type(TYPE_YC_TRANSFORMER).source(map);
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
