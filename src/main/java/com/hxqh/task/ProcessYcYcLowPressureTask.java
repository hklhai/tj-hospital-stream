package com.hxqh.task;

import com.hxqh.domain.YcLowPressure;
import com.hxqh.schema.LowPressureCustomRowSchema;
import com.hxqh.task.sink.MySQLYcLowPressureSink;
import com.hxqh.transfer.ProcessYcLowPressureWaterEmitter;
import com.hxqh.transfer.ProcessYcTransformerWaterEmitter;
import com.hxqh.utils.ConvertUtils;
import com.hxqh.utils.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
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
 * Created by Ocean lin on 2020/4/14.
 *
 * @author Ocean lin
 */
@SuppressWarnings("Duplicates")
public class ProcessYcYcLowPressureTask {

    public static void main(String[] args) {
        args = new String[]{"--input-topic", "yctest", "--bootstrap.servers", "tj-hospital.com:9092",
                "--zookeeper.connect", "tj-hospital.com:2181", "--group.id", "yctest",
                "--output-topic-ats", "lowats", "--output-topic-capacitor", "lowcapacitor", "--output-topic-drawer", "lowdrawer", "--output-topic-acb", "lowacb"};


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
                        .field("IEDParam", ObjectArrayTypeInfo.getInfoFor(
                                Row[].class,
                                Types.ROW(new String[]{"variableName", "value"},
                                        new TypeInformation[]{Types.STRING(), Types.DOUBLE()})))

        ).inAppendMode().registerTableSource("yc_lowpressure");

        Table table = tableEnvironment.sqlQuery("select * from yc_lowpressure where assetYpe like'" + LOW_VOLTAGE_SWITCHGEAR + "%'");
        DataStream<Row> data = tableEnvironment.toAppendStream(table, Row.class);
        data.assignTimestampsAndWatermarks(new ProcessYcLowPressureWaterEmitter());


        // String LOW_VOLTAGE_ATS = "低压开关设备-ATS";
        DataStream<Row> ats = data.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                YcLowPressure lowPressure = ConvertUtils.convert2YcLowPressure(row);
                return LOW_VOLTAGE_ATS.equals(lowPressure.getAssetYpe()) ? true : false;
            }
        });
        FlinkKafkaProducer010<Row> atsProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-ats"), new LowPressureCustomRowSchema(), parameterTool.getProperties());
        ats.addSink(atsProducer);


        // String LOW_VOLTAGE_CAPACITOR = "低压开关设备-电容器";
        DataStream<Row> capacitor = data.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                YcLowPressure lowPressure = ConvertUtils.convert2YcLowPressure(row);
                return LOW_VOLTAGE_CAPACITOR.equals(lowPressure.getAssetYpe()) ? true : false;
            }
        });
        FlinkKafkaProducer010<Row> capacitorProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-capacitor"), new LowPressureCustomRowSchema(), parameterTool.getProperties());
        capacitor.addSink(capacitorProducer);


        // String LOW_VOLTAGE_DRAWER_CABINET = "低压开关设备-抽屉柜";
        DataStream<Row> drawer = data.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                YcLowPressure lowPressure = ConvertUtils.convert2YcLowPressure(row);
                return LOW_VOLTAGE_DRAWER_CABINET.equals(lowPressure.getAssetYpe()) ? true : false;
            }
        });
        FlinkKafkaProducer010<Row> drawerProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-drawer"), new LowPressureCustomRowSchema(), parameterTool.getProperties());
        drawer.addSink(drawerProducer);


        // String LOW_VOLTAGE_INCOMING_CABINET = "低压开关设备-进线柜"; String LOW_VOLTAGE_COUPLER_CABINET = "低压开关设备-母联柜"; String LOW_VOLTAGE_FEEDER_CABINET = "低压开关设备-馈线柜";
        DataStream<Row> acb = data.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                YcLowPressure lowPressure = ConvertUtils.convert2YcLowPressure(row);
                if (LOW_VOLTAGE_INCOMING_CABINET.equals(lowPressure.getAssetYpe()) || LOW_VOLTAGE_COUPLER_CABINET.equals(lowPressure.getAssetYpe()) || LOW_VOLTAGE_FEEDER_CABINET.equals(lowPressure.getAssetYpe())) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        FlinkKafkaProducer010<Row> acbProducer = new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic-acb"), new LowPressureCustomRowSchema(), parameterTool.getProperties());
        acb.addSink(acbProducer);


        data.addSink(new MySQLYcLowPressureSink()).name("Yc-LowPressure-MySQL-Sink");
        persistEs(data);

        try {
            env.execute("ProcessYcYcLowPressureTask");
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
                        YcLowPressure lowPressure = ConvertUtils.convert2YcLowPressure(row);
                        TreeMap<String, Object> map = new TreeMap<>();

                        try {
                            map = ConvertUtils.objToMap(lowPressure);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                        map.put("ColTime", DateUtils.formatDate(lowPressure.getColTime()));
                        map.put("CreateTime", DateUtils.formatDate(now));
                        map.remove("serialVersionUID");

                        return Requests.indexRequest().index(INDEX_YC_LOWPRESSURE).type(TYPE_YC_LOWPRESSURE).source(map);
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
