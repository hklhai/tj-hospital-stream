package com.hxqh.mockdata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cloudwise.sdg.dic.DicInitializer;
import com.cloudwise.sdg.template.TemplateAnalyzer;
import com.google.common.collect.ImmutableMap;
import com.hxqh.constant.GlobalConfig;
import com.hxqh.domain.Orders;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * Created by Ocean lin on 2020/3/5.
 *
 * @author Ocean lin
 */
public class OrdersSimulator {
    public static final Map<String, String> PROVINCE_MAP = new ImmutableMap
            .Builder<String, String>()
            .put("1", "北京")
            .put("2", "上海")
            .put("3", "天津")
            .put("4", "重庆")
            .put("5", "黑龙江")
            .put("6", "吉林")
            .put("7", "辽宁")
            .put("8", "内蒙古")
            .put("9", "河北")
            .put("10", "新疆")
            .put("11", "甘肃")
            .put("12", "青海")
            .put("13", "陕西")
            .put("14", "宁夏")
            .put("15", "河南")
            .put("16", "山东")
            .put("17", "山西")
            .put("18", "安徽")
            .put("19", "湖北")
            .put("20", "湖南")
            .put("21", "江苏")
            .put("22", "四川")
            .put("23", "贵州")
            .put("24", "云南")
            .put("25", "广西")
            .put("26", "西藏")
            .put("27", "浙江")
            .put("28", "江西")
            .put("29", "广东")
            .put("30", "福建")
            .put("31", "台湾")
            .put("32", "海南")
            .put("33", "香港")
            .put("34", "澳门")
            .build();

    private static final String[] FIELD_NAMES = new String[]{
            "orderNo",
            "userId",
            "goodId",
            "goodsMoney",
            "realTotalMoney",
            "payFrom",
            "province"
    };

    private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.BIG_DEC_TYPE_INFO,
            BasicTypeInfo.BIG_DEC_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
    };

    private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setDBUrl(GlobalConfig.DB_URL)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setBatchSize(GlobalConfig.BATCH_SIZE)
                .setQuery("insert into orders (orderNo,userId ,goodId ,goodsMoney ,realTotalMoney ,payFrom ,province) values (?,?,?,?,?,?,?)")
                .setParameterTypes(new TypeInformation[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }).build();

        //模拟生成Orders
        DataStream<Row> orders = sEnv.addSource(new RichParallelSourceFunction<Row>() {
            //定义状态标识位
            private volatile boolean isRunning = true;
            private TemplateAnalyzer ordersTplAnalyzer;
            private Orders orders;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //加载数据字典
                DicInitializer.init();

                //编辑模板
                String ordersTpl = "{\"orderNo\":\"$Dic{orderNo}\",\"userId\":72,\"goodId\":\"$Dic{goodId}\",\"goodsMoney\":\"$Dic{goodsMoney}\",\"realTotalMoney\":\"$Dic{realTotalMoney}\",\"payFrom\":\"$Dic{payFrom}\",\"province\":\"$Dic{province}\"}";

                //创建模板分词器
                ordersTplAnalyzer = new TemplateAnalyzer("orders", ordersTpl);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void run(SourceContext<Row> sc) throws Exception {
                while (isRunning) {
                    orders = JSON.parseObject(ordersTplAnalyzer.analyse(), new TypeReference<Orders>() {
                    });
                    sc.collect(Row.of(
                            orders.getOrderNo(),
                            orders.getUserId(),
                            orders.getGoodId(),
                            orders.getGoodsMoney(),
                            orders.getRealTotalMoney(),
                            orders.getPayFrom(),
                            PROVINCE_MAP.get(orders.getProvince())
                    ));
                    long sleep = (long) (Math.random() * 2000);
                    Thread.sleep(sleep);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, ROW_TYPE);

        //orders.print();

        sink.emitDataStream(orders);

        sEnv.execute();
    }
}
