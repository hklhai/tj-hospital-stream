package com.hxqh.mockdata.generator;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by Ocean lin on 2020/2/24.
 *
 * @author Ocean lin
 */
public class ConfigGenerator {

    public static void main(String[] args) {
        String config = "{\"channel\":\"APP\",\"registerDate\":\"2018-01-01\",\"historyPurchaseTimes\":0,\"maxPurchasePathLength\":3}";

        Properties props = new Properties();
        props.put("bootstrap.servers", "tj-hospital.com:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record;

        record = new ProducerRecord<>("purchasePathAnalysisConf", null, new Random().nextInt() + "", config);
        producer.send(record);

        producer.close();
    }

}
