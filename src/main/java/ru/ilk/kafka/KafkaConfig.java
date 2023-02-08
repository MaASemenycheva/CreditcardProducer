package ru.ilk.kafka;

import org.apache.log4j.Logger;
import ru.ilk.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KafkaConfig {

    private static Logger logger = Logger.getLogger(KafkaConfig.class.getName());

    public static Map<String, Object> kafkaParams = new HashMap();

    // !!!!!!!!!!!!!!!!!!!!!!!!! void
    public static void load() {
        logger.info("Loading Kafka Setttings");
        kafkaParams.put("topic", Config.applicationConf.getString("config.kafka.topic"));
        kafkaParams.put("enable.auto.commit", Config.applicationConf.getString("config.kafka.enable.auto.commit"));
        kafkaParams.put("group.id", Config.applicationConf.getString("config.kafka.group.id"));
        kafkaParams.put("bootstrap.servers", Config.applicationConf.getString("config.kafka.bootstrap.servers"));
        kafkaParams.put("auto.offset.reset", Config.applicationConf.getString("config.kafka.auto.offset.reset"));
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!! void
    /* Default Settings will be used when you run the project from Intellij */
    public static void defaultSetting() {
        kafkaParams.put("topic", "creditcardTransaction");
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("group.id", "RealTime Creditcard FraudDetection");
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("auto.offset.reset", "earliest");
    }

}
