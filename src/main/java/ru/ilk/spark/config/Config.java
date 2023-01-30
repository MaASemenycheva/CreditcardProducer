package ru.ilk.spark.config;

import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Logger;
import java.io.File;

import ru.ilk.spark.cassandra.CassandraConfig;
import ru.ilk.spark.kafka.KafkaConfig;
import ru.ilk.spark.spark.SparkConfig;

public class Config {
    private static Logger logger = Logger.getLogger(Config.class.getName());
    public static com.typesafe.config.Config applicationConf = ConfigFactory.parseResources("application-local.conf");
    String runMode = "local";
    static String localProjectDir = "";

    /**
     * Parse a config object from application.conf file in src/main/resources
     */
    public static void parseArgs(String[] args) {
        if(args.length == 0) {
            defaultSetting();
        } else {
            com.typesafe.config.Config applicationConf = ConfigFactory.parseFile(new File(args[0]));
            String runMode = applicationConf.getString("config.mode");
            if(runMode == "local"){
                localProjectDir = "%s%s%s".formatted("file:///", System.getProperty("user.home"), "/frauddetection/");
            }
            loadConfig();
        }
    }

    public static void loadConfig() {
        CassandraConfig.load();
        KafkaConfig.load();
        SparkConfig.load();
    }

    public static void defaultSetting() {
        CassandraConfig.defaultSettng();
        KafkaConfig.defaultSetting();
        SparkConfig.defaultSetting();
    }

}
